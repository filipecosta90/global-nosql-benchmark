/*
Copyright © 2023 Filipe Oliveira hello@codeperf.io

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"os"
	"time"

	goredis "github.com/go-redis/redis"
	"github.com/matryer/vice/v2/queues/redis"
	"github.com/spf13/cobra"
	"log"
)

// coordCmd represents the coord command
var coordCmd = &cobra.Command{
	Use:   "coord",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("coord called")
		ctx := context.Background()
		viceAddr, _ := cmd.Flags().GetString("messaging-queue-addr")
		vicePassword, _ := cmd.Flags().GetString("messaging-queue-password")
		redisOptions := goredis.Options{
			Addr:     viceAddr,
			Password: vicePassword,
		}
		redisClient := goredis.NewClient(&redisOptions)
		transport := redis.New(redis.WithClient(redisClient))
		defer func() {
			transport.Stop()
			<-transport.Done()
		}()

		numAgents, _ := cmd.Flags().GetInt64("num-agents")
		numReplicas, _ := cmd.Flags().GetInt64("insert-ack-num-replicas")
		numReplicasTimeoutMillis, _ := cmd.Flags().GetInt64("insert-ack-num-replicas-timeout-millis")
		readPreference, _ := cmd.Flags().GetInt64("read-preference")
		readProportion, _ := cmd.Flags().GetFloat64("read-proportion")
		insertProportion, _ := cmd.Flags().GetFloat64("insert-proportion")
		connsPerAgent, _ := cmd.Flags().GetUint64("conns-per-agent")
		duration, _ := cmd.Flags().GetDuration("duration")
		delayStart, _ := cmd.Flags().GetDuration("delay-start")
		rps, _ := cmd.Flags().GetUint64("rps")
		recordCount, _ := cmd.Flags().GetUint64("record-count")
		dataSize, _ := cmd.Flags().GetUint64("data-size")
		dbAddr, _ := cmd.Flags().GetString("db.addr")
		dbPassword, _ := cmd.Flags().GetString("db.password")
		dbUser, _ := cmd.Flags().GetString("db.user")
		var totalAgentsDone int64 = 0
		rpsPerAgent := rps
		if rps > 0 {
			rpsPerAgent = rps / uint64(numAgents)
			log.Printf("Given you've specified a rate of %d requests/sec and there are %d agents, each agent will do %d rps...", rps, numAgents, rpsPerAgent)
		} else {
			log.Printf("Setting an unlimited rate of requests/sec, across %d agents...", numAgents)
		}
		log.Printf("Setting myself as coordinator of benchmark...")
		log.Printf("Waiting for a total of %d agents to be ready to benchmark.", numAgents)
		log.Printf("Setting read-proportion to %f and insert-proportion to %f.", readProportion, insertProportion)
		workReceiverChannel := transport.Receive("global-nosql-benchmark:workers")
		workEndChannel := transport.Receive("global-nosql-benchmark:workers:finish")
		workersChannelMap := make(map[string]*TestResult, 0)
		CommandTypeLatenciesRead = hdrhistogram.New(1, 90000000000, 3)
		CommandTypeLatenciesWrite = hdrhistogram.New(1, 90000000000, 3)

		for {
			select {
			case <-ctx.Done():
				log.Println("finished")
				return
			case testResult := <-workEndChannel:
				var testSpec TestResult
				if err := json.Unmarshal(testResult, &testSpec); err != nil {
					panic(err)
				}
				workersChannelMap[testSpec.Metadata] = &testSpec
				testResultFilename := fmt.Sprintf("%s.json", testSpec.Metadata)
				testResultHistogramFilenameReads := fmt.Sprintf("%s-reads.hgrm", testSpec.Metadata)
				testResultHistogramFilenameWrites := fmt.Sprintf("%s-inserts.hgrm", testSpec.Metadata)
				testResultHistogramFilename := fmt.Sprintf("%s.hgrm", testSpec.Metadata)
				log.Printf("Received result from %s agent. Saving it to %s and to %s", testSpec.Metadata, testResultFilename, testResultHistogramFilename)
				saveJsonResult(&testSpec, testResultFilename)
				encWriteHistogram := testSpec.EncodedWriteHistogram
				wH, err := hdrhistogram.Decode(encWriteHistogram)
				if err != nil {
					panic(err)
				}
				f, err := os.Create(testResultHistogramFilenameWrites)
				if err != nil {
					panic(err)
				}
				wH.PercentilesPrint(f, 1, 1)
				f.Close()
				encReadHistogram := testSpec.EncodedReadHistogram
				rH, err := hdrhistogram.Decode(encReadHistogram)
				if err != nil {
					panic(err)
				}
				f, err = os.Create(testResultHistogramFilenameReads)
				if err != nil {
					panic(err)
				}
				rH.PercentilesPrint(f, 1, 1)
				f.Close()
				rH.Merge(wH)
				f, err = os.Create(testResultHistogramFilename)
				if err != nil {
					panic(err)
				}
				rH.PercentilesPrint(f, 1, 1)
				f.Close()
				CommandTypeLatenciesRead.Merge(rH)
				CommandTypeLatenciesWrite.Merge(wH)
				totalAgentsDone++
				if totalAgentsDone >= numAgents {
					log.Printf("All %d agents are done... Saving global histogram files", totalAgentsDone)
					f, err = os.Create("all-reads.hgrm")
					if err != nil {
						panic(err)
					}
					CommandTypeLatenciesRead.PercentilesPrint(f, 1, 1)
					f, err = os.Create("all-inserts.hgrm")
					if err != nil {
						panic(err)
					}
					CommandTypeLatenciesWrite.PercentilesPrint(f, 1, 1)
					totalAgentsDone = 0
					CommandTypeLatenciesRead.Reset()
					CommandTypeLatenciesWrite.Reset()
				}

			case workersChannelName := <-workReceiverChannel:
				log.Printf("received new worker named %s...", workersChannelName)
				workersChannelMap[string(workersChannelName)] = nil
				totalAgentsReady := int64(len(workersChannelMap))
				log.Printf("There are a total of %d agents ready. We need %d to trigger benchmark...", totalAgentsReady, numAgents)
				if totalAgentsReady >= numAgents {
					totalAgentsDone = 0
					CommandTypeLatenciesRead.Reset()
					CommandTypeLatenciesWrite.Reset()

					currentUtc := time.Now().UTC().Unix()
					delaySecs := int64(delayStart.Seconds())
					startUtc := currentUtc + delaySecs
					log.Printf("Starting benchmark in %d seconds...", delaySecs)
					for workerName, _ := range workersChannelMap {
						log.Printf("Sending work to agent: %s ...", workerName)
						spec := TestSpec{
							Duration:                 duration,
							Rps:                      rpsPerAgent,
							Clients:                  connsPerAgent,
							StartAtUnix:              startUtc,
							ReadPreference:           readPreference,
							NumReplicas:              numReplicas,
							NumReplicasTimeoutMillis: numReplicasTimeoutMillis,
							ReadProportion:           readProportion,
							InsertProportion:         insertProportion,
							RecordCount:              recordCount,
							DataSize:                 dataSize,
							// db related
							Addr:     dbAddr,
							Password: dbPassword,
							Username: dbUser,
						}
						u, err := json.Marshal(spec)
						if err != nil {
							panic(err)
						}
						workersChannel := transport.Send(string(workerName))
						workersChannel <- u
					}
				}
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(coordCmd)

	coordCmd.Flags().Uint64("rps", 0, "requests per second across all regions")
	coordCmd.Flags().Uint64("record-count", 1000000, "keyspace length.")
	coordCmd.Flags().Uint64("data-size", 100, "object data size.")
	coordCmd.Flags().Duration("duration", time.Second*180, "duration of the benchmark")
	coordCmd.Flags().Duration("delay-start", time.Second*10, "delay start of benchmark by the time specified as soon as we have the required number of agents")
	coordCmd.Flags().Int64("num-agents", 1, "minimum number of agents to trigger the benchmark.")
	coordCmd.Flags().Uint64("conns-per-agent", 1, "connections per agent to the db.")
	coordCmd.Flags().Float64("read-proportion", 0.5, "proportion of reads of the benchmark.")
	coordCmd.Flags().Int64("read-preference", ReadPrimary, "route read operations to the primaries (0) or to the secondaries/replicas (1).")
	coordCmd.Flags().Float64("insert-proportion", 0.5, "proportion of inserts of the benchmark.")
	coordCmd.Flags().Int64("insert-ack-num-replicas", 0, "number of replicas that acknowledged the insert.")
	coordCmd.Flags().Int64("insert-ack-num-replicas-timeout-millis", 0, "number of replicas timeout millis. if 0 blocks until ack.")

	// db specific configs
	coordCmd.Flags().String("db.addr", "localhost:6379", "Server address(es) in \"host:port\" form, can be semi-colon ; separated in cluster mode.")
	coordCmd.Flags().String("db.username", "", "Redis server username.")
	coordCmd.Flags().String("db.password", "", "Redis server password.")

}
