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
	goredis "github.com/go-redis/redis"
	"github.com/magiconair/properties"
	"github.com/matryer/vice/v2/queues/redis"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"log"
	"math/rand"
	"sync"
	"time"
)

var CommandTypeLatenciesRead *hdrhistogram.Histogram
var CommandTypeLatenciesWrite *hdrhistogram.Histogram

func processGraphDatapointsChannel(graphStatsChann chan Datapoint, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case dp := <-graphStatsChann:
			{
				switch dp.Type {
				case CommandTypeRead:
					CommandTypeLatenciesRead.RecordValue(int64(dp.Latency))
				case CommandTypeWrite:
					CommandTypeLatenciesWrite.RecordValue(int64(dp.Latency))
				}
			}
		case <-ctx.Done():
			fmt.Println("\nReceived Ctrl-c - shutting down datapoints processor go-routine")
			return
		}
	}
}

// agentCmd represents the agent command
var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("agent called")
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
		agentId, _ := cmd.Flags().GetString("agent-unique-id")
		log.Printf("Setting myself as active agent. Agent unique-id %s", agentId)
		workersChannel := transport.Send("global-nosql-benchmark:workers")
		mychannel := fmt.Sprintf("global-nosql-benchmark:workers:%s", agentId)
		workersChannel <- []byte(mychannel)
		workReceiverChannel := transport.Receive(mychannel)
		errs := transport.ErrChan()
		workersChannelFinished := transport.Send(string("global-nosql-benchmark:workers:finish"))

		for {
			select {
			case <-ctx.Done():
				log.Println("finished")
				return
			case err := <-errs:
				log.Println("an error occurred:", err)
			case spec := <-workReceiverChannel:
				var testSpec TestSpec
				CommandTypeLatenciesRead = hdrhistogram.New(1, 90000000000, 3)
				CommandTypeLatenciesWrite = hdrhistogram.New(1, 90000000000, 3)

				if err := json.Unmarshal(spec, &testSpec); err != nil {
					panic(err)
				}
				currentUtc := time.Now().UTC().Unix()
				differenceUnix := testSpec.StartAtUnix - currentUtc
				dataPointsProcessingWg := sync.WaitGroup{}
				dataPointsChann := make(chan Datapoint, testSpec.Clients)
				log.Printf("Received test spec: %v. sleeping for %d secs and starting test...", testSpec, differenceUnix)

				useRateLimiter := false
				if testSpec.Rps > 0 {
					useRateLimiter = true
				}
				var requestRate = Inf
				var requestBurst = int(testSpec.Rps)
				if testSpec.Rps != 0 {
					log.Printf("Test spec contains rate of %d rps...", testSpec.Rps)
					requestRate = rate.Limit(testSpec.Rps)
					useRateLimiter = true
				} else {
					log.Printf("Test spec does not specify rate of ops/sec. Doing unlimited rate...")
				}

				var rateLimiter = rate.NewLimiter(requestRate, requestBurst)
				log.Printf("Setting read-proportion to %f and insert-proportion to %f.", testSpec.ReadProportion, testSpec.InsertProportion)
				creator := GetDBCreator("redis")
				p := properties.NewProperties()
				dbLocalReadAddr, _ := cmd.Flags().GetString("db.local.read.addr")
				p.Set("db.addr", testSpec.Addr)
				p.Set("db.local.read.addr", dbLocalReadAddr)
				p.Set("db.username", testSpec.Username)
				p.Set("db.password", testSpec.Password)
				time.Sleep(time.Duration(differenceUnix * int64(time.Second)))
				log.Printf("Starting test...")
				// Create a context with a timeout of test duration
				ctxTimeout, cancel := context.WithTimeout(context.Background(), testSpec.Duration)
				defer cancel()
				dataPointsProcessingWg.Add(1)
				go processGraphDatapointsChannel(dataPointsChann, ctxTimeout, &dataPointsProcessingWg)
				startT := time.Now()

				db, err := creator.Create(p)
				if err != nil {
					panic(err)
				}
				table := "table"
				for i := 0; i < int(testSpec.Clients); i++ {
					go agentWork(ctx, db, startT, table, useRateLimiter, rateLimiter, testSpec, dataPointsChann)
				}
				log.Printf("Waiting for all datapoints to be processed...")
				dataPointsProcessingWg.Wait()
				endT := time.Now()
				log.Printf("Finished receiving all datapoints...")
				testResult := NewTestResult(agentId, testSpec.Clients, testSpec.Rps)
				testResult.EncodedWriteHistogram, err = CommandTypeLatenciesWrite.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
				if err != nil {
					panic(err)
				}
				testResult.EncodedReadHistogram, err = CommandTypeLatenciesRead.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
				if err != nil {
					panic(err)
				}

				testDuration := endT.Sub(startT)
				testResult.FillDurationInfo(startT, endT, testDuration)
				_, latenciesWrites := generateLatenciesMap(CommandTypeLatenciesWrite, testDuration)
				_, latenciesReads := generateLatenciesMap(CommandTypeLatenciesRead, testDuration)
				testResult.OverallClientLatencies = make(map[string]map[string]float64)
				testResult.OverallClientLatencies["reads"] = latenciesReads
				testResult.OverallClientLatencies["inserts"] = latenciesWrites

				// merging histograms to get the totals
				CommandTypeLatenciesWrite.Merge(CommandTypeLatenciesRead)
				_, latenciesTotal := generateLatenciesMap(CommandTypeLatenciesWrite, testDuration)
				testResult.OverallClientLatencies["total"] = latenciesTotal
				u, err := json.Marshal(testResult)
				if err != nil {
					panic(err)
				}
				workersChannelFinished <- u
				log.Printf("Finished sending test result...")
				ctx.Done()

			}
		}

		log.Printf("closing myself...")
	},
}

func agentWork(ctx context.Context, db DB, startT time.Time, table string, useRateLimiter bool, rateLimiter *rate.Limiter, testSpec TestSpec, dataPointsChann chan Datapoint) {
	var err error = nil
	testDuration := time.Now().Sub(startT)
	for testDuration < testSpec.Duration {
		key := fmt.Sprintf("%d", rand.Int63n(int64(testSpec.RecordCount)))
		if useRateLimiter {
			r := rateLimiter.ReserveN(time.Now(), int(1))
			time.Sleep(r.Delay())
		}
		cmdType := CommandTypeRead
		commandStartT := time.Now()
		if rand.Float64() > testSpec.ReadProportion {
			cmdType = CommandTypeRead
			// read
			_, err = db.Read(ctx, table, key, testSpec.ReadPreference, []string{"field1"})
		} else {
			// write
			cmdType = CommandTypeWrite
			err = db.Insert(ctx, table, key, testSpec.NumReplicas, testSpec.NumReplicasTimeoutMillis, map[string][]byte{"field1": []byte(stringWithCharset(int(testSpec.DataSize), charset))})
		}
		currentT := time.Now()
		commandDuration := currentT.Sub(commandStartT)
		micros := commandDuration.Microseconds()
		dataPointsChann <- Datapoint{
			Error:   err != nil,
			Latency: uint64(micros),
			Type:    cmdType,
		}
		testDuration = currentT.Sub(startT)
	}
}

func init() {
	rootCmd.AddCommand(agentCmd)

	agentCmd.Flags().String("agent-unique-id", "myid", "Agent unique id")
	agentCmd.Flags().String("db.local.read.addr", "", "comma separated value of local addresses to the db that should be prioritized")
}
