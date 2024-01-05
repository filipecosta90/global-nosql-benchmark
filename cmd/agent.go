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
	"fmt"
	goredis "github.com/go-redis/redis"
	"github.com/matryer/vice/v2/queues/redis"
	"github.com/spf13/cobra"
	"log"
	"time"
)

func Greeter(ctx context.Context, names <-chan []byte, greetings chan<- []byte, errs <-chan error) {
	for {
		select {
		case <-ctx.Done():
			log.Println("finished")
			return
		case err := <-errs:
			log.Println("an error occurred:", err)
		case name := <-names:
			greeting := "Hello " + string(name)
			greetings <- []byte(greeting)
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
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		redisOptions := goredis.Options{
			Addr:     "localhost:6379", // use default Addr
			Password: "",               // no password set
			DB:       0,                // use default DB
		}

		redisClient := goredis.NewClient(&redisOptions)
		transport := redis.New(redis.WithClient(redisClient))
		defer func() {
			transport.Stop()
			<-transport.Done()
		}()
		log.Printf("Setting myself as active agent...")
		workersChannel := transport.Send("global-nosql-benchmark:workers")
		mychannel := fmt.Sprintf("global-nosql-benchmark:workers:%s", "myid")
		workersChannel <- []byte("eu-weast-1")
		workReceiverChannel := transport.Receive()
		errs := transport.ErrChan()

		for {
			select {
			case <-ctx.Done():
				log.Println("finished")
				return
			case err := <-errs:
				log.Println("an error occurred:", err)
			case name := <-names:
				greeting := "Hello " + string(name)
				greetings <- []byte(greeting)
			}
		}

		log.Printf("closing myself...")
	},
}

func init() {
	rootCmd.AddCommand(agentCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// agentCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// agentCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
