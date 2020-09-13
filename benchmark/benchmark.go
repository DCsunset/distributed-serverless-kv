package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/DCsunset/openwhisk-grpc/db"
	simpleDb "github.com/DCsunset/openwhisk-grpc/simple-db"
	"github.com/DCsunset/openwhisk-grpc/utils"
	"google.golang.org/grpc"
)

type Args struct {
	Action string `json:"action,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Key    string `json:"key,omitempty"`
	Server string `json:"server,omitempty"`
}

type Result struct {
	Throughput float64 `json:"throughput"`
	Latency    float64 `json:"latency"`
}

func callWorker(channel chan Result, args *Args) {
	resp := utils.CallAction("benchmark", []byte(utils.ToString(args)))
	var result Result
	json.Unmarshal(resp, &result)
	channel <- result
}

const batchSize = 30

var servers = []string{"aqua07:9000", "aqua08:9000", "aqua09:9000", "aqua10:9000"}
var simpleServer = "aqua11:9001"
var characters = strings.Split("abcdefghijklmnopqrstuvwxyz", "")

func randomWords(length int) string {
	var buffer bytes.Buffer
	for i := 0; i < length; i++ {
		buffer.WriteString(characters[rand.Intn(len(characters))])
	}
	return buffer.String()
}

func main() {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	// parse json args
	var args Args
	json.Unmarshal([]byte(os.Args[1]), &args)

	if args.Action == "runner" {
		channel := make(chan Result, batchSize)
		for i := 0; i < batchSize; i += 1 {
			go callWorker(channel, &Args{
				Action: "worker",
				Kind:   args.Kind,
				Key:    randomWords(8),
				Server: servers[rand.Intn(len(servers))],
			})
		}
		var throughput, latency float64
		throughput = 0
		latency = 0
		for i := 0; i < batchSize; i += 1 {
			result := <-channel
			throughput += result.Throughput
			latency += result.Latency
		}

		latency = float64(latency) / float64(batchSize)
		fmt.Printf("{ \"throughput\": \"%f GiB/s\", \"latency\": \"%f ms\" }", throughput, latency)

	} else {

		value := strings.Repeat("t", 1024*1024)
		ctx := context.Background()

		if args.Kind == "simple" {
			conn, err := grpc.Dial(simpleServer, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Cannot connect: %v", err)
			}
			defer conn.Close()

			client := simpleDb.NewDbServiceClient(conn)

			start := time.Now()
			for i := 0; i < 100; i += 1 {
				client.Set(ctx, &simpleDb.SetRequest{
					Key:   args.Key + randomWords(8),
					Value: value,
				})
			}
			t := time.Since(start).Milliseconds()

			fmt.Printf("{ \"throughput\": %f, \"latency\": %f }", float64(100)/float64(t), float64(t)/100)

		} else {
			// Randomly choose one server
			address := args.Server

			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Cannot connect: %v", err)
			}
			defer conn.Close()

			client := db.NewDbServiceClient(conn)

			start := time.Now()
			for i := 0; i < 100; i += 1 {
				client.Set(ctx, &db.SetRequest{
					Key:   args.Key + randomWords(8),
					Value: value,
				})
			}
			t := time.Since(start).Milliseconds()
			fmt.Printf("{ \"throughput\": %f, \"latency\": %f }", 100.0/float64(t), float64(t)/100.0)
		}
	}
}
