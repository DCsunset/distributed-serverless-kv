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
	Action string
	Kind   string
}

func callWorker(channel chan int64, args *Args) {
	start := time.Now()
	utils.CallAction("benchmark", []byte(utils.ToString(args)))
	channel <- time.Since(start).Milliseconds()
}

const batchSize = 30

var servers = []string{"aqua02:9000", "aqua03:9000"}
var simpleServer = "aqua02:9001"
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
		channel := make(chan int64, batchSize)
		for i := 0; i < batchSize; i += 1 {
			go callWorker(channel, &Args{
				Action: "worker",
				Kind:   args.Kind,
			})
		}
		var sum int64
		sum = 0
		for i := 0; i < batchSize; i += 1 {
			sum += <-channel
		}
		// GiB/s
		throughput := float64(batchSize) / float64(sum)
		// ms
		latency := float64(sum) / float64(batchSize)
		fmt.Printf("{ \"throughput\": \"%f GiB/s\", \"latency\": \"%f ms\" }", throughput, latency)

	} else {

		key := randomWords(20)
		value := strings.Repeat("t", 1024*1024)
		ctx := context.Background()

		if args.Kind != "simple" {
			conn, err := grpc.Dial(simpleServer, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Cannot connect: %v", err)
			}
			defer conn.Close()

			client := simpleDb.NewDbServiceClient(conn)

			client.Set(ctx, &simpleDb.SetRequest{
				Key:   "test",
				Value: value,
			})
		} else {
			// Randomly choose one server
			address := servers[rand.Intn(len(servers))]

			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Cannot connect: %v", err)
			}
			defer conn.Close()

			client := db.NewDbServiceClient(conn)

			// Set merge function
			client.Set(ctx, &db.SetRequest{
				Key:   key,
				Value: value,
			})
		}
	}
}
