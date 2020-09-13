package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
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
	GetThroughput float64 `json:"GetThroughput"`
	GetLatency    float64 `json:"GetLatency"`
	SetThroughput float64 `json:"SetThroughput"`
	SetLatency    float64 `json:"SetLatency"`
}

func callWorker(channel chan Result, args *Args) {
	resp := utils.CallAction("benchmark", []byte(utils.ToString(args)))
	var result Result
	json.Unmarshal(resp, &result)
	channel <- result
}

const batchSize = 20

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

func workerThread(channel chan Result, args *Args) {
	var getDur, setDur int64
	value := strings.Repeat("t", 1024*1024)
	ctx := context.Background()

	if args.Kind == "simple" {
		conn, err := grpc.Dial(simpleServer, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Cannot connect: %v", err)
		}
		defer conn.Close()

		client := simpleDb.NewDbServiceClient(conn)

		var keys []string
		start := time.Now()
		for i := 0; i < 10; i += 1 {
			key := args.Key + randomWords(8)
			keys = append(keys, key)
			client.Set(ctx, &simpleDb.SetRequest{
				Key:   key,
				Value: value,
			})
		}
		setDur = time.Since(start).Milliseconds()

		start = time.Now()
		for _, key := range keys {
			client.Get(ctx, &simpleDb.GetRequest{
				Key: key,
			})
		}
		getDur = time.Since(start).Milliseconds()
	} else {
		// Randomly choose one server
		address := args.Server

		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Cannot connect: %v", err)
		}
		defer conn.Close()

		client := db.NewDbServiceClient(conn)

		var keys []string
		var locs []uint64
		start := time.Now()
		for i := 0; i < 10; i += 1 {
			key := args.Key + randomWords(8)
			keys = append(keys, key)
			resp, _ := client.Set(ctx, &db.SetRequest{
				Dep:   0,
				Key:   key,
				Value: value,
			})
			locs = append(locs, resp.Location)
		}
		setDur = time.Since(start).Milliseconds()

		start = time.Now()
		for i := 0; i < 10; i += 1 {
			client.Get(ctx, &db.GetRequest{
				Location: locs[i],
				Key:      keys[i],
			})
		}
		getDur = time.Since(start).Milliseconds()
	}

	channel <- Result{
		SetLatency:    float64(setDur) / 100,
		SetThroughput: float64(100) / float64(setDur),
		GetLatency:    float64(getDur) / 100,
		GetThroughput: float64(100) / float64(getDur),
	}
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

		result := Result{
			GetLatency:    0,
			GetThroughput: 0,
			SetLatency:    0,
			SetThroughput: 0,
		}
		for i := 0; i < batchSize; i += 1 {
			r := <-channel
			result.GetLatency += r.GetLatency
			result.GetThroughput += r.GetThroughput
			result.SetLatency += r.SetLatency
			result.SetThroughput += r.SetThroughput
		}
		result.GetLatency /= batchSize
		result.SetLatency /= batchSize

		utils.Print(result)
	} else {
		channel := make(chan Result, 10)
		for i := 0; i < 10; i += 1 {
			go workerThread(channel, &args)
		}

		result := Result{
			GetLatency:    0,
			GetThroughput: 0,
			SetLatency:    0,
			SetThroughput: 0,
		}
		for i := 0; i < 10; i += 1 {
			r := <-channel
			result.GetLatency += r.GetLatency
			result.GetThroughput += r.GetThroughput
			result.SetLatency += r.SetLatency
			result.SetThroughput += r.SetThroughput
		}
		result.GetLatency /= 10
		result.SetLatency /= 10

		utils.Print(result)
	}
}
