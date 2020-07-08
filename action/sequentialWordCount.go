package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/DCsunset/openwhisk-grpc/db"
	"google.golang.org/grpc"
)

func wordCount(client db.DbServiceClient, keys []string) int {
	count := 0
	for _, key := range keys {
		response, _ := client.Get(context.Background(), &db.GetRequest{ Key: key })
		words := response.GetValue()
		count += len(strings.Fields(words))
	}
	return count
}

const address = "172.19.0.1:9000"

func main() {
	// parse json args
	var args map[string][]string
	json.Unmarshal([]byte(os.Args[1]), &args)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect: %v", err)
	}
	defer conn.Close()

	client := db.NewDbServiceClient(conn)

	// Keys to count
	keys, _ := args["keys"]
	fmt.Println(keys)
	count := wordCount(client, keys)

	// Must return json result
	result := make(map[string] int)
	result["count"] = count
	res, _ := json.Marshal(result)
	fmt.Println(string(res))
}
