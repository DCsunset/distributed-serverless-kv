package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/DCsunset/openwhisk-grpc/db"
	"google.golang.org/grpc"
)

const address = "172.19.0.1:9000"

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect: %v", err)
	}
	defer conn.Close()

	client := db.NewDbServiceClient(conn)

	// Set key first
	_, err = client.Set(context.Background(), &db.SetRequest{
		Key: "test",
		Value: "Hello",
	})
	if err != nil {
		log.Fatalf("Error calling Set: %v", err)
	}

	// Get key
	response, err := client.Get(context.Background(), &db.GetRequest{ Key: "test" })
	if err != nil {
		log.Fatalf("Error calling Get: %v", err)
	}

	result := make(map[string] string)
	result["value"] = response.GetValue()
	res, _ := json.Marshal(result)
	fmt.Println(string(res))
}
