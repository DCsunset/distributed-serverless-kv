package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"

	"github.com/DCsunset/openwhisk-grpc/db"
	"google.golang.org/grpc"
)

const address = "172.18.0.1:9000"

func makeUpdate(localData map[string]string, key string, value string) map[string]string {
	update := make(map[string]string)
	update[key] = value
	localData[key] = value
	return update
}

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect: %v", err)
	}
	defer conn.Close()

	client := db.NewDbServiceClient(conn)
	// Simulate the local cache
	localData := make(map[string]string)

	// Session ID
	id := rand.Int63()

	update := makeUpdate(localData, "chained-1", "a a")
	res1, _ := client.Set(context.Background(), &db.SetRequest{
		SessionId:  id,
		Data:       update,
		Dep:        0,
		VirtualLoc: 1,
	})
	update = makeUpdate(localData, "chained-2", localData["chained-1"]+" b b")
	res2, _ := client.Set(context.Background(), &db.SetRequest{
		SessionId:  id,
		Data:       update,
		Dep:        -2, // -2 means nil
		VirtualLoc: 2,
		VirtualDep: 1,
	})
	update = makeUpdate(localData, "chained-1", localData["chained-1"]+" a a")
	res3, _ := client.Set(context.Background(), &db.SetRequest{
		SessionId:  id,
		Data:       update,
		Dep:        -1, // -1 means nil
		VirtualLoc: 3,
		VirtualDep: 2,
	})

	// Must return json result
	result := make(map[string]int64)
	result["loc1"] = res1.GetLoc()
	result["loc2"] = res2.GetLoc()
	result["loc3"] = res3.GetLoc()
	res, _ := json.Marshal(result)
	fmt.Println(string(res))
}
