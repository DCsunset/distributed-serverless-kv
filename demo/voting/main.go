package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"

	"github.com/DCsunset/openwhisk-grpc/db"
	"google.golang.org/grpc"
)

func vote(ctx context.Context, client db.DbServiceClient, name string) {
	res, err := client.Get(ctx, &db.GetRequest{
		Key: name,
	})

	var votes int
	if err != nil {
		// Not found
		votes = 1
	} else {
		votes, _ = strconv.Atoi(res.Value)
		votes += 1
	}
	setRequest := &db.SetRequest{
		Key:   name,
		Value: strconv.Itoa(votes),
		Dep:   0,
	}

	client.Set(ctx, setRequest)
}

type Argument struct {
	Name string `json:"name"`
}

// Server list
var addresses = [...]string{"aqua02:9000", "aqua03:9000", "aqua04:9000", "aqua05:9000"}

func main() {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	// parse json args
	var args Argument
	json.Unmarshal([]byte(os.Args[1]), &args)

	// Randomly choose one server
	address := addresses[rand.Intn(len(addresses))]

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect: %v", err)
	}
	defer conn.Close()

	client := db.NewDbServiceClient(conn)
	ctx := context.Background()
	vote(ctx, client, args.Name)

	fmt.Println("{}")
}
