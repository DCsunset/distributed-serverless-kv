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
	"github.com/DCsunset/openwhisk-grpc/utils"
	"google.golang.org/grpc"
)

// Server list
var addresses = [...]string{"aqua02:9000", "aqua03:9000", "aqua04:9000", "aqua05:9000"}

func main() {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	// parse json args
	var parent *db.Node
	json.Unmarshal([]byte(os.Args[1]), &parent)

	// Randomly choose one server
	address := addresses[rand.Intn(len(addresses))]

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect: %v", err)
	}
	defer conn.Close()

	client := db.NewDbServiceClient(conn)
	ctx := context.Background()

	var children []*db.Node
	votesNum := 0
	maxVotes := 0
	for _, child := range parent.Children {
		node, err := client.GetNode(ctx, &db.GetNodeRequest{
			Location: child,
		})
		if err != nil {
			log.Fatalln(err)
		}

		if node.Key == "votes" {
			votesNum += 1
			if maxVotes < utils.Str2Int(node.Value) {
				maxVotes = utils.Str2Int(node.Value)
			}
		} else {
			// Keep other nodes
			children = append(children, node)
		}
	}
	children = append(children, &db.Node{
		Dep:   parent.Location,
		Key:   "votes",
		Value: strconv.Itoa(maxVotes + votesNum - 1),
	})

	data, _ := json.Marshal(children)

	fmt.Println(string(data))
}
