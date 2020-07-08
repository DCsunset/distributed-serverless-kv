package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/DCsunset/openwhisk-grpc/db"
	"google.golang.org/grpc"
)

func randomWords(characters []string, length int) string {
	var buffer bytes.Buffer
	for i := 0; i < length; i++ {
		buffer.WriteString(characters[rand.Intn(len(characters))])
	}
	return buffer.String()
}

const address = "localhost:9000"

func main() {
	rand.Seed(time.Now().Unix())
	characters := strings.Split("abcdefghijklmnopqrstuvwxyz \t", "")

	const keyLength = 8
	const valueLength = 256
	const keyCount = 1024

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect: %v", err)
	}
	defer conn.Close()

	client := db.NewDbServiceClient(conn)

	for i := 0; i < keyCount; i++ {
		k := randomWords(characters, keyLength)
		v := randomWords(characters, valueLength)
		client.Set(context.Background(), &db.SetRequest{
			Key:   k,
			Value: v,
		})
		fmt.Printf("Set key %s\n", k)
	}
}
