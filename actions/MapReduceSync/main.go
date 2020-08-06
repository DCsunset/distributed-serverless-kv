package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/DCsunset/openwhisk-grpc/db"
	"github.com/DCsunset/openwhisk-grpc/storage"
	"github.com/DCsunset/openwhisk-grpc/utils"
	"google.golang.org/grpc"
)

func mapper(sessionId int64, cache *storage.Store, virtualLoc int64, location int64) map[string]string {
	count := make(map[string]string)
	var keys []string
	key := strconv.Itoa(int(virtualLoc))
	keys = append(keys, key)

	res := cache.Get(sessionId, keys, location, -1)

	words := strings.Fields(res[key])
	for _, word := range words {
		c, ok := count[word]
		if ok {
			orig, _ := strconv.Atoi(c)
			count[word] = strconv.Itoa(orig + 1)
		} else {
			count[word] = "1"
		}
	}

	return count
}

func reducer(sessionId int64, mapperResults []map[string]string) map[string]string {
	count := make(map[string]string)

	// From key 0 to 20
	virtualLocs := utils.MakeRange(0, 20)

	for _, loc := range virtualLocs {
		partialData := mapperResults[loc]
		for key, valueStr := range partialData {
			c, ok := count[key]
			if ok {
				orig, _ := strconv.Atoi(c)
				value, _ := strconv.Atoi(valueStr)
				count[key] = strconv.Itoa(orig + value)
			} else {
				count[key] = valueStr
			}
		}
	}

	return count
}

func runner(client db.DbServiceClient, sessionId int64, location int64) {
	// Local cache
	var cache = storage.Store{}
	cache.Init()

	merkleTreeResp, _ := client.GetMerkleTree(context.Background(), &db.GetMerkleTreeRequest{
		Location: 0,
	})
	// outdated locations
	locations := cache.Compare(merkleTreeResp.Nodes)

	// Download data from server
	resp, _ := client.Download(context.Background(), &db.DownloadRequest{
		Locations: locations,
	})
	cache.Upload(resp.Nodes)

	// Count key from 0 to 20
	slices := utils.MakeRange(0, 20)

	// This part can run in parallel
	for _, slice := range slices {
		count := mapper(sessionId, &cache, slice, location)
		utils.Print(count)

		// Store intermediate results locally
		cache.Set(sessionId, count, slice, 0, -1)
	}

	// Fetch the result from local cache
	var mapperResults []map[string]string
	for _, slice := range slices {
		result := cache.Get(sessionId, nil, -1, slice)
		mapperResults = append(mapperResults, result)
	}

	println("R: ", mapperResults)

	res := reducer(sessionId, mapperResults)
	utils.Print(res)
}

type Argument struct {
	Location int64 `json:"location"`
}

const address = "172.18.0.1:9000"

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect: %v", err)
	}
	defer conn.Close()
	client := db.NewDbServiceClient(conn)

	// parse json args
	var args Argument
	print(os.Args[1])
	json.Unmarshal([]byte(os.Args[1]), &args)

	// Generate new session ID
	id := rand.Int63()
	runner(client, id, args.Location)
}
