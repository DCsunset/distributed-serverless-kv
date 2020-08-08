package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/DCsunset/openwhisk-grpc/db"
	"github.com/DCsunset/openwhisk-grpc/storage"
	"github.com/DCsunset/openwhisk-grpc/utils"
	"google.golang.org/grpc"
)

type Argument struct {
	// Location to store the words
	Location      int64               `json:"location"`
	Kind          string              `json:"kind"`
	Sentence      string              `json:"virtualLoc,omitempty"`
	MapperResults []map[string]string `json:"mapperResults,omitempty"`
}

const APIHOST = "172.18.0.4:31001"
const ACTION = "mapreduce"
const username = "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
const password = "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"

func callAction(params *Argument) []byte {
	jsonValue, _ := json.Marshal(params)

	url := fmt.Sprintf("https://%s/api/v1/namespaces/guest/actions/%s?blocking=true&result=true", APIHOST, ACTION)

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	req.SetBasicAuth(username, password)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {
		log.Fatalf("Fail to invoke action: %v", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	return bodyBytes
}

func mapper(sentence string) map[string]string {
	count := make(map[string]string)

	words := strings.Fields(sentence)
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

func reducer(mapperResults []map[string]string) map[string]string {
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
		var keys []string
		key := strconv.Itoa(int(slice))
		keys = append(keys, key)

		sentence := cache.Get(sessionId, keys, location, -1)[key]

		res := callAction(&Argument{
			Kind:     "mapper",
			Sentence: sentence,
		})
		var count map[string]string
		json.Unmarshal(res, &count)
		//utils.Print(count)

		// Store intermediate results locally
		cache.Set(sessionId, count, slice, 0, -1)
	}

	// Fetch the result from local cache
	var mapperResults []map[string]string
	for _, slice := range slices {
		result := cache.Get(sessionId, nil, -1, slice)
		mapperResults = append(mapperResults, result)
	}

	// Reduce using virtual locations
	res := callAction(&Argument{
		Kind:          "reducer",
		MapperResults: mapperResults,
	})
	fmt.Println(string(res))
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
	json.Unmarshal([]byte(os.Args[1]), &args)

	// Generate new session ID
	if args.Kind == "runner" {
		// Generate new session ID
		id := rand.Int63()
		runner(client, id, args.Location)
	} else if args.Kind == "mapper" {
		mapper(args.Sentence)
	} else {
		reducer(args.MapperResults)
	}
}
