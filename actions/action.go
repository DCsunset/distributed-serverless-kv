package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/DCsunset/openwhisk-grpc/db"
	"google.golang.org/grpc"
)

type Argument struct {
	// keys from [low, high)
	Low      int  `json:"low"`
	High     int  `json:"high"`
	Parallel bool `json:"parallel"`
}

const APIHOST = "172.18.0.4:31001"
const ACTION = "test-grpc"
const username = "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
const password = "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"

type Result struct {
	Count int `json:"count"`
}

func callAction(params Argument, ch chan int) {
	jsonValue, _ := json.Marshal(params)

	client := &http.Client{}
	req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/api/v1/namespaces/guest/actions/%s?blocking=true&result=true", APIHOST, ACTION), bytes.NewBuffer(jsonValue))
	req.SetBasicAuth(username, password)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {
		log.Fatalf("Fail to invoke action: %v", err)
	}
	defer resp.Body.Close()

	var result Result
	// body, _ := ioutil.ReadAll(resp.Body)
	// fmt.Println(string(body))
	json.NewDecoder(resp.Body).Decode(&result)

	ch <- result.Count
}

func wordCountSequential(client db.DbServiceClient, low int, high int) int {
	count := 0
	for i := low; i < high; i++ {
		response, err := client.Get(context.Background(), &db.GetRequest{Key: strconv.Itoa(i)})
		if err != nil {
			log.Fatalf("%v", err)
		}
		words := response.GetValue()
		count += len(strings.Fields(words))
	}
	return count
}

func wordCountParallel(low int, high int) int {
	count := 0

	ch := make(chan int)
	for i := low; i < high; i++ {
		go callAction(Argument{
			Low:      i,
			High:     i + 1,
			Parallel: false,
		}, ch)
	}

	// aggregate all results
	for i := low; i < high; i++ {
		count += <-ch
	}
	return count
}

const address = "172.18.0.1:9000"

func main() {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	// parse json args
	var args Argument
	json.Unmarshal([]byte(os.Args[1]), &args)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect: %v", err)
	}
	defer conn.Close()

	client := db.NewDbServiceClient(conn)

	var count int
	if !args.Parallel {
		count = wordCountSequential(client, args.Low, args.High)
	} else {
		count = wordCountParallel(args.Low, args.High)
	}

	// Must return json result
	result := make(map[string]int)
	result["count"] = count
	res, _ := json.Marshal(result)
	fmt.Println(string(res))
}
