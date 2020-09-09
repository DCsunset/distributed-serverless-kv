package utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func MakeRange(min, max int64) []int64 {
	a := make([]int64, max-min)
	for i := range a {
		a[i] = min + int64(i)
	}
	return a
}

func Print(m interface{}) {
	str, _ := json.Marshal(m)
	fmt.Println(string(str))
}

func ToString(m interface{}) string {
	str, _ := json.Marshal(m)
	return string(str)
}

func Hash(data []byte) []byte {
	// Compute hash
	hash := sha256.New()
	return hash.Sum(data)
}

func Hash2Uint(digest []byte) uint32 {
	if digest == nil {
		return 0
	}
	return uint32(binary.BigEndian.Uint32(digest[:4]))
}

func KeyHash(location uint64) uint32 {
	return uint32(location >> 32)
}

const APIHOST = "aqua02:31001"
const USER = "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
const PASS = "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"

func CallAction(params []byte, action string) []byte {
	url := fmt.Sprintf("https://%s/api/v1/namespaces/guest/actions/%s?blocking=true&result=true", APIHOST, action)

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(params))
	req.SetBasicAuth(USER, PASS)
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
