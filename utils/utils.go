package utils

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
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

func Hash2int(digest []byte) int64 {
	if digest == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
