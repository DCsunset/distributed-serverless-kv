package utils

import (
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

func PrintMap(m map[string]string) {
	str, _ := json.Marshal(m)
	fmt.Println(string(str))
}
