package indexing

import (
	"fmt"

	"github.com/DCsunset/openwhisk-grpc/utils"
)

type Range struct {
	L, R int64
}

type Mapping struct {
	R       Range
	Address string
}

type Service struct {
	Mappings []Mapping
}

func (s *Service) AddMapping(r Range, server string) {
	s.Mappings = append(s.Mappings, Mapping{r, server})
}

func (s *Service) LocateKey(key string) (string, error) {
	keyHash := utils.Hash2int(utils.Hash([]byte(key)))

	for _, m := range s.Mappings {
		r := m.R
		address := m.Address
		if keyHash >= r.L && keyHash <= r.R {
			return address, nil
		}
	}
	return "", fmt.Errorf("Key %s not found", key)
}
