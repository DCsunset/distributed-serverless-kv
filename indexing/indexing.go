package indexing

import (
	"fmt"

	"github.com/DCsunset/openwhisk-grpc/utils"
)

type Mapping struct {
	Left    int64
	Right   int64
	Address string
}

type Service struct {
	Mappings []Mapping
}

func (s *Service) AddMapping(left, right int64, server string) {
	s.Mappings = append(s.Mappings, Mapping{left, right, server})
}

func (s *Service) RemoveMapping(left, right int64) {
	for i, mapping := range s.Mappings {
		if mapping.Left == left && mapping.Right == right {
			l := len(s.Mappings)
			s.Mappings[i], s.Mappings[l-1] = s.Mappings[l-1], s.Mappings[i]
			s.Mappings = s.Mappings[:l]
			return
		}
	}
}

func (s *Service) LocateKey(key string) (string, error) {
	keyHash := utils.Hash2int(utils.Hash([]byte(key)))

	for _, m := range s.Mappings {
		if keyHash >= m.Left && keyHash <= m.Right {
			return m.Address, nil
		}
	}
	return "", fmt.Errorf("Key %s not found", key)
}

func (s *Service) Range(server string) (int64, int64) {
	for _, mapping := range s.Mappings {
		if mapping.Address == server {
			return mapping.Left, mapping.Right
		}
	}
	return 0, 0
}
