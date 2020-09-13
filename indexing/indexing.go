package indexing

import (
	"fmt"

	"github.com/DCsunset/openwhisk-grpc/utils"
)

type Mapping struct {
	Left    uint32
	Right   uint32
	Address string
}

type Service struct {
	Mappings []Mapping
}

func (s *Service) AddMapping(left, right uint32, server string) {
	s.Mappings = append(s.Mappings, Mapping{left, right, server})
}

func (s *Service) RemoveMapping(left, right uint32) {
	for i, mapping := range s.Mappings {
		if mapping.Left == left && mapping.Right == right {
			l := len(s.Mappings)
			s.Mappings[i], s.Mappings[l-1] = s.Mappings[l-1], s.Mappings[i]
			s.Mappings = s.Mappings[:l-1]
			return
		}
	}
}

func (self *Service) Locate(keyHash uint32) string {
	for _, m := range self.Mappings {
		if keyHash >= m.Left && keyHash <= m.Right {
			return m.Address
		}
	}
	panic(fmt.Sprintf("Key hash %x not found", keyHash))
}

func (s *Service) LocateKey(key string) string {
	keyHash := utils.Hash2Uint(utils.Hash([]byte(key)))
	return s.Locate(keyHash)
}

func (s *Service) Range(server string) (uint32, uint32) {
	for _, mapping := range s.Mappings {
		if mapping.Address == server {
			return mapping.Left, mapping.Right
		}
	}
	return 0, 0
}

func (s *Service) Print() {
	fmt.Println("Mappings:")
	for _, m := range s.Mappings {
		fmt.Printf("%x-%x: %s\n", m.Left, m.Right, m.Address)
	}
}
