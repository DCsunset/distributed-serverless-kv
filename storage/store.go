package storage

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/DCsunset/openwhisk-grpc/db"
	"github.com/DCsunset/openwhisk-grpc/utils"
)

type Node struct {
	Location uint64 // The location of the key
	Dep      uint64
	Key      string
	Children []uint64
	Value    string
}

type Store struct {
	Nodes []Node // all nodes
	// Map hash locations to memory locations
	MemLocation map[uint64]int
}

func (s *Store) Init() {
	if len(s.Nodes) == 0 {
		// Create a root and map first
		s.MemLocation = make(map[uint64]int)
		root := Node{
			Dep:      math.MaxUint64,
			Location: 0,
		}
		s.Nodes = append(s.Nodes, root)
		s.MemLocation[0] = 0
	}
}

func (s *Store) newNode(location uint64, dep uint64, key string, value string) {
	node := Node{
		Location: location,
		Dep:      dep,
		Key:      key,
		Children: nil,
		Value:    value,
	}
	s.Nodes = append(s.Nodes, node)
	memLoc := len(s.Nodes) - 1

	s.MemLocation[location] = memLoc
}

func (s *Store) Get(key string, loc uint64) (string, error) {
	var node *Node
	node = s.GetNode(loc)

	// Find till root
	for {
		if node.Key == key {
			return node.Value, nil
		}
		if node.Dep == math.MaxUint64 {
			break
		}
		node = s.GetNode(node.Dep)
	}
	return "", fmt.Errorf("Key %s not found", key)
}

type Data struct {
	Key   string
	Value string
	Dep   int64
}

func (self *Store) AddChild(location uint64, child uint64) *Node {
	node := self.GetNode(location)
	node.Children = append(node.Children, child)
	return node
}

func (s *Store) Set(key string, value string, dep uint64) uint64 {
	// Use random number + key hash
	loc := uint64(rand.Uint32()) + (uint64(utils.Hash2Uint(utils.Hash([]byte(key)))) << 32)
	s.newNode(loc, dep, key, value)

	return loc
}

func (s *Store) GetNode(loc uint64) *Node {
	memLoc, ok := s.MemLocation[loc]
	if !ok {
		return nil
	}
	return &s.Nodes[memLoc]
}

func (s *Store) AddNodes(nodes []*db.Node) {
	for _, node := range nodes {
		s.newNode(node.Location, node.Dep, node.Key, node.Value)
	}
}

func (s *Store) RemoveNode(location uint64) {
	for i, node := range s.Nodes {
		if node.Location == location {
			s.Nodes[i] = Node{}
			return
		}
	}
}

func (s *Store) Print() {
	fmt.Println("Nodes:")
	for _, node := range s.Nodes {
		fmt.Printf("%s: %s\n", node.Key, node.Value)
	}
}
