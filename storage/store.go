package storage

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/DCsunset/openwhisk-grpc/db"
	"github.com/DCsunset/openwhisk-grpc/utils"
)

type Node struct {
	Dep     int64
	Key     string
	KeyHash int64 // The hash of the key
	Value   string
	Digest  []byte // Use digest for comparation and location
}

type Store struct {
	Nodes []Node // all nodes
	// Map hash locations to memory locations
	MemLocation map[int64]int64
}

func (s *Store) Init() {
	if len(s.Nodes) == 0 {
		// Create a root and map first
		s.MemLocation = make(map[int64]int64)
		root := Node{
			Dep:    -1,
			Digest: nil,
		}
		s.Nodes = append(s.Nodes, root)
		s.MemLocation[0] = 0
	}
}

func (s *Store) newNode(dep int64, key string, value string, digest []byte) int64 {
	// Use hash location
	loc := utils.Hash2int(digest)

	node := Node{
		Dep:     dep,
		Key:     key,
		KeyHash: utils.Hash2int(utils.Hash([]byte(key))),
		Value:   value,
		Digest:  digest,
	}
	s.Nodes = append(s.Nodes, node)
	memLoc := int64(len(s.Nodes)) - 1

	s.MemLocation[loc] = memLoc

	return loc
}

func (s *Store) Get(id int64, key string, loc int64) (string, error) {
	var node *Node
	node = s.getNode(loc)

	// Find till root
	for {
		if node.Key == key {
			return node.Value, nil
		}
		if node.Dep == -1 {
			break
		}
		node = s.getNode(node.Dep)
	}
	return "", fmt.Errorf("Key %s not found", key)
}

type Data struct {
	Key   string
	Value string
	Dep   int64
}

func (s *Store) Set(id int64, key string, value string, dep int64) int64 {
	// Compute hash
	dataBytes, _ := json.Marshal(Data{Key: key, Value: value, Dep: dep})
	digest := utils.Hash(dataBytes)

	newLoc := s.newNode(dep, key, value, digest)
	return newLoc
}

func (s *Store) getNode(loc int64) *Node {
	memLoc, ok := s.MemLocation[loc]
	if !ok {
		return nil
	}
	return &s.Nodes[memLoc]
}

func (s *Store) AddNodes(nodes []*db.Node) {
	for _, node := range nodes {
		s.newNode(node.Dep, node.Key, node.Value, node.Digest)
	}
}

func (s *Store) RemoveNode(dataDigest []byte) {
	for i, node := range s.Nodes {
		if bytes.Compare(node.Digest, dataDigest) == 0 {
			s.Nodes[i] = Node{}
			return
		}
	}
}
