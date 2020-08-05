package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"

	"github.com/DCsunset/openwhisk-grpc/db"
)

type Node struct {
	dep        int64
	children   []int64
	data       map[string]string // record current updates
	digest     []byte            // Save Merkle tree inline
	dataDigest []byte            // Save data digest for comparing itself
}

type Store struct {
	Nodes []Node // all nodes
	// Map virtual locations to read ones (with sessionId)
	LocMap map[int64]map[int64]int64
	// Map hash locations to memory locations
	MemLocation map[int64]int64
}

func (s *Store) Init() {
	if len(s.Nodes) == 0 {
		// Create a root and map first
		s.MemLocation = make(map[int64]int64)
		root := Node{
			dep:        -1,
			data:       make(map[string]string),
			children:   nil,
			dataDigest: nil,
		}
		s.Nodes = append(s.Nodes, root)
		s.MemLocation[0] = 0
	}
}

func (s *Store) newNode(dep int64, data map[string]string, dataDigest []byte) int64 {
	// Use hash location
	loc := int64(binary.BigEndian.Uint64(dataDigest[:8]))

	node := Node{
		dep:        dep,
		data:       data,
		children:   nil,
		dataDigest: dataDigest,
	}
	s.Nodes = append(s.Nodes, node)
	memLoc := int64(len(s.Nodes)) - 1

	s.MemLocation[loc] = memLoc

	// Append to parent's child list
	parent := s.Nodes[memLoc]
	parent.children = append(parent.children, loc)

	return loc
}

func (s *Store) Get(id int64, keys []string, loc int64, virtualLoc int64) map[string]string {
	var node *Node
	if loc >= 0 {
		node = s.getNode(loc)
	} else {
		realLoc := s.LocMap[id][virtualLoc]
		node = s.getNode(realLoc)
	}

	if keys == nil {
		return node.data
	}

	data := make(map[string]string)

	// Find till root
	for {
		for _, key := range keys {
			_, ok := data[key]
			if !ok {
				value, ok := node.data[key]
				if ok {
					data[key] = value
				}
			}
		}
		if node.dep == -1 {
			break
		}
		node = s.getNode(node.dep)
	}
	return data
}

func (s *Store) addToLocMap(id int64, virtualLoc int64, loc int64) {
	if s.LocMap == nil {
		s.LocMap = make(map[int64]map[int64]int64)
	}
	if s.LocMap[id] == nil {
		s.LocMap[id] = make(map[int64]int64)
	}
	s.LocMap[id][virtualLoc] = loc
}

func (s *Store) Set(id int64, data map[string]string, virtualLoc int64, dep int64, virtualDep int64) int64 {
	// Compute hash
	hash := sha256.New()
	bytes, _ := json.Marshal(data)
	digest := hash.Sum(bytes)

	var newLoc int64
	if dep != -2 {
		newLoc = s.newNode(dep, data, digest)
		s.addToLocMap(id, virtualLoc, newLoc)
	} else {
		realDep := s.LocMap[id][virtualDep]
		newLoc = s.newNode(realDep, data, digest)
	}

	s.updateHash(newLoc, digest)

	return newLoc
}

func (s *Store) getNode(loc int64) *Node {
	memLoc := s.MemLocation[loc]
	return &s.Nodes[memLoc]
}

func (s *Store) updateHash(loc int64, currentDigest []byte) {
	node := s.getNode(loc)
	node.dataDigest = currentDigest
	// Update till root
	for {
		hash := sha256.New()
		for _, child := range node.children {
			// Concatenate all digests
			hash.Write(s.getNode(child).digest)
		}
		node.digest = hash.Sum(currentDigest)
		if node.dep == -1 {
			break
		}
		node = s.getNode(node.dep)
	}
}

func (s *Store) GetMerkleTree(location int64) []*db.Node {
	// Extract only hash info from tree
	var nodes []*db.Node

	// topological order
	var queue []int64
	queue = append(queue, location)
	for len(queue) > 0 {
		currentLocation := queue[0]
		queue = queue[1:]
		node := s.getNode(currentLocation)
		nodes = append(nodes, &db.Node{
			Dep:        node.dep,
			Digest:     node.digest,
			DataDigest: node.dataDigest,
			Children:   node.children,
		})
		for _, child := range node.children {
			queue = append(queue, child)
		}
	}

	return nodes
}

func (s *Store) Download(location int64) []*db.Node {
	// Extract only hash info from tree
	var nodes []*db.Node

	var queue []int64
	queue = append(queue, location)
	for len(queue) > 0 {
		currentLocation := queue[0]
		queue = queue[1:]
		node := s.getNode(currentLocation)
		nodes = append(nodes, &db.Node{
			Dep:        node.dep,
			Digest:     node.digest,
			DataDigest: node.dataDigest,
			Children:   node.children,
			Data:       node.data,
		})
		for _, child := range node.children {
			queue = append(queue, child)
		}
	}

	return nodes
}

func (s *Store) Upload(nodes []*db.Node) {
	for _, node := range nodes {
		// Skip root
		if node.Dep == -1 {
			continue
		}
		loc := s.newNode(node.Dep, node.Data, node.DataDigest)
		n := s.getNode(loc)
		n.children = node.Children
		n.digest = node.Digest
	}
}
