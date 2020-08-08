package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"reflect"

	"github.com/DCsunset/openwhisk-grpc/db"
)

type Node struct {
	Dep        int64
	Children   []int64
	Data       map[string]string // record current updates
	Digest     []byte            // Save Merkle tree inline
	DataDigest []byte            // Save data digest for comparing itself
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
			Dep:        -1,
			Data:       make(map[string]string),
			Children:   nil,
			DataDigest: nil,
		}
		s.Nodes = append(s.Nodes, root)
		s.MemLocation[0] = 0
	}
}

func nodeLocation(dataDigest []byte) int64 {
	if dataDigest == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(dataDigest[:8]))
}

func (s *Store) newNode(dep int64, data map[string]string, dataDigest []byte) int64 {
	// Use hash location
	loc := nodeLocation(dataDigest)

	node := Node{
		Dep:        dep,
		Data:       data,
		Children:   nil,
		DataDigest: dataDigest,
	}
	s.Nodes = append(s.Nodes, node)
	memLoc := int64(len(s.Nodes)) - 1

	s.MemLocation[loc] = memLoc

	// Append to parent's child list
	parent := s.getNode(dep)
	parent.Children = append(parent.Children, loc)

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
		return node.Data
	}

	data := make(map[string]string)

	// Find till root
	for {
		for _, key := range keys {
			_, ok := data[key]
			if !ok {
				value, ok := node.Data[key]
				if ok {
					data[key] = value
				}
			}
		}
		if node.Dep == -1 {
			break
		}
		node = s.getNode(node.Dep)
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
	dataBytes, _ := json.Marshal(data)
	hash.Write(dataBytes)
	depBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(depBytes, uint64(dep))
	hash.Write(depBytes)
	digest := hash.Sum(nil)

	var newLoc int64
	if dep >= 0 {
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
	memLoc, ok := s.MemLocation[loc]
	if !ok {
		return nil
	}
	return &s.Nodes[memLoc]
}

func (s *Store) updateHash(loc int64, currentDigest []byte) {
	node := s.getNode(loc)
	node.DataDigest = currentDigest
	// Update till root
	for {
		hash := sha256.New()
		for _, child := range node.Children {
			// Concatenate all digests
			hash.Write(s.getNode(child).Digest)
		}
		node.Digest = hash.Sum(currentDigest)
		if node.Dep == -1 {
			break
		}
		node = s.getNode(node.Dep)
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
			Dep:        node.Dep,
			Digest:     node.Digest,
			DataDigest: node.DataDigest,
			Children:   node.Children,
		})
		for _, child := range node.Children {
			queue = append(queue, child)
		}
	}

	return nodes
}

func (s *Store) Download(locations []int64) []*db.Node {
	// Extract only hash info from tree
	var nodes []*db.Node

	var queue []int64
	queue = append(queue, locations...)
	for len(queue) > 0 {
		currentLocation := queue[0]
		queue = queue[1:]
		node := s.getNode(currentLocation)
		nodes = append(nodes, &db.Node{
			Dep:        node.Dep,
			Digest:     node.Digest,
			DataDigest: node.DataDigest,
			Children:   node.Children,
			Data:       node.Data,
		})
		for _, child := range node.Children {
			queue = append(queue, child)
		}
	}

	// nodes are in topological order
	return nodes
}

func (s *Store) Upload(nodes []*db.Node) {
	for _, node := range nodes {
		location := nodeLocation(node.DataDigest)
		localNode := s.getNode(location)
		if localNode != nil {
			localNode.Children = append(localNode.Children, node.Children...)
			s.updateHash(location, localNode.DataDigest)
		} else {
			loc := s.newNode(node.Dep, node.Data, node.DataDigest)
			n := s.getNode(loc)
			n.Children = node.Children
			n.Digest = node.Digest
		}
	}
}

// Compare outdated notes
// The nodes are in the Merkle tree
func (s *Store) Compare(nodes []*db.Node) []int64 {
	outdatedNodes := make(map[int64]bool)
	sameNodes := make(map[int64]bool)

	var results []int64

	for _, node := range nodes {
		location := nodeLocation(node.DataDigest)
		_, ok := outdatedNodes[location]
		if ok {
			// Add children to outdatedNodes
			for _, child := range node.Children {
				outdatedNodes[child] = true
			}
			continue
		}
		_, ok = sameNodes[location]
		if ok {
			// Add children to sameNodes
			for _, child := range node.Children {
				sameNodes[child] = true
			}
			continue
		}

		localNode := s.getNode(location)
		// The whole subtree is missed
		if localNode == nil {
			results = append(results, location)
			outdatedNodes[location] = true
			// Add children to outdated
			for _, child := range node.Children {
				outdatedNodes[child] = true
			}
			continue
		}

		if reflect.DeepEqual(localNode.Digest, node.Digest) {
			sameNodes[location] = true
			// Add children to sameNodes
			for _, child := range node.Children {
				sameNodes[child] = true
			}
			continue
		}

		// Only the subtree is different, continue
	}

	return results
}
