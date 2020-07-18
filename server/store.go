package main

type Node struct {
	dep  int64
	data map[string]string // record current updates
}

type Store struct {
	nodes []Node // all nodes
}

func (s *Store) newNode(dep int64) int64 {
	node := Node{
		dep:  -1,
		data: make(map[string]string),
	}
	s.nodes = append(s.nodes, node)
	return int64(len(s.nodes)) - 1
}

func (s *Store) Get(key string, loc int64) string {
	node := s.nodes[loc]
	// Find till root
	for _, ok := node.data[key]; !ok && node.dep != -1; {
		node = s.nodes[node.dep]
	}
	return node.data[key]
}

func (s *Store) Set(key string, value string, virtualLoc int64, dep int64, virtualDep int64) int64 {
	newLoc := s.newNode(dep)
	node := s.nodes[newLoc]
	node.data[key] = value
	return newLoc
}
