package main

type Node struct {
	dep  int
	data map[string]string // record current updates
}

type Store struct {
	nodes []Node // all nodes
}

func (s *Store) newNode(dep int) int {
	node := Node{
		dep:  -1,
		data: make(map[string]string),
	}
	s.nodes = append(s.nodes, node)
	return len(s.nodes) - 1
}

func (s *Store) Get(key string, loc int) string {
	node := s.nodes[loc]
	// Find till root
	for _, ok := node.data[key]; !ok && node.dep != -1; {
		node = s.nodes[node.dep]
	}
	return node.data[key]
}

func (s *Store) Set(key string, value string, virtualLoc int, dep int, virtualDep int) int {
	newLoc := s.newNode(dep)
	node := s.nodes[newLoc]
	node.data[key] = value
	return newLoc
}
