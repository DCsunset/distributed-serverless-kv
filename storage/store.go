package storage

type Node struct {
	dep  int64
	data map[string]string // record current updates
}

type Store struct {
	nodes []Node // all nodes
	// Map virtual locations to read ones (with sessionId)
	locMap map[int64]map[int64]int64
}

func (s *Store) newNode(dep int64, data map[string]string) int64 {
	node := Node{
		dep:  -1,
		data: data,
	}
	s.nodes = append(s.nodes, node)
	return int64(len(s.nodes)) - 1
}

func (s *Store) Get(id int64, keys []string, loc int64, virtualLoc int64) map[string]string {
	var node Node
	if loc >= 0 {
		node = s.nodes[loc]
	} else {
		realLoc := s.locMap[id][virtualLoc]
		node = s.nodes[realLoc]
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
		node = s.nodes[node.dep]
	}
	return data
}

func (s *Store) addToLocMap(id int64, virtualLoc int64, loc int64) {
	if s.locMap == nil {
		s.locMap = make(map[int64]map[int64]int64)
	}
	if s.locMap[id] == nil {
		s.locMap[id] = make(map[int64]int64)
	}
	s.locMap[id][virtualLoc] = loc
}

func (s *Store) Set(id int64, data map[string]string, virtualLoc int64, dep int64, virtualDep int64) int64 {
	var newLoc int64
	if dep != -2 {
		newLoc = s.newNode(dep, data)
		s.addToLocMap(id, virtualLoc, newLoc)
	} else {
		realDep := s.locMap[id][virtualDep]
		newLoc = s.newNode(realDep, data)
	}
	return newLoc
}