package storage

type Node struct {
	dep  int64
	data map[string]string // record current updates
}

type Store struct {
	Nodes []Node // all nodes
	// Map virtual locations to read ones (with sessionId)
	LocMap map[int64]map[int64]int64
}

func (s *Store) newNode(dep int64, data map[string]string) int64 {
	node := Node{
		dep:  -1,
		data: data,
	}
	s.Nodes = append(s.Nodes, node)
	return int64(len(s.Nodes)) - 1
}

func (s *Store) Get(id int64, keys []string, loc int64, virtualLoc int64) map[string]string {
	var node Node
	if loc >= 0 {
		node = s.Nodes[loc]
	} else {
		realLoc := s.LocMap[id][virtualLoc]
		node = s.Nodes[realLoc]
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
		node = s.Nodes[node.dep]
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
	var newLoc int64
	if dep != -2 {
		newLoc = s.newNode(dep, data)
		s.addToLocMap(id, virtualLoc, newLoc)
	} else {
		realDep := s.LocMap[id][virtualDep]
		newLoc = s.newNode(realDep, data)
	}
	return newLoc
}
