package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"sort"

	"github.com/DCsunset/openwhisk-grpc/db"
	"github.com/DCsunset/openwhisk-grpc/indexing"
	"github.com/DCsunset/openwhisk-grpc/storage"
	"github.com/DCsunset/openwhisk-grpc/utils"
	"google.golang.org/grpc"
)

type Server struct {
	Servers          []string `json:"servers"`
	AvailableServers []string `json:"availableServers"`
	// Self address
	Self string `json:"self"`
	// Initial server
	Initial string `json:"initial"`
	// Split threshold
	Threshold int `json:"threshold"`

	mergeFunction       map[string]string
	globalMergeFunction string
}

var store = storage.Store{}
var indexingService = indexing.Service{}

func (s *Server) Init() {
	store.Init()
	s.globalMergeFunction = ""
	s.mergeFunction = make(map[string]string)

	// Server configuration
	data, err := ioutil.ReadFile("./server.json")
	if err != nil {
		log.Fatalln(err)
	}
	json.Unmarshal(data, s)

	// Use initial server first
	indexingService.AddMapping(
		0,
		math.MaxUint32,
		s.Initial,
	)
}

func (self *Server) AddChild(ctx context.Context, in *db.AddChildRequest) (*db.Empty, error) {
	address := indexingService.Locate(utils.KeyHash(in.Location))

	if address == self.Self {
		store.AddChild(in.Location, in.Child)
		return &db.Empty{}, nil
	} else {
		// Forward request to the correct server
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return &db.Empty{}, err
		}
		defer conn.Close()
		client := db.NewDbServiceClient(conn)

		return client.AddChild(ctx, in)
	}
}

func (s *Server) Get(ctx context.Context, in *db.GetRequest) (*db.GetResponse, error) {
	address := indexingService.LocateKey(in.Key)

	if address == s.Self {
		value, err := store.Get(in.Key, in.Location)
		return &db.GetResponse{Value: value}, err
	} else {
		// Forward request to the correct server
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return &db.GetResponse{}, err
		}
		defer conn.Close()
		client := db.NewDbServiceClient(conn)

		return client.Get(ctx, in)
	}
}

func (s *Server) Set(ctx context.Context, in *db.SetRequest) (*db.SetResponse, error) {
	address := indexingService.LocateKey(in.Key)

	// Debug
	indexingService.Print()
	store.Print()

	if address == s.Self {
		loc := store.Set(in.Key, in.Value, in.Dep)
		if len(store.Nodes) > s.Threshold {
			s.splitKeys()
		}

		return &db.SetResponse{Location: loc}, nil
	} else {
		// Forward request to the correct server
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return &db.SetResponse{}, err
		}
		defer conn.Close()
		client := db.NewDbServiceClient(conn)

		resp, err := client.Set(ctx, in)
		if len(store.Nodes) > s.Threshold {
			s.splitKeys()
		}
		return resp, err
	}
}

// [l, m] [m+1, r]
func (s *Server) Split(ctx context.Context, in *db.SplitRequest) (*db.Empty, error) {
	indexingService.RemoveMapping(in.Left, in.Right)
	indexingService.AddMapping(in.Left, in.Mid, in.LeftServer)
	indexingService.AddMapping(in.Mid+1, in.Right, in.RightServer)

	// Remove from available servers
	for i, server := range s.AvailableServers {
		if server == in.LeftServer || server == in.RightServer {
			l := len(s.AvailableServers)
			s.AvailableServers[i] = s.AvailableServers[l-1]
			s.AvailableServers = s.AvailableServers[:l-1]
			break
		}
	}

	// Debug
	indexingService.Print()
	store.Print()

	return &db.Empty{}, nil
}

// Split based on key range
func (s *Server) splitKeys() {
	if len(s.AvailableServers) == 0 {
		return
	}

	var keys []uint32
	for i, node := range store.Nodes {
		if i == 0 {
			continue
		}
		keys = append(keys, utils.KeyHash(node.Location))
	}
	if len(keys) == 0 {
		return
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	mid := keys[len(keys)/2]

	less := 0
	greater := 0
	for _, key := range keys {
		if key > mid {
			greater += 1
		} else if key < mid {
			less += 1
		}
	}

	if greater == 0 && less == 0 {
		return
	}

	server := s.AvailableServers[rand.Intn(len(s.AvailableServers))]
	var leftServer, rightServer string
	var results []*db.Node
	if greater >= less {
		for _, node := range store.Nodes {
			if utils.KeyHash(node.Location) > mid {
				results = append(results, &db.Node{
					Location: node.Location,
					Dep:      node.Dep,
					Key:      node.Key,
					Value:    node.Value,
					Children: node.Children,
				})
				store.RemoveNode(node.Location)
			}
		}
		rightServer = server
		leftServer = s.Self
	} else {
		for _, node := range store.Nodes {
			if utils.KeyHash(node.Location) < mid {
				results = append(results, &db.Node{
					Location: node.Location,
					Dep:      node.Dep,
					Key:      node.Key,
					Value:    node.Value,
					Children: node.Children,
				})
				store.RemoveNode(node.Location)
			}
		}
		mid -= 1
		rightServer = s.Self
		leftServer = server
	}

	// Forward request to the correct server
	conn, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()
	client := db.NewDbServiceClient(conn)
	ctx := context.Background()

	client.AddNodes(ctx, &db.AddNodesRequest{
		Nodes: results,
	})

	// Transfer merge function
	for _, node := range results {
		f, ok := s.mergeFunction[node.Key]
		if ok {
			delete(s.mergeFunction, node.Key)
			client.SetMergeFunction(ctx, &db.SetMergeFunctionRequest{
				Key:  node.Key,
				Name: f,
			})
		}
	}

	// Update indexing server
	left, right := indexingService.Range(s.Self)
	request := &db.SplitRequest{
		Left:        left,
		Right:       right,
		Mid:         mid,
		LeftServer:  leftServer,
		RightServer: rightServer,
	}

	for _, addr := range s.Servers {
		if addr == s.Self {
			s.Split(ctx, request)
		} else if addr == server {
			client.Split(ctx, request)
		} else {
			// Forward request to all servers
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalln(err)
			}
			defer conn.Close()
			client := db.NewDbServiceClient(conn)

			client.AddNodes(context.Background(), &db.AddNodesRequest{
				Nodes: results,
			})
		}
	}
}

func (s *Server) AddNodes(ctx context.Context, in *db.AddNodesRequest) (*db.Empty, error) {
	store.AddNodes(in.Nodes)
	return &db.Empty{}, nil
}

func (self *Server) SetMergeFunction(ctx context.Context, in *db.SetMergeFunctionRequest) (*db.Empty, error) {
	if len(in.Name) == 0 {
		delete(self.mergeFunction, in.Key)
	} else {
		self.mergeFunction[in.Key] = in.Name
	}
	return &db.Empty{}, nil
}

func (self *Server) SetGlobalMergeFunction(ctx context.Context, in *db.SetGlobalMergeFunctionRequest) (*db.Empty, error) {
	for _, addr := range self.Servers {
		if addr == self.Self {
			self.globalMergeFunction = in.Name
		} else {
			// Forward request to all servers
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalln(err)
			}
			defer conn.Close()
			client := db.NewDbServiceClient(conn)

			client.SetGlobalMergeFunction(ctx, in)
		}
	}
	return &db.Empty{}, nil
}
