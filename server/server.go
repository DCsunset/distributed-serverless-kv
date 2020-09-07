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
}

var store = storage.Store{}
var indexingService = indexing.Service{}

func (s *Server) Init() {
	store.Init()

	// Server configuration
	data, err := ioutil.ReadFile("./server.json")
	if err != nil {
		log.Fatalln(err)
	}
	json.Unmarshal(data, s)

	// Use initial server first
	indexingService.AddMapping(
		math.MinInt64,
		math.MaxInt64,
		s.Initial,
	)
}

func (s *Server) Get(ctx context.Context, in *db.GetRequest) (*db.GetResponse, error) {
	address := indexingService.LocateKey(in.Key)

	if address == s.Self {
		value, err := store.Get(in.SessionId, in.Key, in.Loc)
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

	if address == s.Self {
		loc := store.Set(in.SessionId, in.Key, in.Value, in.Dep)
		if len(store.Nodes) > s.Threshold {
			s.splitKeys()
		}
		return &db.SetResponse{Loc: loc}, nil
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
func (s *Server) Split(ctx context.Context, in *db.SplitRequest) (*db.SplitResponse, error) {
	indexingService.RemoveMapping(in.Left, in.Right)
	indexingService.AddMapping(in.Left, in.Mid, in.LeftServer)
	indexingService.AddMapping(in.Mid+1, in.Right, in.RightServer)

	// Debug
	indexingService.Print()
	store.Print()

	return &db.SplitResponse{}, nil
}

// Split based on key range
func (s *Server) splitKeys() {
	if len(s.AvailableServers) == 0 {
		return
	}

	var keys []int64
	for i, node := range store.Nodes {
		if i == 0 {
			continue
		}
		keys = append(keys, node.KeyHash)
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
			if node.KeyHash > mid {
				results = append(results, &db.Node{
					Dep:     node.Dep,
					Digest:  node.Digest,
					Key:     node.Key,
					KeyHash: node.KeyHash,
					Value:   node.Value,
				})
				store.RemoveNode(node.Digest)
			}
		}
		rightServer = server
		leftServer = s.Self
	} else {
		for _, node := range store.Nodes {
			if node.KeyHash < mid {
				results = append(results, &db.Node{
					Dep:     node.Dep,
					Digest:  node.Digest,
					Key:     node.Key,
					KeyHash: node.KeyHash,
					Value:   node.Value,
				})
				store.RemoveNode(node.Digest)
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

	client.AddNodes(context.Background(), &db.AddNodesRequest{
		Nodes: results,
	})

	// Update indexing server
	left, right := indexingService.Range(s.Self)
	ctx := context.Background()
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

func (s *Server) AddNodes(ctx context.Context, in *db.AddNodesRequest) (*db.AddNodesResponse, error) {
	store.AddNodes(in.Nodes)
	return &db.AddNodesResponse{}, nil
}
