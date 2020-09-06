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
	indexingService.AddMapping(indexing.Range{
		L: math.MinInt64,
		R: math.MaxInt64,
	}, s.Initial)
}

func (s *Server) Get(ctx context.Context, in *db.GetRequest) (*db.GetResponse, error) {
	address, err := indexingService.LocateKey(in.Key)
	if err != nil {
		return &db.GetResponse{}, err
	}

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
	address, err := indexingService.LocateKey(in.Key)
	if err != nil {
		return &db.SetResponse{}, err
	}

	if address == s.Self {
		loc := store.Set(in.SessionId, in.Key, in.Value, in.Dep)
		return &db.SetResponse{Loc: loc}, nil
	} else {
		// Forward request to the correct server
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return &db.SetResponse{}, err
		}
		defer conn.Close()
		client := db.NewDbServiceClient(conn)

		return client.Set(ctx, in)
	}
}

// Split based on key range
func (s *Server) split() {
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
	}

	server := s.AvailableServers[rand.Intn(len(s.AvailableServers))]
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
	// TODO: Update indexing server
}

func (s *Server) AddNodes(ctx context.Context, in *db.AddNodesRequest) (*db.AddNodesResponse, error) {
	store.AddNodes(in.Nodes)
	return &db.AddNodesResponse{}, nil
}
