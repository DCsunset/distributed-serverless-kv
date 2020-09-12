package main

import (
	"context"

	simpleDb "github.com/DCsunset/openwhisk-grpc/simple-db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
}

var store = make(map[string]string)

func (s *Server) Get(ctx context.Context, in *simpleDb.GetRequest) (*simpleDb.GetResponse, error) {
	value, ok := store[in.Key]
	if ok {
		return &simpleDb.GetResponse{Value: value}, nil
	}
	return nil, status.Errorf(codes.NotFound, "Key not found")
}

func (s *Server) Set(ctx context.Context, in *simpleDb.SetRequest) (*simpleDb.SetResponse, error) {
	store[in.Key] = in.Value
	return &simpleDb.SetResponse{}, nil
}
