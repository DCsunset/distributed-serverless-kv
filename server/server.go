package main

import (
	"context"

	"github.com/DCsunset/openwhisk-grpc/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
}

var store = make(map[string]string)

func (s *Server) Get(ctx context.Context, in *db.GetRequest) (*db.GetResponse, error) {
	value, ok := store[in.Key]
	if ok {
		return &db.GetResponse{Value: value}, nil
	}
	return nil, status.Errorf(codes.NotFound, "Key not found")
}

func (s *Server) Set(ctx context.Context, in *db.SetRequest) (*db.SetResponse, error) {
	store[in.Key] = in.Value
	return &db.SetResponse{}, nil
}
