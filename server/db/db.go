package db

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
}

var store = make(map[string]string)

func (s *Server) Get(ctx context.Context, in *GetRequest) (*GetResponse, error) {
	value, ok := store[in.Key]
	if ok {
		return &GetResponse{Value: value}, nil
	}
	return nil, status.Errorf(codes.NotFound, "Key not found")
}

func (s *Server) Set(ctx context.Context, in *SetRequest) (*SetResponse, error) {
	store[in.Key] = in.Value
	return &SetResponse{}, nil
}
