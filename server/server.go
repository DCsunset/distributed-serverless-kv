package main

import (
	"context"

	"github.com/DCsunset/openwhisk-grpc/db"
)

type Server struct{}

var store = Store{}

func (s *Server) Get(ctx context.Context, in *db.GetRequest) (*db.GetResponse, error) {
	value := store.Get(in.Key, in.Loc)
	return &db.GetResponse{Value: value}, nil
}

func (s *Server) Set(ctx context.Context, in *db.SetRequest) (*db.SetResponse, error) {
	loc := store.Set(in.Key, in.Value, in.VirtualLoc, in.Dep, in.VirtualDep)
	return &db.SetResponse{Location: loc}, nil
}
