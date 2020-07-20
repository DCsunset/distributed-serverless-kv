package main

import (
	"context"

	"github.com/DCsunset/openwhisk-grpc/db"
)

type Server struct{}

var store = Store{}

func (s *Server) Get(ctx context.Context, in *db.GetRequest) (*db.GetResponse, error) {
	data := store.Get(in.Keys, in.Loc)
	return &db.GetResponse{Data: data}, nil
}

func (s *Server) Set(ctx context.Context, in *db.SetRequest) (*db.SetResponse, error) {
	loc := store.Set(in.SessionId, in.Data, in.VirtualLoc, in.Dep, in.VirtualDep)
	return &db.SetResponse{Loc: loc}, nil
}
