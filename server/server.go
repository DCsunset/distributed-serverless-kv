package main

import (
	"context"

	"github.com/DCsunset/openwhisk-grpc/db"
)

type Server struct{}

var store = Store{}

type sessionId string

func (c sessionId) String() string {
	return string(c)
}

func (s *Server) Get(ctx context.Context, in *db.GetRequest) (*db.GetResponse, error) {
	data := store.Get(in.Keys, in.Loc)
	return &db.GetResponse{Data: data}, nil
}

func (s *Server) Set(ctx context.Context, in *db.SetRequest) (*db.SetResponse, error) {
	id := ctx.Value(sessionId("id")).(int64)
	loc := store.Set(id, in.Data, in.VirtualLoc, in.Dep, in.VirtualDep)
	return &db.SetResponse{Loc: loc}, nil
}
