package main

import (
	"context"

	"github.com/DCsunset/openwhisk-grpc/db"
	"github.com/DCsunset/openwhisk-grpc/storage"
)

type Server struct{}

var store = storage.Store{}

func (s *Server) Init() {
	store.Init()
}

func (s *Server) Get(ctx context.Context, in *db.GetRequest) (*db.GetResponse, error) {
	data := store.Get(in.SessionId, in.Keys, in.Loc, in.VirtualLoc)
	return &db.GetResponse{Data: data}, nil
}

func (s *Server) Set(ctx context.Context, in *db.SetRequest) (*db.SetResponse, error) {
	loc := store.Set(in.SessionId, in.Data, in.VirtualLoc, in.Dep, in.VirtualDep)
	return &db.SetResponse{Loc: loc}, nil
}

func (s *Server) GetMerkleTree(ctx context.Context, in *db.GetMerkleTreeRequest) (*db.GetMerkleTreeResponse, error) {
	nodes := store.GetMerkleTree(in.Location)
	return &db.GetMerkleTreeResponse{Nodes: nodes}, nil
}

func (s *Server) Download(ctx context.Context, in *db.DownloadRequest) (*db.DownloadResponse, error) {
	nodes := store.Download(in.Locations)
	return &db.DownloadResponse{Nodes: nodes}, nil
}

func (s *Server) Upload(ctx context.Context, in *db.UploadRequest) (*db.UploadResponse, error) {
	store.Upload(in.Nodes)
	return &db.UploadResponse{}, nil
}
