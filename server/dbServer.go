package main

import (
	"log"
	"net"

	"github.com/DCsunset/openwhisk-grpc/db"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Println("Server listen at :9000")

	server := Server{}
	server.Init()
	grpcServer := grpc.NewServer()
	db.RegisterDbServiceServer(grpcServer, &server)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
