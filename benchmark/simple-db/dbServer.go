package main

import (
	"log"
	"net"

	simpleDb "github.com/DCsunset/openwhisk-grpc/simple-db"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":9001")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Println("Server listen at :9001")

	server := Server{}
	grpcServer := grpc.NewServer()
	simpleDb.RegisterDbServiceServer(grpcServer, &server)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
