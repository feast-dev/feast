package main

import (
	"fmt"
	"github.com/feast-dev/feast/go/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"path/filepath"
)

const (
	flagFeastRepoPath    = "FEAST_REPO_PATH"
	flagFeastGrpcPort    = "FEAST_GRPC_PORT"
	defaultFeastGrpcPort = "6566"
	feastServerVersion   = "0.18.0"
)

func main() {
	repoPath := os.Getenv(flagFeastRepoPath)
	grpcPort, ok := os.LookupEnv(flagFeastGrpcPort)
	if !ok {
		grpcPort = defaultFeastGrpcPort
	}
	log.Printf("Reading Feast repo config at %s", filepath.Join(repoPath, "feature_store.yaml"))
	config, err := feast.NewRepoConfig(repoPath)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Initializing feature store...")
	fs, err := feast.NewFeatureStore(config)
	if err != nil {
		log.Fatalln(err)
	}
	server := servingServiceServer{
		fs: fs,
	}
	log.Printf("Starting a gRPC server at port %s...", grpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort))
	if err != nil {
		log.Fatalln(err)
	}
	grpcServer := grpc.NewServer()
	serving.RegisterServingServiceServer(grpcServer, &server)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalln(err)
	}
	// TODO: implement HTTP server endpoint
}
