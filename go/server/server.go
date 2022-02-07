package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"path/filepath"
)

type servingServiceServer struct {
	fs *feast.FeatureStore
}

func (s *servingServiceServer) GetFeastServingInfo(ctx context.Context, request *serving.GetFeastServingInfoRequest) (*serving.GetFeastServingInfoResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *servingServiceServer) GetOnlineFeatures(ctx context.Context, request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {
	return nil, errors.New("not implemented")
}

func main() {
	repoPath := os.Getenv("FEAST_REPO_PATH")
	grpcPort, ok := os.LookupEnv("FEAST_GRPC_PORT")
	if !ok {
		grpcPort = "6566"
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
	grpcServer.Serve(lis)
	// TODO: implement HTTP server endpoint
}
