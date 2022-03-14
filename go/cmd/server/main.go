package main

import (
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

const (
	flagFeastRepoPath   = "FEAST_REPO_PATH"
	flagFeastRepoConfig = "FEAST_REPO_CONFIG"
	flagFeastSockFile   = "FEAST_GRPC_SOCK_FILE"
	feastServerVersion  = "0.18.0"
)

// TODO: Add a proper logging library such as https://github.com/Sirupsen/logrus
func main() {
	repoPath := os.Getenv(flagFeastRepoPath)
	repoConfigJSON := os.Getenv(flagFeastRepoConfig)
	sockFile := os.Getenv(flagFeastSockFile)
	if repoPath == "" && repoConfigJSON == "" {
		log.Fatalln(fmt.Sprintf("One of %s of %s environment variables must be set", flagFeastRepoPath, flagFeastRepoConfig))
	}

	var repoConfig *feast.RepoConfig
	var err error
	if repoConfigJSON != "" {
		repoConfig, err = feast.NewRepoConfigFromJSON(repoPath, repoConfigJSON)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		repoConfig, err = feast.NewRepoConfigFromFile(repoPath)
		if err != nil {
			log.Fatalln(err)
		}
	}

	log.Println("Initializing feature store...")
	fs, err := feast.NewFeatureStore(repoConfig)
	if err != nil {
		log.Fatalln(err)
	}
	defer fs.DestructOnlineStore()
	startGrpcServer(fs, sockFile)
}

func startGrpcServer(fs *feast.FeatureStore, sockFile string) {
	server := newServingServiceServer(fs)
	log.Printf("Starting a gRPC server listening on %s\n", sockFile)
	lis, err := net.Listen("unix", sockFile)
	if err != nil {
		log.Fatalln(err)
	}
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	serving.RegisterServingServiceServer(grpcServer, server)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalln(err)
	}
}
