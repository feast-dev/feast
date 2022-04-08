package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/feast-dev/feast/go/cmd/server/logging"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
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

	var repoConfig *registry.RepoConfig
	var err error
	if repoConfigJSON != "" {
		repoConfig, err = registry.NewRepoConfigFromJSON(repoPath, repoConfigJSON)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		repoConfig, err = registry.NewRepoConfigFromFile(repoPath)
		if err != nil {
			log.Fatalln(err)
		}
	}

	log.Println("Initializing feature store...")
	fs, err := feast.NewFeatureStore(repoConfig)
	if err != nil {
		log.Fatalln(err)
	}
	// Disable logging for now
	loggingService, err := logging.NewLoggingService(fs, 1000, "", false)
	if err != nil {
		log.Fatalln(err)
	}
	defer fs.DestructOnlineStore()
	startGrpcServer(fs, loggingService, sockFile)
}

func startGrpcServer(fs *feast.FeatureStore, loggingService *logging.LoggingService, sockFile string) {
	server := newServingServiceServer(fs, loggingService)
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
