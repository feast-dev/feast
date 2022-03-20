package main

import (
	"fmt"
	"log"
	"net"
	"os"

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

type FeastEnvConfig struct {
	RepoPath   string `envconfig:"FEAST_REPO_PATH"`
	RepoConfig string `envconfig:"FEAST_REPO_CONFIG"`
	SockFile   string `envconfig:"FEAST_GRPC_SOCK_FILE"`
}

// TODO: Add a proper logging library such as https://github.com/Sirupsen/logrus
func main() {
	// TODO(kevjumba) Figure out how large this log channel should be.
	logChannel := make(chan Log, 1000)
	defer close(logChannel)

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
	fs, err := feast.NewFeatureStore(repoConfig, nil)
	loggingService := NewLoggingService(fs)
	if err != nil {
		log.Fatalln(err)
	}
	defer fs.DestructOnlineStore()
	startGrpcServer(fs, loggingService, sockFile)
}

func startGrpcServer(fs *feast.FeatureStore, loggingService *LoggingService, sockFile string) {
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
