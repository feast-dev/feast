package main

import (
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/kelseyhightower/envconfig"
	"google.golang.org/grpc"
	"log"
	"net"
)

const (
	flagFeastRepoPath    = "FEAST_REPO_PATH"
	flagFeastRepoConfig  = "FEAST_REPO_CONFIG"
	flagFeastGrpcPort    = "FEAST_GRPC_PORT"
	feastServerVersion   = "0.18.0"
	defaultFeastHttpPort = "8081"
)

type FeastEnvConfig struct {
	RepoPath   string `envconfig:"FEAST_REPO_PATH"`
	RepoConfig string `envconfig:"FEAST_REPO_CONFIG"`
	GrpcPort   string `default:"6566" envconfig:"FEAST_GRPC_PORT"`
}

// TODO: Add a proper logging library such as https://github.com/Sirupsen/logrus

func main() {

	var feastEnvConfig FeastEnvConfig
	var err error
	err = envconfig.Process("feast", &feastEnvConfig)
	if err != nil {
		log.Fatal(err)
	}
	if feastEnvConfig.RepoPath == "" && feastEnvConfig.RepoConfig == "" {
		log.Fatalln(fmt.Sprintf("One of %s of %s environment variables must be set", flagFeastRepoPath, flagFeastRepoConfig))
	}
	// TODO(Ly): Review: Should we return and error here if both repoPath and repoConfigJson are set and use the cwd for NewRepoConfigFromJson?

	var repoConfig *feast.RepoConfig

	if len(feastEnvConfig.RepoConfig) > 0 {
		repoConfig, err = feast.NewRepoConfigFromJson(feastEnvConfig.RepoPath, feastEnvConfig.RepoConfig)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		repoConfig, err = feast.NewRepoConfigFromFile(feastEnvConfig.RepoPath)
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
	startGrpcServer(fs, feastEnvConfig.GrpcPort)
}

func startGrpcServer(fs *feast.FeatureStore, grpcPort string) {
	server := newServingServiceServer(fs)
	log.Printf("Starting a gRPC server at port %s...", grpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort))
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
