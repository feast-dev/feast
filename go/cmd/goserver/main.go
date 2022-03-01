package main

import (
	"fmt"
	"github.com/feast-dev/feast/go/internal/config"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

const (
	flagFeastRepoPath    = "FEAST_REPO_PATH"
	flagFeastRepoConfig  = "FEAST_REPO_CONFIG"
	flagFeastGrpcPort    = "FEAST_GRPC_PORT"
	defaultFeastGrpcPort = "6566"
	feastServerVersion   = "0.18.0"
	defaultFeastHttpPort = "8081"
)

// TODO: Add a proper logging library such as https://github.com/Sirupsen/logrus

func main() {

	repoPath := os.Getenv(flagFeastRepoPath)
	repoConfigJson := os.Getenv(flagFeastRepoConfig)

	if repoPath == "" && repoConfigJson == "" {
		log.Fatalln(fmt.Sprintf("One of %s of %s environment variables must be set", flagFeastRepoPath, flagFeastRepoConfig))
		return
	}
	// TODO(Ly): Review: Should we return and error here if both repoPath and repoConfigJson are set and use the cwd for NewRepoConfigFromJson?

	var repoConfig *config.RepoConfig
	var err error
	if len(repoConfigJson) > 0 {
		repoConfig, err = config.NewRepoConfigFromJson(repoPath, repoConfigJson)
		if err != nil {
			log.Fatalln(err)
			return
		}
	} else {
		repoConfig, err = config.NewRepoConfigFromFile(repoPath)
		if err != nil {
			log.Fatalln(err)
			return
		}
	}

	log.Println("Initializing feature store...")
	fs, err := feast.NewFeatureStore(repoConfig)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer fs.DestructOnlineStore()

	grpcPort, ok := os.LookupEnv(flagFeastGrpcPort)
	if !ok {
		grpcPort = defaultFeastGrpcPort
	}
	startGrpcServer(fs, grpcPort)
}

func startGrpcServer(fs *feast.FeatureStore, grpcPort string) {
	server := servingServiceServer{
		fs: fs,
	}
	log.Printf("Starting a gRPC server at port %s...", grpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort))
	if err != nil {
		log.Fatalln(err)
		return
	}
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	serving.RegisterServingServiceServer(grpcServer, &server)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalln(err)
		return
	}
}
