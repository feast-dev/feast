package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/kelseyhightower/envconfig"
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

type MemoryBuffer struct {
	logs []Log
}

// TODO: Add a proper logging library such as https://github.com/Sirupsen/logrus
func main() {
	// TODO(kevjumba) Figure out how large this log channel should be.
	logChannel := make(chan Log, 1000)
	defer close(logChannel)
	var logBuffer = MemoryBuffer{
		logs: make([]Log, 0),
	}

	var feastEnvConfig FeastEnvConfig
	var err error
	err = envconfig.Process("feast", &feastEnvConfig)
	if err != nil {
		log.Fatal(err)
	}
	if feastEnvConfig.RepoPath == "" && feastEnvConfig.RepoConfig == "" {
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
	if err != nil {
		log.Fatalln(err)
	}
	defer fs.DestructOnlineStore()
	startGrpcServer(fs, logChannel, &logBuffer, feastEnvConfig.SockFile)
}

func startGrpcServer(fs *feast.FeatureStore, logChannel chan Log, logBuffer *MemoryBuffer, sockFile string) {
	go processLogs(logChannel, logBuffer)
	server := newServingServiceServer(fs, logChannel)
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

func processLogs(log_channel chan Log, logBuffer *MemoryBuffer) {
	// start a periodic flush
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			log.Printf("time is %d", t)
			log.Printf("Flushing buffer to offline storage with channel length: %d\n", len(logBuffer.logs))
		case new_log := <-log_channel:
			log.Printf("Pushing %s to memory.\n", new_log.featureValues)
			logBuffer.logs = append(logBuffer.logs, new_log)
		}
	}
}
