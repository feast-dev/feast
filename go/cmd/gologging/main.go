package logging

import (
	"log"
	"net"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
)

const (
	flagFeastRepoPath   = "FEAST_REPO_PATH"
	flagFeastRepoConfig = "FEAST_REPO_CONFIG"
	feastServerVersion  = "0.18.0"
)

type FeastEnvConfig struct {
	RepoPath   string `envconfig:"FEAST_REPO_PATH"`
	RepoConfig string `envconfig:"FEAST_REPO_CONFIG"`
	SockFile   string `envconfig:"FEAST_GRPC_SOCK_FILE"`
}

type servingServiceServer struct {
	fs *feast.FeatureStore
	serving.UnimplementedServingServiceServer
}

func main() {
	log_buffer := make(chan string, 1000)
	startGrpcServer(log_buffer, "9000")
}

func startGrpcServer(channel chan string, port string) {
	server := newLoggingServiceServer(&channel)
	// make new log service
	log.Printf("Starting a gRPC server listening on %s\n", port)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalln(err)
	}
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	serving.RegisterLoggingServiceServer(grpcServer, server)
	// register the servicing service with the grpc server
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalln(err)
	}
}

func processLogs() {
	// start a periodic flush
	for {
		// select function that either receives from channel or starts a subprocess to flush
		// probably need to wait for that
	}
}
