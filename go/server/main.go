package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strings"
)

const (
	flagFeastRepoPath    = "FEAST_REPO_PATH"
	flagFeastRepoConfig  = "FEAST_REPO_CONFIG"
	flagFeastGrpcPort    = "FEAST_GRPC_PORT"
	defaultFeastGrpcPort = "6566"
	feastServerVersion   = "0.18.0"
	defaultFeastHttpPort = "8081"
)

func main() {

	repoPath := os.Getenv(flagFeastRepoPath)
	repoConfig := os.Getenv(flagFeastRepoConfig)

	if repoPath == "" && repoConfig == "" {
		log.Fatalln(fmt.Sprintf("One of %s of %s environment variables must be set", flagFeastRepoPath, flagFeastRepoConfig))
		return
	}
	config, err := feast.NewRepoConfig(repoPath, repoConfig)
	if err != nil {
		log.Fatalln(err)
		return
	}
	log.Println("Initializing feature store...")
	fs, err := feast.NewFeatureStore(config)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer fs.DestructOnlineStore()

	grpcPort, ok := os.LookupEnv(flagFeastGrpcPort)
	if !ok {
		grpcPort = defaultFeastGrpcPort
	}

	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	serverCounts := 0
	for {
		text, _ := reader.ReadString('\n')
		text = strings.Trim(text, "\n")
		commands := strings.Split(text, " ")
		if len(commands) == 0 {
			log.Fatalln(errors.New("Invalid command. Should be [startGrpc] or [stop]"))
			return
		} else if commands[0] == "startGrpc" {
			writer.Flush()
			go func() {
				startGrpcServer(fs, grpcPort)
			}()
			serverCounts += 1
		} else if commands[0] == "stop" {
			break
		}
	}
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

	serving.RegisterServingServiceServer(grpcServer, &server)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalln(err)
		return
	}
}
