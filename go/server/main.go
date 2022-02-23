package main

// THIS WORKS

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"log"
	"net"
	"net/http"
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
			log.Fatalln(errors.New("Invalid command. Should be [startGrpc] or [startHttp host:port] or [stop]"))
			return
		} else if commands[0] == "startGrpc" {
			writer.Flush()
			go func() {
				startGrpcServer(fs, grpcPort)
			}()
			serverCounts += 1
		} else if commands[0] == "startHttp" {
			writer.Flush()
			if len(commands) < 2 {
				log.Fatalln(errors.New("Invalid command. Should be: startHttp host:port"))
				return
			}
			go func() {
				startHttpServer(fs, commands[1])
			}()
			serverCounts += 1
		} else if commands[0] == "stop" {
			break
		}
	}
	log.Println("end server from go")
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

func startHttpServer(fs *feast.FeatureStore, address string) {
	http.HandleFunc("/get-online-features", func(w http.ResponseWriter, req *http.Request) {
		reqBodyBytes, err := io.ReadAll(req.Body)
		var grpcRequest serving.GetOnlineFeaturesRequest
		if err = protojson.Unmarshal(reqBodyBytes, &grpcRequest); err != nil {
			// panic(err)
			http.Error(w, err.Error(), 500)
		} else {
			response, err := fs.GetOnlineFeatures(&grpcRequest)
			if err != nil {
				http.Error(w, err.Error(), 500)
			}
			responseBytes, err := protojson.Marshal(response)
			if err != nil {
				http.Error(w, err.Error(), 500)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(responseBytes)
			}

		}
	})
	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Fatalln(err)
		return
	}
}
