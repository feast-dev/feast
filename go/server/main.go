package main

// THIS WORKS

import (
	"fmt"
	"github.com/feast-dev/feast/go/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"bufio"
	"errors"
	"io"
)

const (
	flagFeastRepoPath    = "FEAST_REPO_PATH"
	flagFeastRepoConfig  = "FEAST_REPO_CONFIG"
	flagFeastGrpcPort    = "FEAST_GRPC_PORT"
	defaultFeastGrpcPort = "6566"
	feastServerVersion   = "0.18.0"
	defaultFeastHttpPort = "8081"
)

var wg sync.WaitGroup

func main() {
	repoPath := os.Getenv(flagFeastRepoPath)
	repoConfig := os.Getenv(flagFeastRepoConfig)
	
	if repoPath == "" && repoConfig == "" {
		log.Fatalln(fmt.Sprintf("One of %s of %s environment variables must be set", flagFeastRepoPath, flagFeastRepoConfig))
	}
	config, err := feast.NewRepoConfig(repoPath, repoConfig)
	log.Println("Initializing feature store...")
	fs, err := feast.NewFeatureStore(config)
	if err != nil {
		log.Fatalln(err)
	}

	grpcPort, ok := os.LookupEnv(flagFeastGrpcPort)
	if !ok {
		grpcPort = defaultFeastGrpcPort
	}
	
	// fmt.Println("starting for loop")
	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	serverCounts := 0
	for {
	
		text, _ := reader.ReadString('\n')
		text = strings.Trim(text, "\n")
		// fmt.Println("Received from stdin", text)
		commands := strings.Split(text, " ")
		if len(commands) == 0 {
			log.Fatalln(errors.New("Invalid command. Should be [startGrpc] or [startHttp host:port]"))
		} else if commands[0] == "startGrpc" {
			// fmt.Fprintf(writer, "Success!")
			writer.Flush()
			wg.Add(1)
			go func() {
				defer wg.Done()
				// startGrpcServer(fs, grpcPort)
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
				err = grpcServer.Serve(lis)
				if err != nil {
					log.Fatalln(err)
				}
			}()
			serverCounts += 1
		} else if commands[0] == "startHttp" {
			// fmt.Fprintf(writer, "Success!")
			writer.Flush()
			if len(commands) < 2 {
				log.Fatalln(errors.New("Invalid command. Should be: startHttp host:port"))
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				startHttpServer(fs, commands[1])
			}()
			serverCounts += 1
		}
		if serverCounts == 2 {
			break
		}
	}
	wg.Wait()
}



func startGrpcServer(fs *feast.FeatureStore, grpcPort string) {
	fmt.Println("startGrpcServer", grpcPort)
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
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalln(err)
	}
}

func startHttpServer(fs *feast.FeatureStore, address string) {
	fmt.Println("startHttpServer", address)
    http.HandleFunc("/get-online-features", func(w http.ResponseWriter, req *http.Request){
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

    log.Fatal(http.ListenAndServe(address, nil))
}