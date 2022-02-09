package main

// THIS WORKS

import (
	"fmt"
	"github.com/feast-dev/feast/go/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"path/filepath"
	"net/http"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc/credentials/insecure"
	"context"
	"sync"
	"github.com/golang/glog"
	"strings"
)

const (
	flagFeastRepoPath    = "FEAST_REPO_PATH"
	flagFeastGrpcPort    = "FEAST_GRPC_PORT"
	defaultFeastGrpcPort = "6566"
	feastServerVersion   = "0.18.0"
	defaultFeastHttpPort = "8081"
)

var wg sync.WaitGroup

func main() {
	repoPath := os.Getenv(flagFeastRepoPath)
	grpcPort, ok := os.LookupEnv(flagFeastGrpcPort)
	if !ok {
		grpcPort = defaultFeastGrpcPort
	}
	log.Printf("Reading Feast repo config at %s", filepath.Join(repoPath, "feature_store.yaml"))
	config, err := feast.NewRepoConfig(repoPath)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Initializing feature store...")
	fs, err := feast.NewFeatureStore(config)
	if err != nil {
		log.Fatalln(err)
	}
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
	wg.Add(1)
	go func () {
		defer wg.Done()
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatalln(err)
		}
	}()

	// Implement HTTP server Endpoint
	log.Printf("Starting a HTTP server at port %s...", defaultFeastHttpPort)
	if err = runHttp(fmt.Sprintf(":%s", grpcPort)); err != nil {
		log.Fatalln(err)
	}
	wg.Wait()
}

func runHttp(grpcServerEndpoint string) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
  
	// Register gRPC server endpoint
	// Note: Make sure the gRPC server is running properly and accessible
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	err := serving.RegisterServingServiceHandlerFromEndpoint(ctx, mux,  grpcServerEndpoint, opts)
	if err != nil {
	  return err
	}

	return http.ListenAndServe(fmt.Sprintf(":%s", defaultFeastHttpPort), allowCORS(mux))
}

func preflightHandler(w http.ResponseWriter, r *http.Request) {
	headers := []string{"Content-Type", "Accept"}
	w.Header().Set("Access-Control-Allow-Headers", strings.Join(headers, ","))
	methods := []string{"GET", "HEAD", "POST", "PUT", "DELETE"}
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(methods, ","))
	glog.Infof("preflight request for %s", r.URL.Path)
	return
}

// allowCORS allows Cross Origin Resoruce Sharing from any origin.
// Don't do this without consideration in production systems.
func allowCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			if r.Method == "OPTIONS" && r.Header.Get("Access-Control-Request-Method") != "" {
				preflightHandler(w, r)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}
