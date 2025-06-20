package server

import (
	"context"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"path/filepath"
	"runtime"
	"time"
)

// Starts a new grpc server, registers the serving service and returns a client.
func getClient(ctx context.Context, offlineStoreType string, basePath string, logPath string) (serving.ServingServiceClient, func()) {
	buffer := 1024 * 1024
	listener := bufconn.Listen(buffer)

	server := grpc.NewServer()
	config, err := registry.NewRepoConfigFromFile(getRepoPath(basePath))
	if err != nil {
		panic(err)
	}
	fs, err := feast.NewFeatureStore(config, nil)
	if err != nil {
		panic(err)
	}

	var logSink logging.LogSink
	if logPath != "" {
		logSink, err = logging.NewFileLogSink(logPath)
		if err != nil {
			panic(err)
		}
	}
	loggingService, err := logging.NewLoggingService(fs, logSink, logging.LoggingOptions{
		WriteInterval:   10 * time.Millisecond,
		FlushInterval:   logging.DefaultOptions.FlushInterval,
		EmitTimeout:     logging.DefaultOptions.EmitTimeout,
		ChannelCapacity: logging.DefaultOptions.ChannelCapacity,
	})
	if err != nil {
		panic(err)
	}
	servingServiceServer := NewGrpcServingServiceServer(fs, loggingService)

	serving.RegisterServingServiceServer(server, servingServiceServer)
	go func() {
		if err := server.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithInsecure())

	closer := func() {
		listener.Close()
		server.Stop()
	}

	client := serving.NewServingServiceClient(conn)

	return client, closer
}

// Return absolute path to the test_repo directory regardless of the working directory
func getRepoPath(basePath string) string {
	// Get the file path of this source file, regardless of the working directory
	if basePath == "" {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			panic("couldn't find file path of the test file")
		}
		return filepath.Join(filename, "..", "..", "feature_repo")
	} else {
		return filepath.Join(basePath, "feature_repo")
	}
}
