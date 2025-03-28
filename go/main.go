package main

import (
	"flag"
	"fmt"
	"google.golang.org/grpc/reflection"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/server"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	grpcPrometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "go.uber.org/automaxprocs"
	grpcTrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type ServerStarter interface {
	StartHttpServer(fs *feast.FeatureStore, host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error
	StartGrpcServer(fs *feast.FeatureStore, host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error
}

type RealServerStarter struct{}

func (s *RealServerStarter) StartHttpServer(fs *feast.FeatureStore, host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error {
	return StartHttpServer(fs, host, port, writeLoggedFeaturesCallback, loggingOpts)
}

func (s *RealServerStarter) StartGrpcServer(fs *feast.FeatureStore, host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error {
	return StartGrpcServer(fs, host, port, writeLoggedFeaturesCallback, loggingOpts)
}

func main() {
	// Default values
	serverType := "http"
	host := ""
	port := 8080
	server := RealServerStarter{}
	// Current Directory
	repoPath, err := os.Getwd()
	if err != nil {
		log.Error().Stack().Err(err).Msg("Failed to get current directory")
	}

	flag.StringVar(&serverType, "type", serverType, "Specify the server type (http or grpc)")
	flag.StringVar(&repoPath, "chdir", repoPath, "Repository path where feature store yaml file is stored")

	flag.StringVar(&host, "host", host, "Specify a host for the server")
	flag.IntVar(&port, "port", port, "Specify a port for the server")
	flag.Parse()

	repoConfig, err := registry.NewRepoConfigFromFile(repoPath)
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to convert to RepoConfig")
	}

	fs, err := feast.NewFeatureStore(repoConfig, nil)
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to create NewFeatureStore")
	}

	loggingOptions, err := repoConfig.GetLoggingOptions()
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to get LoggingOptions")
	}

	// TODO: writeLoggedFeaturesCallback is defaulted to nil. write_logged_features functionality needs to be
	// implemented in Golang specific to OfflineStoreSink. Python Feature Server doesn't support this.
	if serverType == "http" {
		err = server.StartHttpServer(fs, host, port, nil, loggingOptions)
	} else if serverType == "grpc" {
		err = server.StartGrpcServer(fs, host, port, nil, loggingOptions)
	} else {
		fmt.Println("Unknown server type. Please specify 'http' or 'grpc'.")
	}

	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to start server")
	}

}

func constructLoggingService(fs *feast.FeatureStore, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) (*logging.LoggingService, error) {
	var loggingService *logging.LoggingService = nil
	if writeLoggedFeaturesCallback != nil {
		sink, err := logging.NewOfflineStoreSink(writeLoggedFeaturesCallback)
		if err != nil {
			return nil, err
		}

		loggingService, err = logging.NewLoggingService(fs, sink, logging.LoggingOptions{
			ChannelCapacity: loggingOpts.ChannelCapacity,
			EmitTimeout:     loggingOpts.EmitTimeout,
			WriteInterval:   loggingOpts.WriteInterval,
			FlushInterval:   loggingOpts.FlushInterval,
		})
		if err != nil {
			return nil, err
		}
	}
	return loggingService, nil
}

// StartGprcServerWithLogging starts gRPC server with enabled feature logging
func StartGrpcServer(fs *feast.FeatureStore, host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error {
	if strings.ToLower(os.Getenv("ENABLE_DATADOG_TRACING")) == "true" {
		tracer.Start(tracer.WithRuntimeMetrics())
		defer tracer.Stop()
	}
	loggingService, err := constructLoggingService(fs, writeLoggedFeaturesCallback, loggingOpts)
	if err != nil {
		return err
	}
	ser := server.NewGrpcServingServiceServer(fs, loggingService)
	log.Info().Msgf("Starting a gRPC server on host %s port %d", host, port)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}

	grpcPromMetrics := grpcPrometheus.NewServerMetrics()
	prometheus.MustRegister(grpcPromMetrics)
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpcTrace.UnaryServerInterceptor(), grpcPromMetrics.UnaryServerInterceptor()),
	)
	serving.RegisterServingServiceServer(grpcServer, ser)
	healthService := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthService)
	reflection.Register(grpcServer)

	// Running Prometheus metrics endpoint on a separate goroutine
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Info().Msg("Starting metrics endpoint on port 8080")
		log.Fatal().Stack().Err(http.ListenAndServe(":8080", nil))
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// As soon as these signals are received from OS, try to gracefully stop the gRPC server
		<-stop
		log.Info().Msg("Stopping the gRPC server...")
		grpcServer.GracefulStop()
		if loggingService != nil {
			loggingService.Stop()
		}
		log.Info().Msg("gRPC server terminated")
	}()

	return grpcServer.Serve(lis)
}

// StartHttpServerWithLogging starts HTTP server with enabled feature logging
// Go does not allow direct assignment to package-level functions as a way to
// mock them for tests
func StartHttpServer(fs *feast.FeatureStore, host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error {
	loggingService, err := constructLoggingService(fs, writeLoggedFeaturesCallback, loggingOpts)
	if err != nil {
		return err
	}
	ser := server.NewHttpServer(fs, loggingService)
	log.Info().Msgf("Starting a HTTP server on host %s, port %d", host, port)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// As soon as these signals are received from OS, try to gracefully stop the gRPC server
		<-stop
		log.Info().Msg("Stopping the HTTP server...")
		err := ser.Stop()
		if err != nil {
			log.Error().Err(err).Msg("Error when stopping the HTTP server")
		}
		if loggingService != nil {
			loggingService.Stop()
		}
		log.Info().Msg("HTTP server terminated")
	}()

	return ser.Serve(host, port)
}
