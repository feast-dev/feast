package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/server"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

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

	// Initial tracer
	if OTELTracingEnabled() {
		ctx := context.Background()

		exp, err := newExporter(ctx)
		if err != nil {
			log.Fatal().Stack().Err(err).Msg("Failed to initialize exporter.")
		}

		// Create a new tracer provider with a batch span processor and the given exporter.
		tp, err := newTracerProvider(exp)
		if err != nil {
			log.Fatal().Stack().Err(err).Msg("Failed to initialize tracer provider.")
		}

		// Handle shutdown properly so nothing leaks.
		defer func() { _ = tp.Shutdown(ctx) }()

		otel.SetTracerProvider(tp)

		// Finally, set the tracer that can be used for this package.
		tracer = tp.Tracer("github.com/feast-dev/feast/go")
	}

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

	grpcServer := grpc.NewServer()
	serving.RegisterServingServiceServer(grpcServer, ser)
	healthService := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthService)

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

func OTELTracingEnabled() bool {
	return strings.ToLower(os.Getenv("ENABLE_OTEL_TRACING")) == "true"
}

func newExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	return exp, nil
}

func newTracerProvider(exp sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("FeastGoFeatureServer"),
		),
	)

	if err != nil {
		return nil, err
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	), nil
}

