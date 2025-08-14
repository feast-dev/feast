package main

import (
	"flag"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
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
	"github.com/feast-dev/feast/go/internal/feast/version"
	"github.com/rs/zerolog/log"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "go.uber.org/automaxprocs"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type ServerStarter interface {
	StartHttpServer(fs *feast.FeatureStore, host string, port int, loggingService *logging.LoggingService) error
	StartGrpcServer(fs *feast.FeatureStore, host string, port int, loggingService *logging.LoggingService) error
	StartHybridServer(fs *feast.FeatureStore, host string, httpPort int, grpcPort int, loggingService *logging.LoggingService) error
}

type RealServerStarter struct{}

func (s *RealServerStarter) StartHttpServer(fs *feast.FeatureStore, host string, port int, loggingService *logging.LoggingService) error {
	return StartHttpServer(fs, host, port, loggingService)
}

func (s *RealServerStarter) StartGrpcServer(fs *feast.FeatureStore, host string, port int, loggingService *logging.LoggingService) error {
	return StartGrpcServer(fs, host, port, loggingService)
}

func (s *RealServerStarter) StartHybridServer(fs *feast.FeatureStore, host string, httpPort int, grpcPort int, loggingService *logging.LoggingService) error {
	return StartHybridServer(fs, host, httpPort, grpcPort, loggingService)
}

func main() {
	// Default values
	serverType := "http"
	host := ""
	port := 8080
	grpcPort := 6566
	serverStarter := RealServerStarter{}
	printVersion := false
	// Current Directory
	repoPath, err := os.Getwd()
	if err != nil {
		log.Error().Stack().Err(err).Msg("Failed to get current directory")
	}

	flag.StringVar(&serverType, "type", serverType, "Specify the serverStarter type (http, grpc, or hybrid)")
	flag.StringVar(&repoPath, "chdir", repoPath, "Repository path where feature store yaml file is stored")

	flag.StringVar(&host, "host", host, "Specify a host for the serverStarter")
	flag.IntVar(&port, "port", port, "Specify a port for the serverStarter")
	flag.IntVar(&grpcPort, "grpcPort", grpcPort, "Specify a grpc port for the serverStarter")
	flag.BoolVar(&printVersion, "version", printVersion, "Print the version information and exit")
	flag.Parse()

	version.ServerType = serverType

	versionInfo := version.GetVersionInfo()

	if printVersion && flag.NFlag() == 1 && flag.NArg() == 0 {
		fmt.Printf("Feature Server Version: %s\nBuild Time: %s\nCommit Hash: %s\nGo Version: %s\n",
			versionInfo.Version, versionInfo.BuildTime, versionInfo.CommitHash, versionInfo.GoVersion)
		os.Exit(0)
	}

	log.Info().Msgf("Feature Server Version: %s", versionInfo.Version)
	log.Info().Msgf("Build Time: %s", versionInfo.BuildTime)
	log.Info().Msgf("Commit Hash: %s", versionInfo.CommitHash)
	log.Info().Msgf("Go Version: %s", versionInfo.GoVersion)
	log.Info().Msgf("Server Type: %s", versionInfo.ServerType)

	go publishVersionInfoToDatadog(versionInfo)

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

	loggingService, err := constructLoggingService(fs, nil, loggingOptions)
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to create loggingService")
	}

	// TODO: writeLoggedFeaturesCallback is defaulted to nil. write_logged_features functionality needs to be
	// implemented in Golang specific to OfflineStoreSink. Python Feature Server doesn't support this.
	switch serverType {
	case "http":
		err = serverStarter.StartHttpServer(fs, host, port, loggingService)
	case "grpc":
		err = serverStarter.StartGrpcServer(fs, host, port, loggingService)
	case "hybrid":
		// hybrid starts both gRPC(on gRPC port) & http(on port)
		err = serverStarter.StartHybridServer(fs, host, port, grpcPort, loggingService)
	default:
		fmt.Println("Unknown serverStarter type. Please specify 'http', 'grpc', or 'hybrid'.")
	}

	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to start serverStarter")
	}

}

func datadogTracingEnabled() bool {
	return strings.ToLower(os.Getenv("ENABLE_DATADOG_TRACING")) == "true"
}

func publishVersionInfoToDatadog(info *version.Info) {
	if datadogTracingEnabled() {
		if statsdHost, ok := os.LookupEnv("DD_AGENT_HOST"); ok {
			var client, err = statsd.New(fmt.Sprintf("%s:8125", statsdHost))
			if err != nil {
				log.Error().Err(err).Msg("Failed to connect to statsd")
				return
			}
			defer func(client *statsd.Client) {
				var err error
				err = client.Flush()
				if err != nil {
					log.Error().Err(err).Msg("Failed to flush heartbeat to statsd client")
				}
				err = client.Close()
				if err != nil {
					log.Error().Err(err).Msg("Failed to close statsd client")
				}
			}(client)
			tags := []string{
				"feast_version:" + info.Version,
				"build_time:" + info.BuildTime,
				"commit_hash:" + info.CommitHash,
				"go_version:" + info.GoVersion,
				"server_type:" + info.ServerType,
			}
			err = client.Gauge("featureserver.heartbeat", 1, tags, 1)
			if err != nil {
				log.Error().Err(err).Msg("Failed to publish feature server heartbeat info to datadog")
			}
		} else {
			log.Info().Msg("DD_AGENT_HOST environment variable is not set, skipping publishing version info to Datadog")
		}
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

// StartGrpcServerWithLogging creates a gRPC server with enabled feature logging
func StartGrpcServer(fs *feast.FeatureStore, host string, port int, loggingService *logging.LoggingService) error {
	if datadogTracingEnabled() {
		tracer.Start(tracer.WithRuntimeMetrics())
		defer tracer.Stop()
	}

	ser := server.NewGrpcServingServiceServer(fs, loggingService)
	log.Info().Msgf("Starting a gRPC server on host %s port %d", host, port)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}

	grpcServer := ser.RegisterServices()

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

// StartHttpServerWithLogging creates an HTTP server with enabled feature logging
// Go does not allow direct assignment to package-level functions as a way to
// mock them for tests
func StartHttpServer(fs *feast.FeatureStore, host string, port int, loggingService *logging.LoggingService) error {
	ser := server.NewHttpServer(fs, loggingService)
	log.Info().Msgf("Starting a HTTP server on host %s, port %d", host, port)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// As soon as these signals are received from OS, try to gracefully stop the HTTP server
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

	return ser.Serve(host, port, server.DefaultHttpHandlers(ser))
}

// StartHybridServer creates a gRPC Server and HTTP server
// Handlers for these are defined in hybrid_server.go
// Stops both servers if a stop signal is received.
func StartHybridServer(fs *feast.FeatureStore, host string, httpPort int, grpcPort int, loggingService *logging.LoggingService) error {
	if datadogTracingEnabled() {
		tracer.Start(tracer.WithRuntimeMetrics())
		defer tracer.Stop()
	}

	ser := server.NewGrpcServingServiceServer(fs, loggingService)
	log.Info().Msgf("Starting a gRPC server on host %s port %d", host, grpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, grpcPort))
	if err != nil {
		return err
	}

	grpcSer := ser.RegisterServices()

	httpSer := server.NewHttpServer(fs, loggingService)
	log.Info().Msgf("Starting a HTTP server on host %s, port %d", host, httpPort)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		log.Info().Msg("Stopping the HTTP server...")
		err := httpSer.Stop()
		if err != nil {
			log.Error().Err(err).Msg("Error when stopping the HTTP server")
		}

		log.Info().Msg("Stopping the gRPC server...")
		grpcSer.GracefulStop()

		if loggingService != nil {
			loggingService.Stop()
		}
		log.Info().Msg("HTTP and gRPC servers terminated")
	}()

	go func() {
		if err := httpSer.Serve(host, httpPort, server.DefaultHybridHandlers(httpSer, grpcPort)); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("HTTP server failed")
		}
	}()

	if err != nil {
		return err
	}

	return grpcSer.Serve(lis)
}
