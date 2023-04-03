package embedded

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/cdata"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"google.golang.org/grpc"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/server"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/internal/feast/transformation"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
)

type OnlineFeatureService struct {
	fs         *feast.FeatureStore
	grpcStopCh chan os.Signal
	httpStopCh chan os.Signal

	statusColumnBuildersToRelease []*array.Int32Builder
	tsColumnBuildersToRelease     []*array.Int64Builder
	arraysToRelease               []arrow.Array
	resultsToRelease              []arrow.Record

	err error
}

type OnlineFeatureServiceConfig struct {
	RepoPath   string
	RepoConfig string
}

type DataTable struct {
	DataPtr   uintptr
	SchemaPtr uintptr
}

// LoggingOptions is a public (embedded) copy of logging.LoggingOptions struct.
// See logging.LoggingOptions for properties description
type LoggingOptions struct {
	ChannelCapacity int
	EmitTimeout     time.Duration
	WriteInterval   time.Duration
	FlushInterval   time.Duration
}

func NewOnlineFeatureService(conf *OnlineFeatureServiceConfig, transformationCallback transformation.TransformationCallback) *OnlineFeatureService {
	repoConfig, err := registry.NewRepoConfigFromJSON(conf.RepoPath, conf.RepoConfig)
	if err != nil {
		return &OnlineFeatureService{
			err: err,
		}
	}

	fs, err := feast.NewFeatureStore(repoConfig, transformationCallback)
	if err != nil {
		return &OnlineFeatureService{
			err: err,
		}
	}

	// Notify these channels when receiving interrupt or termination signals from OS
	httpStopCh := make(chan os.Signal, 1)
	grpcStopCh := make(chan os.Signal, 1)
	signal.Notify(httpStopCh, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(grpcStopCh, syscall.SIGINT, syscall.SIGTERM)

	return &OnlineFeatureService{fs: fs, httpStopCh: httpStopCh, grpcStopCh: grpcStopCh}
}

func (s *OnlineFeatureService) GetEntityTypesMap(featureRefs []string) (map[string]int32, error) {
	viewNames := make(map[string]interface{})
	for _, featureRef := range featureRefs {
		viewName, _, err := onlineserving.ParseFeatureReference(featureRef)
		if err != nil {
			return nil, err
		}
		viewNames[viewName] = nil
	}

	joinKeyTypes := make(map[string]int32)

	for viewName := range viewNames {
		view, err := s.fs.GetFeatureView(viewName, true)
		if err != nil {
			// skip on demand feature views
			continue
		}
		for _, entityColumn := range view.EntityColumns {
			joinKeyTypes[entityColumn.Name] = int32(entityColumn.Dtype.Number())
		}
	}

	return joinKeyTypes, nil
}

func (s *OnlineFeatureService) GetEntityTypesMapByFeatureService(featureServiceName string) (map[string]int32, error) {
	featureService, err := s.fs.GetFeatureService(featureServiceName)
	if err != nil {
		return nil, err
	}

	joinKeyTypes := make(map[string]int32)

	for _, projection := range featureService.Projections {
		view, err := s.fs.GetFeatureView(projection.Name, true)
		if err != nil {
			// skip on demand feature views
			continue
		}
		for _, entityColumn := range view.EntityColumns {
			joinKeyTypes[entityColumn.Name] = int32(entityColumn.Dtype.Number())
		}
	}

	return joinKeyTypes, nil
}

func (s *OnlineFeatureService) CheckForInstantiationError() error {
	return s.err
}

func (s *OnlineFeatureService) GetOnlineFeatures(
	featureRefs []string,
	featureServiceName string,
	entities DataTable,
	requestData DataTable,
	fullFeatureNames bool,
	output DataTable) error {

	entitiesRecord, err := readArrowRecord(entities)
	if err != nil {
		return err
	}
	defer entitiesRecord.Release()

	numRows := entitiesRecord.Column(0).Len()

	entitiesProto, err := recordToProto(entitiesRecord)
	if err != nil {
		return err
	}

	requestDataRecords, err := readArrowRecord(requestData)
	if err != nil {
		return err
	}
	defer requestDataRecords.Release()

	requestDataProto, err := recordToProto(requestDataRecords)
	if err != nil {
		return err
	}

	var featureService *model.FeatureService
	if featureServiceName != "" {
		featureService, err = s.fs.GetFeatureService(featureServiceName)
	}

	resp, err := s.fs.GetOnlineFeatures(
		context.Background(),
		featureRefs,
		featureService,
		entitiesProto,
		requestDataProto,
		fullFeatureNames)

	if err != nil {
		return err
	}

	// Release all objects that are no longer required.
	for _, statusColumnBuilderToRelease := range s.statusColumnBuildersToRelease {
		statusColumnBuilderToRelease.Release()
	}
	for _, tsColumnBuilderToRelease := range s.tsColumnBuildersToRelease {
		tsColumnBuilderToRelease.Release()
	}
	for _, arrayToRelease := range s.arraysToRelease {
		arrayToRelease.Release()
	}
	for _, resultsToRelease := range s.resultsToRelease {
		resultsToRelease.Release()
	}
	s.statusColumnBuildersToRelease = nil
	s.tsColumnBuildersToRelease = nil
	s.arraysToRelease = nil
	s.resultsToRelease = nil

	outputFields := make([]arrow.Field, 0)
	outputColumns := make([]arrow.Array, 0)
	pool := memory.NewCgoArrowAllocator()
	for _, featureVector := range resp {
		outputFields = append(outputFields,
			arrow.Field{
				Name: featureVector.Name,
				Type: featureVector.Values.DataType()})
		outputFields = append(outputFields,
			arrow.Field{
				Name: fmt.Sprintf("%s__status", featureVector.Name),
				Type: arrow.PrimitiveTypes.Int32})
		outputFields = append(outputFields,
			arrow.Field{
				Name: fmt.Sprintf("%s__timestamp", featureVector.Name),
				Type: arrow.PrimitiveTypes.Int64})

		outputColumns = append(outputColumns, featureVector.Values)

		statusColumnBuilder := array.NewInt32Builder(pool)
		for _, status := range featureVector.Statuses {
			statusColumnBuilder.Append(int32(status))
		}
		statusColumn := statusColumnBuilder.NewArray()
		outputColumns = append(outputColumns, statusColumn)

		tsColumnBuilder := array.NewInt64Builder(pool)
		for _, ts := range featureVector.Timestamps {
			tsColumnBuilder.Append(ts.GetSeconds())
		}
		tsColumn := tsColumnBuilder.NewArray()
		outputColumns = append(outputColumns, tsColumn)

		// Mark builders and arrays for release.
		s.statusColumnBuildersToRelease = append(s.statusColumnBuildersToRelease, statusColumnBuilder)
		s.tsColumnBuildersToRelease = append(s.tsColumnBuildersToRelease, tsColumnBuilder)
		s.arraysToRelease = append(s.arraysToRelease, statusColumn)
		s.arraysToRelease = append(s.arraysToRelease, tsColumn)
		s.arraysToRelease = append(s.arraysToRelease, featureVector.Values)
	}

	result := array.NewRecord(arrow.NewSchema(outputFields, nil), outputColumns, int64(numRows))
	s.resultsToRelease = append(s.resultsToRelease, result)

	cdata.ExportArrowRecordBatch(result, cdata.ArrayFromPtr(output.DataPtr), cdata.SchemaFromPtr(output.SchemaPtr))

	return nil
}

// StartGprcServer starts gRPC server with disabled feature logging and blocks the thread
func (s *OnlineFeatureService) StartGprcServer(host string, port int) error {
	return s.StartGprcServerWithLogging(host, port, nil, LoggingOptions{})
}

// StartGprcServerWithLoggingDefaultOpts starts gRPC server with enabled feature logging but default configuration for logging
// Caller of this function must provide Python callback to flush buffered logs
func (s *OnlineFeatureService) StartGprcServerWithLoggingDefaultOpts(host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback) error {
	defaultOpts := LoggingOptions{
		ChannelCapacity: logging.DefaultOptions.ChannelCapacity,
		EmitTimeout:     logging.DefaultOptions.EmitTimeout,
		WriteInterval:   logging.DefaultOptions.WriteInterval,
		FlushInterval:   logging.DefaultOptions.FlushInterval,
	}
	return s.StartGprcServerWithLogging(host, port, writeLoggedFeaturesCallback, defaultOpts)
}

func (s *OnlineFeatureService) constructLoggingService(writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts LoggingOptions) (*logging.LoggingService, error) {
	var loggingService *logging.LoggingService = nil
	if writeLoggedFeaturesCallback != nil {
		sink, err := logging.NewOfflineStoreSink(writeLoggedFeaturesCallback)
		if err != nil {
			return nil, err
		}

		loggingService, err = logging.NewLoggingService(s.fs, sink, logging.LoggingOptions{
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
// Caller of this function must provide Python callback to flush buffered logs as well as logging configuration (loggingOpts)
func (s *OnlineFeatureService) StartGprcServerWithLogging(host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts LoggingOptions) error {
	loggingService, err := s.constructLoggingService(writeLoggedFeaturesCallback, loggingOpts)
	if err != nil {
		return err
	}
	ser := server.NewGrpcServingServiceServer(s.fs, loggingService)
	log.Printf("Starting a gRPC server on host %s port %d\n", host, port)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	serving.RegisterServingServiceServer(grpcServer, ser)

	go func() {
		// As soon as these signals are received from OS, try to gracefully stop the gRPC server
		<-s.grpcStopCh
		log.Println("Stopping the gRPC server...")
		grpcServer.GracefulStop()
		if loggingService != nil {
			loggingService.Stop()
		}
		log.Println("gRPC server terminated")
	}()

	err = grpcServer.Serve(lis)
	if err != nil {
		return err
	}
	return nil
}

// StartHttpServer starts HTTP server with disabled feature logging and blocks the thread
func (s *OnlineFeatureService) StartHttpServer(host string, port int) error {
	return s.StartHttpServerWithLogging(host, port, nil, LoggingOptions{})
}

// StartHttpServerWithLoggingDefaultOpts starts HTTP server with enabled feature logging but default configuration for logging
// Caller of this function must provide Python callback to flush buffered logs
func (s *OnlineFeatureService) StartHttpServerWithLoggingDefaultOpts(host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback) error {
	defaultOpts := LoggingOptions{
		ChannelCapacity: logging.DefaultOptions.ChannelCapacity,
		EmitTimeout:     logging.DefaultOptions.EmitTimeout,
		WriteInterval:   logging.DefaultOptions.WriteInterval,
		FlushInterval:   logging.DefaultOptions.FlushInterval,
	}
	return s.StartHttpServerWithLogging(host, port, writeLoggedFeaturesCallback, defaultOpts)
}

// StartHttpServerWithLogging starts HTTP server with enabled feature logging
// Caller of this function must provide Python callback to flush buffered logs as well as logging configuration (loggingOpts)
func (s *OnlineFeatureService) StartHttpServerWithLogging(host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts LoggingOptions) error {
	loggingService, err := s.constructLoggingService(writeLoggedFeaturesCallback, loggingOpts)
	if err != nil {
		return err
	}
	ser := server.NewHttpServer(s.fs, loggingService)
	log.Printf("Starting a HTTP server on host %s port %d\n", host, port)

	go func() {
		// As soon as these signals are received from OS, try to gracefully stop the gRPC server
		<-s.httpStopCh
		log.Println("Stopping the HTTP server...")
		err := ser.Stop()
		if err != nil {
			log.Printf("Error when stopping the HTTP server: %v\n", err)
		}
		if loggingService != nil {
			loggingService.Stop()
		}
		log.Println("HTTP server terminated")
	}()

	return ser.Serve(host, port)
}

func (s *OnlineFeatureService) StopHttpServer() {
	s.httpStopCh <- syscall.SIGINT
}

func (s *OnlineFeatureService) StopGrpcServer() {
	s.grpcStopCh <- syscall.SIGINT
}

/*
Read Record Batch from memory managed by Python caller.
Python part uses C ABI interface to export this record into C Data Interface,
and then it provides pointers (dataPtr & schemaPtr) to the Go part.
Here we import this data from given pointers and wrap the underlying values
into Go Arrow Interface (array.Record).
See export code here https://github.com/feast-dev/feast/blob/master/sdk/python/feast/embedded_go/online_features_service.py
*/
func readArrowRecord(data DataTable) (arrow.Record, error) {
	return cdata.ImportCRecordBatch(
		cdata.ArrayFromPtr(data.DataPtr),
		cdata.SchemaFromPtr(data.SchemaPtr))
}

func recordToProto(rec arrow.Record) (map[string]*prototypes.RepeatedValue, error) {
	r := make(map[string]*prototypes.RepeatedValue)
	schema := rec.Schema()
	for idx, column := range rec.Columns() {
		field := schema.Field(idx)
		values, err := types.ArrowValuesToProtoValues(column)
		if err != nil {
			return nil, err
		}
		r[field.Name] = &prototypes.RepeatedValue{Val: values}
	}
	return r, nil
}
