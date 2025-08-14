package server

import (
	"context"
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/errors"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/internal/feast/version"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/reflection"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	grpcPrometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcTrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
)

const feastServerVersion = "0.0.1"

type grpcServingServiceServer struct {
	fs             *feast.FeatureStore
	loggingService *logging.LoggingService
	serving.UnimplementedServingServiceServer
}

func NewGrpcServingServiceServer(fs *feast.FeatureStore, loggingService *logging.LoggingService) *grpcServingServiceServer {
	return &grpcServingServiceServer{fs: fs, loggingService: loggingService}
}

func (s *grpcServingServiceServer) GetFeastServingInfo(ctx context.Context, request *serving.GetFeastServingInfoRequest) (*serving.GetFeastServingInfoResponse, error) {
	return &serving.GetFeastServingInfoResponse{
		Version: feastServerVersion,
	}, nil
}

// GetVersionInfo Returns GO Binary Version Information
func (s *grpcServingServiceServer) GetVersionInfo(ctx context.Context, request *serving.GetVersionInfoRequest) (*serving.GetVersionInfoResponse, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "gerVersionInfo", tracer.ResourceName("ServingService/GetVersionInfo"))
	defer span.Finish()

	versionInfo := version.GetVersionInfo()
	return &serving.GetVersionInfoResponse{
		Version:    versionInfo.Version,
		BuildTime:  versionInfo.BuildTime,
		CommitHash: versionInfo.CommitHash,
		GoVersion:  versionInfo.GoVersion,
		ServerType: versionInfo.ServerType,
	}, nil
}

// GetOnlineFeatures Returns an object containing the response to GetOnlineFeatures.
// Metadata contains feature names that corresponds to the number of rows in response.Results.
// Results contains values including the value of the feature, the event timestamp, and feature status in a columnar format.
func (s *grpcServingServiceServer) GetOnlineFeatures(ctx context.Context, request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {

	span, ctx := tracer.StartSpanFromContext(ctx, "getOnlineFeatures", tracer.ResourceName("ServingService/GetOnlineFeatures"))
	defer span.Finish()

	logSpanContext := LogWithSpanContext(span)

	requestId := GenerateRequestId()
	featuresOrService, err := s.fs.ParseFeatures(request.GetKind())

	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error parsing feature service or feature list from request")
		return nil, err
	}

	featureVectors, err := s.fs.GetOnlineFeatures(
		ctx,
		featuresOrService.FeaturesRefs,
		featuresOrService.FeatureService,
		request.GetEntities(),
		request.GetRequestContext(),
		request.GetFullFeatureNames())

	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error getting online features")
		return nil, errors.GrpcFromError(err)
	}

	resp := &serving.GetOnlineFeaturesResponse{
		Results: make([]*serving.GetOnlineFeaturesResponse_FeatureVector, 0),
		Metadata: &serving.GetOnlineFeaturesResponseMetadata{
			FeatureNames: &serving.FeatureList{Val: make([]string, 0)},
		},
	}
	// JoinKeys are currently part of the features as a value and the order that we add it to the resp MetaData
	// Need to figure out a way to map the correct entities to the correct ordering
	entityValuesMap := make(map[string][]*prototypes.Value, 0)
	featureNames := make([]string, len(featureVectors))
	for idx, vector := range featureVectors {
		resp.Metadata.FeatureNames.Val = append(resp.Metadata.FeatureNames.Val, vector.Name)
		featureNames[idx] = vector.Name
		values, err := types.ArrowValuesToProtoValues(vector.Values)
		if err != nil {
			logSpanContext.Error().Err(err).Msg("Error converting Arrow values to proto values")
			return nil, errors.GrpcFromError(err)
		}
		if _, ok := request.Entities[vector.Name]; ok {
			entityValuesMap[vector.Name] = values
		}

		featureVector := &serving.GetOnlineFeaturesResponse_FeatureVector{
			Values: values,
		}

		if request.GetIncludeMetadata() {
			featureVector.Statuses = vector.Statuses
			featureVector.EventTimestamps = vector.Timestamps
		}

		resp.Results = append(resp.Results, featureVector)
	}

	featureService := featuresOrService.FeatureService
	if featureService != nil && featureService.LoggingConfig != nil && s.loggingService != nil {
		logger, err := s.loggingService.GetOrCreateLogger(featureService)
		if err != nil {
			logSpanContext.Error().Err(err).Msg("Error to instantiating logger for feature service: " + featuresOrService.FeatureService.Name)
			fmt.Printf("Couldn't instantiate logger for feature service %s: %+v", featuresOrService.FeatureService.Name, err)
		}

		err = logger.Log(request.Entities, resp.Results[len(request.Entities):], resp.Metadata.FeatureNames.Val[len(request.Entities):], request.RequestContext, requestId)
		if err != nil {
			logSpanContext.Error().Err(err).Msg("Error to logging to feature service: " + featuresOrService.FeatureService.Name)
			fmt.Printf("LoggerImpl error[%s]: %+v", featuresOrService.FeatureService.Name, err)
		}
	}
	return resp, nil
}

// GetOnlineFeaturesRange Returns an object containing the response to GetOnlineFeaturesRange.
// Similar to GetOnlineFeatures but returns a range of features for each entity based on sort keys.
func (s *grpcServingServiceServer) GetOnlineFeaturesRange(ctx context.Context, request *serving.GetOnlineFeaturesRangeRequest) (*serving.GetOnlineFeaturesRangeResponse, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "getOnlineFeaturesRange", tracer.ResourceName("ServingService/GetOnlineFeaturesRange"))
	defer span.Finish()

	logSpanContext := LogWithSpanContext(span)

	featuresOrService, err := s.fs.ParseFeatures(request.GetKind())
	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error parsing feature service or feature list from request")
		return nil, err
	}

	rangeFeatureVectors, err := s.fs.GetOnlineFeaturesRange(
		ctx,
		featuresOrService.FeaturesRefs,
		featuresOrService.FeatureService,
		request.GetEntities(),
		request.GetSortKeyFilters(),
		request.GetReverseSortOrder(),
		request.GetLimit(),
		request.GetRequestContext(),
		request.GetFullFeatureNames(),
	)

	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error getting online features range")
		return nil, errors.GrpcFromError(err)
	}

	entities := request.GetEntities()
	results := make([]*serving.GetOnlineFeaturesRangeResponse_RangeFeatureVector, 0, len(rangeFeatureVectors))
	featureNames := make([]string, 0, len(rangeFeatureVectors))

	for _, vector := range rangeFeatureVectors {
		featureNames = append(featureNames, vector.Name)

		rangeValues, err := types.ArrowValuesToRepeatedProtoValues(vector.RangeValues)
		if err != nil {
			logSpanContext.Error().Err(err).Msgf("Error converting feature '%s' from Arrow to Proto", vector.Name)
			return nil, errors.GrpcFromError(err)
		}

		featureVector := &serving.GetOnlineFeaturesRangeResponse_RangeFeatureVector{
			Values: rangeValues,
		}

		if request.GetIncludeMetadata() {
			rangeStatuses := make([]*serving.RepeatedFieldStatus, len(rangeValues))
			for j := range rangeValues {
				statusValues := make([]serving.FieldStatus, len(vector.RangeStatuses[j]))
				for k, fieldStatus := range vector.RangeStatuses[j] {
					statusValues[k] = fieldStatus
				}
				rangeStatuses[j] = &serving.RepeatedFieldStatus{Status: statusValues}
			}

			timeValues := make([]*prototypes.RepeatedValue, len(rangeValues))
			for j, timestamps := range vector.RangeTimestamps {
				timestampValues := make([]*prototypes.Value, len(timestamps))
				for k, ts := range timestamps {
					timestampValues[k] = &prototypes.Value{
						Val: &prototypes.Value_UnixTimestampVal{
							UnixTimestampVal: types.GetTimestampSeconds(ts),
						},
					}
				}

				timeValues[j] = &prototypes.RepeatedValue{Val: timestampValues}
			}

			featureVector.Statuses = rangeStatuses
			featureVector.EventTimestamps = timeValues
		}

		results = append(results, featureVector)
	}

	resp := &serving.GetOnlineFeaturesRangeResponse{
		Metadata: &serving.GetOnlineFeaturesResponseMetadata{
			FeatureNames: &serving.FeatureList{Val: featureNames},
		},
		Entities: entities,
		Results:  results,
	}

	return resp, nil
}

// Register services used by the grpcServingServiceServer.
func (s *grpcServingServiceServer) RegisterServices() *grpc.Server {
	grpcPromMetrics := grpcPrometheus.NewServerMetrics()
	prometheus.MustRegister(grpcPromMetrics)
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpcTrace.UnaryServerInterceptor(), grpcPromMetrics.UnaryServerInterceptor()),
	)

	serving.RegisterServingServiceServer(grpcServer, s)
	healthService := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthService)
	reflection.Register(grpcServer)

	return grpcServer
}

func GenerateRequestId() string {
	id := uuid.New()
	return id.String()
}
