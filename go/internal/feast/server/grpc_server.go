package server

import (
	"context"
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"

	"google.golang.org/grpc/reflection"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		return nil, err
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
			return nil, err
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
		return nil, err
	}

	entityNames := make([]string, 0)
	featureNames := make([]string, 0)

	entityNamesMap := make(map[string]bool)
	for entityName := range request.GetEntities() {
		entityNamesMap[entityName] = true
		entityNames = append(entityNames, entityName)
	}

	requestedFeatureNames := make([]string, 0)
	if featuresOrService.FeatureService != nil {
		for _, projection := range featuresOrService.FeatureService.Projections {
			for _, feature := range projection.Features {
				featureName := feature.Name
				if request.GetFullFeatureNames() {
					featureName = fmt.Sprintf("%s:%s", projection.NameToUse(), feature.Name)
				}
				requestedFeatureNames = append(requestedFeatureNames, featureName)
			}
		}
	} else {
		requestedFeatureNames = featuresOrService.FeaturesRefs
	}

	vectorsByName := make(map[string]*onlineserving.RangeFeatureVector)
	for _, vector := range rangeFeatureVectors {
		vectorsByName[vector.Name] = vector
	}

	entities := make([]*prototypes.RepeatedValue, 0, len(entityNames))
	for _, entityName := range entityNames {
		if vector, exists := vectorsByName[entityName]; exists {
			rangeValues, err := types.ArrowValuesToRepeatedProtoValues(vector.RangeValues)
			if err != nil {
				logSpanContext.Error().Err(err).Msgf("Error converting entity %s values", entityName)
				return nil, err
			}

			entityValues := &prototypes.RepeatedValue{Val: make([]*prototypes.Value, len(rangeValues))}
			for i, repeatedValue := range rangeValues {
				if repeatedValue != nil && len(repeatedValue.Val) > 0 {
					entityValues.Val[i] = repeatedValue.Val[0]
				} else {
					entityValues.Val[i] = &prototypes.Value{}
				}
			}
			entities = append(entities, entityValues)
		} else {
			entities = append(entities, &prototypes.RepeatedValue{Val: []*prototypes.Value{}})
		}
	}

	features := make([]*serving.GetOnlineFeaturesRangeResponse_RangeFeatureVector, 0)
	for _, featureName := range requestedFeatureNames {
		if !entityNamesMap[featureName] {
			if vector, exists := vectorsByName[featureName]; exists {
				featureNames = append(featureNames, featureName)

				rangeValues, err := types.ArrowValuesToRepeatedProtoValues(vector.RangeValues)
				if err != nil {
					logSpanContext.Error().Err(err).Msgf("Error converting feature %s values", featureName)
					return nil, err
				}

				rangeStatuses := make([]*serving.RepeatedFieldStatus, len(rangeValues))
				for j := range rangeValues {
					statusValues := make([]serving.FieldStatus, len(vector.RangeStatuses[j]))
					for k, status := range vector.RangeStatuses[j] {
						statusValues[k] = status
					}
					rangeStatuses[j] = &serving.RepeatedFieldStatus{Status: statusValues}
				}

				timeValues := make([]*prototypes.RepeatedValue, len(rangeValues))
				for j, timestamps := range vector.RangeTimestamps {
					timestampValues := make([]*prototypes.Value, len(timestamps))
					for k, ts := range timestamps {
						timestampValues[k] = &prototypes.Value{
							Val: &prototypes.Value_UnixTimestampVal{
								UnixTimestampVal: types.GetTimestampMillis(ts),
							},
						}
					}

					if len(timestampValues) == 0 {
						now := timestamppb.Now()
						timestampValues = []*prototypes.Value{
							{
								Val: &prototypes.Value_UnixTimestampVal{
									UnixTimestampVal: types.GetTimestampMillis(now),
								},
							},
						}
					}
					timeValues[j] = &prototypes.RepeatedValue{Val: timestampValues}
				}

				featureVector := &serving.GetOnlineFeaturesRangeResponse_RangeFeatureVector{
					Values: rangeValues,
				}

				if request.GetIncludeMetadata() {
					featureVector.Statuses = rangeStatuses
					featureVector.EventTimestamps = timeValues
				}

				features = append(features, featureVector)
			}
		}
	}

	resp := &serving.GetOnlineFeaturesRangeResponse{
		Metadata: &serving.GetOnlineFeaturesResponseMetadata{
			FeatureNames: &serving.FeatureList{Val: featureNames},
		},
		Entities: entities,
		Features: features,
	}

	// TODO: Implement logging for GetOnlineFeaturesRange for feature services when support for feature services is added

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
