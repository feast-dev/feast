package server

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
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

// Returns an object containing the response to GetOnlineFeatures.
// Metadata contains featurenames that corresponds to the number of rows in response.Results.
// Results contains values including the value of the feature, the event timestamp, and feature status in a columnar format.
func (s *grpcServingServiceServer) GetOnlineFeatures(ctx context.Context, request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {
	requestId := GenerateRequestId()
	featuresOrService, err := s.fs.ParseFeatures(request.GetKind())
	if err != nil {
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
			return nil, err
		}
		if _, ok := request.Entities[vector.Name]; ok {
			entityValuesMap[vector.Name] = values
		}

		resp.Results = append(resp.Results, &serving.GetOnlineFeaturesResponse_FeatureVector{
			Values:          values,
			Statuses:        vector.Statuses,
			EventTimestamps: vector.Timestamps,
		})
	}

	featureService := featuresOrService.FeatureService
	if featureService != nil && featureService.LoggingConfig != nil && s.loggingService != nil {
		logger, err := s.loggingService.GetOrCreateLogger(featureService)
		if err != nil {
			fmt.Printf("Couldn't instantiate logger for feature service %s: %+v", featuresOrService.FeatureService.Name, err)
		}

		err = logger.Log(request.Entities, resp.Results[len(request.Entities):], resp.Metadata.FeatureNames.Val[len(request.Entities):], request.RequestContext, requestId)
		if err != nil {
			fmt.Printf("LoggerImpl error[%s]: %+v", featuresOrService.FeatureService.Name, err)
		}
	}
	return resp, nil
}

func GenerateRequestId() string {
	id := uuid.New()
	return id.String()
}
