package main

import (
	"context"

	"github.com/feast-dev/feast/go/cmd/server/logging"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"github.com/google/uuid"
)

type servingServiceServer struct {
	fs             *feast.FeatureStore
	loggingService *logging.LoggingService
	serving.UnimplementedServingServiceServer
}

func newServingServiceServer(fs *feast.FeatureStore, loggingService *logging.LoggingService) *servingServiceServer {
	return &servingServiceServer{fs: fs, loggingService: loggingService}
}

func (s *servingServiceServer) GetFeastServingInfo(ctx context.Context, request *serving.GetFeastServingInfoRequest) (*serving.GetFeastServingInfoResponse, error) {
	return &serving.GetFeastServingInfoResponse{
		Version: feastServerVersion,
	}, nil
}

// Returns an object containing the response to GetOnlineFeatures.
// Metadata contains featurenames that corresponds to the number of rows in response.Results.
// Results contains values including the value of the feature, the event timestamp, and feature status in a columnar format.
func (s *servingServiceServer) GetOnlineFeatures(ctx context.Context, request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {
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
	// Entities are currently part of the features as a value and the order that we add it to the resp MetaData
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
	if featuresOrService.FeatureService != nil {
		go s.loggingService.GenerateLogs(featuresOrService.FeatureService, entityValuesMap, featureNames, resp.Results[len(request.Entities):], request.RequestContext, requestId)
	}
	return resp, nil
}

func GenerateRequestId() string {
	id := uuid.New()
	return id.String()
}
