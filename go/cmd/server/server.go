package main

import (
	"context"

	"github.com/feast-dev/feast/go/cmd/server/logging"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"google.golang.org/protobuf/types/known/timestamppb"
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
<<<<<<< HEAD

	for _, vector := range featureVectors {
=======
	// Entities are currently part of the features as a value
	entityNames := make([]string, 0)
	entityValues := make([]*prototypes.Value, 0)
	for name, values := range request.Entities {
		resp.Metadata.FeatureNames.Val = append(resp.Metadata.FeatureNames.Val, name)
		vec := &serving.GetOnlineFeaturesResponse_FeatureVector{
			Values:          make([]*prototypes.Value, 0),
			Statuses:        make([]serving.FieldStatus, 0),
			EventTimestamps: make([]*timestamp.Timestamp, 0),
		}
		resp.Results = append(resp.Results, vec)
		for _, v := range values.Val {
			entityNames = append(entityNames, name)
			entityValues = append(entityValues, v)
			vec.Values = append(vec.Values, v)
			vec.Statuses = append(vec.Statuses, serving.FieldStatus_PRESENT)
			vec.EventTimestamps = append(vec.EventTimestamps, &timestamp.Timestamp{})
		}
	}
	featureNames := make([]string, len(featureVectors))
	for idx, vector := range featureVectors {
>>>>>>> 118a377d (Working state)
		resp.Metadata.FeatureNames.Val = append(resp.Metadata.FeatureNames.Val, vector.Name)
		featureNames[idx] = vector.Name
		values, err := types.ArrowValuesToProtoValues(vector.Values)
		if err != nil {
			return nil, err
		}
		resp.Results = append(resp.Results, &serving.GetOnlineFeaturesResponse_FeatureVector{
			Values:          values,
			Statuses:        vector.Statuses,
			EventTimestamps: vector.Timestamps,
		})
	}
	go generateLogs(s, entityNames, entityValues, featureNames, resp.Results, request)
	return resp, nil
}

func generateLogs(s *servingServiceServer, entityNames []string, entityValues []*prototypes.Value, featureNames []string, results []*serving.GetOnlineFeaturesResponse_FeatureVector, request *serving.GetOnlineFeaturesRequest) error {
	// Add a log with the request context
	if request.RequestContext != nil && len(request.RequestContext) > 0 {
		requestContextLog := logging.Log{
			RequestContext: request.RequestContext,
		}
		s.loggingService.EmitLog(&requestContextLog)
	}

	if len(results) <= 0 {
		return nil
	}
	numFeatures := len(featureNames)
	numRows := len(results[0].Values)
	featureValueLogRows := make([][]*prototypes.Value, numRows)
	featureStatusLogRows := make([][]serving.FieldStatus, numRows)
	eventTimestampLogRows := make([][]*timestamppb.Timestamp, numRows)

	for row_idx := 0; row_idx < numRows; row_idx++ {
		featureValueLogRows[row_idx] = make([]*prototypes.Value, numFeatures)
		featureStatusLogRows[row_idx] = make([]serving.FieldStatus, numFeatures)
		eventTimestampLogRows[row_idx] = make([]*timestamppb.Timestamp, numFeatures)
		for idx := 1; idx < len(results); idx++ {
			featureValueLogRows[row_idx][idx-1] = results[idx].Values[row_idx]
			featureStatusLogRows[row_idx][idx-1] = results[idx].Statuses[row_idx]
			eventTimestampLogRows[row_idx][idx-1] = results[idx].EventTimestamps[row_idx]
		}
		newLog := logging.Log{
			EntityName:      entityNames[row_idx],
			EntityValue:     entityValues[row_idx],
			FeatureNames:    featureNames,
			FeatureValues:   featureValueLogRows[row_idx],
			FeatureStatuses: featureStatusLogRows[row_idx],
			EventTimestamps: eventTimestampLogRows[row_idx],
		}
		err := s.loggingService.EmitLog(&newLog)
		if err != nil {
			return err
		}
	}
	return nil
}
