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
	// Entities are currently part of the features as a value
	entityValues := make(map[string][]*prototypes.Value, 0)
	for name, values := range request.Entities {
		resp.Metadata.FeatureNames.Val = append(resp.Metadata.FeatureNames.Val, name)
		vec := &serving.GetOnlineFeaturesResponse_FeatureVector{
			Values:          make([]*prototypes.Value, 0),
			Statuses:        make([]serving.FieldStatus, 0),
			EventTimestamps: make([]*timestamp.Timestamp, 0),
		}
		resp.Results = append(resp.Results, vec)
		valueSlice := make([]*prototypes.Value, 0)
		for _, v := range values.Val {
			valueSlice = append(valueSlice, v)
			vec.Values = append(vec.Values, v)
			vec.Statuses = append(vec.Statuses, serving.FieldStatus_PRESENT)
			vec.EventTimestamps = append(vec.EventTimestamps, &timestamp.Timestamp{})
		}
		entityValues[name] = valueSlice
	}
	featureNames := make([]string, len(featureVectors))
	for idx, vector := range featureVectors {
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
	go generateLogs(s, entityValues, featureNames, resp.Results, request.RequestContext)
	return resp, nil
}

func generateLogs(s *servingServiceServer, entities map[string][]*prototypes.Value, featureNames []string, features []*serving.GetOnlineFeaturesResponse_FeatureVector, requestData map[string]*prototypes.RepeatedValue) error {
	// Add a log with the request context
	if requestData != nil && len(requestData) > 0 {
		requestContextLog := logging.Log{
			RequestContext: requestData,
		}
		s.loggingService.EmitLog(&requestContextLog)
	}

	//schema := getSchemaFromFeatureService(featureService)
	//featuresByName := make(map[string]*serving.GetOnlineFeaturesResponse_FeatureVector)
	if len(features) <= 0 {
		return nil
	}
	numFeatures := len(featureNames)
	numRows := len(features[0].Values)
	featureValueLogRows := make([][]*prototypes.Value, numRows)
	featureStatusLogRows := make([][]serving.FieldStatus, numRows)
	eventTimestampLogRows := make([][]*timestamppb.Timestamp, numRows)

	for row_idx := 0; row_idx < numRows; row_idx++ {
		featureValueLogRows[row_idx] = make([]*prototypes.Value, numFeatures)
		featureStatusLogRows[row_idx] = make([]serving.FieldStatus, numFeatures)
		eventTimestampLogRows[row_idx] = make([]*timestamppb.Timestamp, numFeatures)
		for idx := 1; idx < len(features); idx++ {
			featureValueLogRows[row_idx][idx-1] = features[idx].Values[row_idx]
			featureStatusLogRows[row_idx][idx-1] = features[idx].Statuses[row_idx]
			eventTimestampLogRows[row_idx][idx-1] = features[idx].EventTimestamps[row_idx]
		}
		entityRow := make([]*prototypes.Value, 0)
		for _, val := range entities {
			entityRow = append(entityRow, val[row_idx])
		}
		newLog := logging.Log{
			EntityValue:     entityRow,
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
