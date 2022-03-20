package main

import (
	"context"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type servingServiceServer struct {
	fs             *feast.FeatureStore
	loggingService *LoggingService
	serving.UnimplementedServingServiceServer
}

func newServingServiceServer(fs *feast.FeatureStore, loggingService *LoggingService) *servingServiceServer {
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

	for _, vector := range featureVectors {
		resp.Metadata.FeatureNames.Val = append(resp.Metadata.FeatureNames.Val, vector.Name)

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

	return resp, nil
}

func generateLogs(s *servingServiceServer, onlineResponse *serving.GetOnlineFeaturesResponse, request *serving.GetOnlineFeaturesRequest) {
	for _, featureVector := range onlineResponse.Results {
		featureValues := make([]*prototypes.Value, len(featureVector.Values))
		eventTimestamps := make([]*timestamppb.Timestamp, len(featureVector.EventTimestamps))
		for idx, featureValue := range featureVector.Values {
			if featureVector.Statuses[idx] != serving.FieldStatus_PRESENT {
				continue
			}
			featureValues[idx] = &prototypes.Value{Val: featureValue.Val}
		}
		for idx, ts := range featureVector.EventTimestamps {
			if featureVector.Statuses[idx] != serving.FieldStatus_PRESENT {
				continue
			}
			eventTimestamps[idx] = &timestamppb.Timestamp{Seconds: ts.Seconds, Nanos: ts.Nanos}
		}
		// TODO(kevjumba): For filtering out and extracting entity names when the bug with join keys is fixed.
		// for idx, featureName := range onlineResponse.Metadata.FeatureNames.Val {
		// 	if _, ok := request.Entities[featureName]; ok {
		// 		entityNames = append(entityNames, featureName)
		// 		entityValues = append(entityValues, featureVector.Values[idx])
		// 	} else {
		// 		featureNames = append(featureNames, onlineResponse.Metadata.FeatureNames.Val[idx:]...)
		// 		featureValues = append(featureValues, featureVector.Values[idx:]...)
		// 		featureStatuses = append(featureStatuses, featureVector.Statuses[idx:]...)
		// 		eventTimestamps = append(eventTimestamps, featureVector.EventTimestamps[idx:]...)
		// 		break
		// 	}
		// }
		// featureNames = append(featureNames, onlineResponse.Metadata.FeatureNames.Val[:]...)
		// featureValues = append(featureValues, featureVector.Values[:]...)
		// featureStatuses = append(featureStatuses, featureVector.Statuses[:]...)
		// eventTimestamps = append(eventTimestamps, featureVector.EventTimestamps[:]...)

		newLog := Log{
			featureNames:    onlineResponse.Metadata.FeatureNames.Val,
			featureValues:   featureValues,
			featureStatuses: featureVector.Statuses,
			eventTimestamps: eventTimestamps,
			// TODO(kevjumba): figure out if this is required
			RequestContext: request.RequestContext,
		}
		s.loggingService.emitLog(&newLog)
	}
}
