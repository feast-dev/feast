package main

import (
	"context"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/types"
)

type servingServiceServer struct {
	fs         *feast.FeatureStore
	logChannel chan Log
	serving.UnimplementedServingServiceServer
}

type Log struct {
	// Example: driver_id, customer_id
	entityNames []string
	// Example: val{int64_val: 5017}, val{int64_val: 1003}
	entityValues []*prototypes.Value

	// Feature names is 1:1 correspondence with featureValue, featureStatus, and timestamp
	featureNames []string

	featureValues   []*prototypes.Value
	featureStatuses []serving.FieldStatus
	eventTimestamps []*timestamp.Timestamp
	RequestContext  map[string]*prototypes.RepeatedValue
}

func newServingServiceServer(fs *feast.FeatureStore, logChannel chan Log) *servingServiceServer {
	return &servingServiceServer{fs: fs, logChannel: logChannel}
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

func flushToChannel(s *servingServiceServer, onlineResponse *serving.GetOnlineFeaturesResponse, request *serving.GetOnlineFeaturesRequest) {
	for _, featureVector := range onlineResponse.Results {
		featureValues := make([]*prototypes.Value, len(featureVector.Values))
		eventTimestamps := make([]*timestamp.Timestamp, len(featureVector.EventTimestamps))
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
			eventTimestamps[idx] = &timestamp.Timestamp{Seconds: ts.Seconds, Nanos: ts.Nanos}
		}
		// log.Println(request.Entities)
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
		s.logChannel <- newLog
	}
}
