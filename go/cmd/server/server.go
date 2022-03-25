package main

import (
	"context"
	"log"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type servingServiceServer struct {
	fs *feast.FeatureStore
	serving.UnimplementedServingServiceServer
}

func newServingServiceServer(fs *feast.FeatureStore) *servingServiceServer {
	return &servingServiceServer{fs: fs}
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
		log.Println("first error")

		return nil, err
	}

	featureVectors, err := s.fs.GetOnlineFeatures(
		ctx,
		featuresOrService.FeaturesRefs,
		featuresOrService.FeatureService,
		request.GetEntities(),
		request.GetFullFeatureNames())
	if err != nil {
		log.Println("2nd error")
		return nil, err
	}

	resp := &serving.GetOnlineFeaturesResponse{
		Results: make([]*serving.GetOnlineFeaturesResponse_FeatureVector, 0),
		Metadata: &serving.GetOnlineFeaturesResponseMetadata{
			FeatureNames: &serving.FeatureList{Val: make([]string, 0)},
		},
	}
	for name, values := range request.Entities {
		resp.Metadata.FeatureNames.Val = append(resp.Metadata.FeatureNames.Val, name)

		vec := &serving.GetOnlineFeaturesResponse_FeatureVector{
			Values:          make([]*prototypes.Value, 0),
			Statuses:        make([]serving.FieldStatus, 0),
			EventTimestamps: make([]*timestamp.Timestamp, 0),
		}
		resp.Results = append(resp.Results, vec)

		for _, v := range values.Val {
			vec.Values = append(vec.Values, v)
			vec.Statuses = append(vec.Statuses, serving.FieldStatus_PRESENT)
			vec.EventTimestamps = append(vec.EventTimestamps, &timestamp.Timestamp{})
		}
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
