package main

import (
	"context"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/utils"
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
		request.GetFullFeatureNames())

	resp := &serving.GetOnlineFeaturesResponse{
		Results: make([]*serving.GetOnlineFeaturesResponse_FeatureVector, 0),
		Metadata: &serving.GetOnlineFeaturesResponseMetadata{
			FeatureNames: &serving.FeatureList{Val: make([]string, 0)},
		},
	}
	for name, values := range request.Entities {
		resp.Metadata.FeatureNames.Val = append(resp.Metadata.FeatureNames.Val, name)

		vec := &serving.GetOnlineFeaturesResponse_FeatureVector{
			Values:          make([]*types.Value, 0),
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

		values, err := utils.ArrowValuesToProtoValues(vector.Values)
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
