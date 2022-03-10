package main

import (
	"context"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
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
	return s.fs.GetOnlineFeatures(ctx, request)
}
