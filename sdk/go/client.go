package feast

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"

	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"google.golang.org/grpc"

	"go.opencensus.io/plugin/ocgrpc"
)

// Client is a feast serving client.
type Client interface {
	GetOnlineFeatures(ctx context.Context, req *OnlineFeaturesRequest) (*OnlineFeaturesResponse, error)
	GetFeastServingInfo(ctx context.Context, in *serving.GetFeastServingInfoRequest) (*serving.GetFeastServingInfoResponse, error)
	Close() error
}

// GrpcClient is a grpc client for feast serving.
type GrpcClient struct {
	cli  serving.ServingServiceClient
	conn *grpc.ClientConn
}

// NewGrpcClient constructs a client that can interact via grpc with the feast serving instance at the given host:port.
func NewGrpcClient(host string, port int) (*GrpcClient, error) {
	feastCli := &GrpcClient{}

	adr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.Dial(adr, grpc.WithStatsHandler(&ocgrpc.ClientHandler{}), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	feastCli.cli = serving.NewServingServiceClient(conn)
	feastCli.conn = conn
	return feastCli, nil
}

// GetOnlineFeatures gets the latest values of the request features from the Feast serving instance provided.
func (fc *GrpcClient) GetOnlineFeatures(ctx context.Context, req *OnlineFeaturesRequest) (
	*OnlineFeaturesResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "get_online_features")
	defer span.Finish()

	featuresRequest, err := req.buildRequest()
	if err != nil {
		return nil, err
	}
	resp, err := fc.cli.GetOnlineFeatures(ctx, featuresRequest)

	return &OnlineFeaturesResponse{RawResponse: resp}, err
}

// GetFeastServingInfo gets information about the feast serving instance this client is connected to.
func (fc *GrpcClient) GetFeastServingInfo(ctx context.Context, in *serving.GetFeastServingInfoRequest) (
	*serving.GetFeastServingInfoResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "get_info")
	defer span.Finish()

	return fc.cli.GetFeastServingInfo(ctx, in)
}

// Close the grpc connection.
func (fc *GrpcClient) Close() error {
	return fc.conn.Close()
}
