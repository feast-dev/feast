package feast

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"

	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"google.golang.org/grpc"

	"github.com/opentracing-contrib/go-grpc"
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

// GrpcClientBuilder is used to construct the client with more advanced options.
type GrpcClientBuilder struct {
	host   string
	port   int
	tracer opentracing.Tracer
}

// WithTracer supplies an existing tracer instance to Feast client, so that the gRPC calls
// to Feast Online Serving can be traced. If no tracer is supplied, a no ops tracer will
// be used instead.
func (builder *GrpcClientBuilder) WithTracer(tracer opentracing.Tracer) *GrpcClientBuilder {
	builder.tracer = tracer
	return builder
}

// Build build the client.
func (builder *GrpcClientBuilder) Build() (*GrpcClient, error) {

	feastCli := &GrpcClient{}
	if builder.tracer == nil {
		builder.tracer = opentracing.NoopTracer{}
	}

	adr := fmt.Sprintf("%s:%d", builder.host, builder.port)
	conn, err := grpc.Dial(adr, grpc.WithStatsHandler(&ocgrpc.ClientHandler{}), grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			otgrpc.OpenTracingClientInterceptor(builder.tracer)),
		grpc.WithStreamInterceptor(
			otgrpc.OpenTracingStreamClientInterceptor(builder.tracer)))
	if err != nil {
		return nil, err
	}
	feastCli.cli = serving.NewServingServiceClient(conn)
	feastCli.conn = conn
	return feastCli, nil
}

// NewGrpcClientBuilder instantiate new GrpcClientBuilder.
func NewGrpcClientBuilder(host string, port int) *GrpcClientBuilder {
	return &GrpcClientBuilder{
		host: host,
		port: port,
	}
}

// NewGrpcClient constructs a client that can interact via grpc with the feast serving instance at the given host:port.
func NewGrpcClient(host string, port int) (*GrpcClient, error) {
	return NewGrpcClientBuilder(host, port).Build()
}

// GetOnlineFeatures gets the latest values of the request features from the Feast serving instance provided.
func (fc *GrpcClient) GetOnlineFeatures(ctx context.Context, req *OnlineFeaturesRequest) (
	*OnlineFeaturesResponse, error) {

	featuresRequest, err := req.buildRequest()
	if err != nil {
		return nil, err
	}
	resp, err := fc.cli.GetOnlineFeatures(ctx, featuresRequest)

	// collect unqiue entity refs from entity rows
	entityRefs := make(map[string]struct{})
	for _, entityRows := range req.Entities {
		for ref := range entityRows {
			entityRefs[ref] = struct{}{}
		}
	}
	return &OnlineFeaturesResponse{RawResponse: resp}, err
}

// GetFeastServingInfo gets information about the feast serving instance this client is connected to.
func (fc *GrpcClient) GetFeastServingInfo(ctx context.Context, in *serving.GetFeastServingInfoRequest) (
	*serving.GetFeastServingInfoResponse, error) {

	return fc.cli.GetFeastServingInfo(ctx, in)
}

// Close the grpc connection.
func (fc *GrpcClient) Close() error {
	return fc.conn.Close()
}
