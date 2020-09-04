package feast

import (
	"context"
	"fmt"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/opentracing/opentracing-go"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

// SecurityConfig wraps security config for GrpcClient
type SecurityConfig struct {
	// Whether to enable TLS SSL trasnport security if true.
	EnableTLS bool
	// Optional: Provides path to TLS certificate use the verify Service identity.
	TLSCertPath string
	// Optional: Credential used for authentication.
	// Disables authentication if unspecified.
	Credential *Credential
}

// NewGrpcClient constructs a client that can interact via grpc with the feast serving instance at the given host:port.
func NewGrpcClient(host string, port int) (*GrpcClient, error) {
	return NewAuthGrpcClient(host, port, SecurityConfig{
		EnableTLS:  false,
		Credential: nil,
	})
}

// NewAuthGrpcClient constructs a client that can connect with feast serving instances with authentication enabled.
// host - hostname of the serving host/instance to connect to.
// port - post of the host to service host/instancf to connect to.
// securityConfig - security config configures client security.
func NewAuthGrpcClient(host string, port int, security SecurityConfig) (*GrpcClient, error) {
	feastCli := &GrpcClient{}
	adr := fmt.Sprintf("%s:%d", host, port)

	// Compile grpc dial options from security config.
	options := []grpc.DialOption{grpc.WithStatsHandler(&ocgrpc.ClientHandler{})}
	if !security.EnableTLS {
		options = append(options, grpc.WithInsecure())
	}
	// Read TLS certificate from given path instead of using system certs if specified.
	if security.EnableTLS && security.TLSCertPath != "" {
		tlsCreds, err := credentials.NewClientTLSFromFile(security.TLSCertPath, "")
		if err != nil {
			return nil, err
		}
		options = append(options, grpc.WithTransportCredentials(tlsCreds))
	}
	// Enable authentication by attaching credentials if given
	if security.Credential != nil {
		options = append(options, grpc.WithPerRPCCredentials(security.Credential))
	}

	conn, err := grpc.Dial(adr, options...)
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
	span, ctx := opentracing.StartSpanFromContext(ctx, "get_info")
	defer span.Finish()

	return fc.cli.GetFeastServingInfo(ctx, in)
}

// Close the grpc connection.
func (fc *GrpcClient) Close() error {
	return fc.conn.Close()
}
