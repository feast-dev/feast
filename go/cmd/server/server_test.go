package main

import (
	"context"
	"log"
	"net"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// Return absolute path to the test_repo directory regardless of the working directory
func getRepoPath(basePath string) string {
	// Get the file path of this source file, regardless of the working directory
	if basePath == "" {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			panic("couldn't find file path of the test file")
		}
		return filepath.Join(filename, "..", "..", "feature_repo")
	} else {
		return filepath.Join(basePath, "feature_repo")
	}
}

func getClient(ctx context.Context, basePath string) (serving.ServingServiceClient, func()) {
	buffer := 1024 * 1024
	listener := bufconn.Listen(buffer)

	server := grpc.NewServer()
	config, err := feast.NewRepoConfigFromFile(getRepoPath(basePath))
	if err != nil {
		panic(err)
	}
	fs, err := feast.NewFeatureStore(config)
	if err != nil {
		panic(err)
	}
	serving.RegisterServingServiceServer(server, &servingServiceServer{fs: fs})
	go func() {
		if err := server.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithInsecure())

	closer := func() {
		listener.Close()
		server.Stop()
	}

	client := serving.NewServingServiceClient(conn)

	return client, closer
}

func TestGetFeastServingInfo(t *testing.T) {
	t.Skip("@todo(achals): feature_repo isn't checked in yet")
	ctx := context.Background()
	client, closer := getClient(ctx, "")
	defer closer()
	response, err := client.GetFeastServingInfo(ctx, &serving.GetFeastServingInfoRequest{})
	assert.Nil(t, err)
	assert.Equal(t, feastServerVersion, response.Version)
}

func TestGetOnlineFeatures(t *testing.T) {
	//t.Skip("@todo(achals): feature_repo isn't checked in yet")
	ctx := context.Background()
	client, closer := getClient(ctx, "../../internal/test")
	defer closer()
	entities := make(map[string]*types.RepeatedValue)
	entities["driver"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1001}},
			{Val: &types.Value_Int64Val{Int64Val: 1003}},
			{Val: &types.Value_Int64Val{Int64Val: 1005}},
		},
	}
	request := &serving.GetOnlineFeaturesRequest{
		Kind: &serving.GetOnlineFeaturesRequest_Features{
			Features: &serving.FeatureList{
				Val: []string{"driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate", "driver_hourly_stats:avg_daily_trips"},
			},
		},
		Entities: entities,
	}
	response, err := client.GetOnlineFeatures(ctx, request)
	assert.Nil(t, err)
	assert.NotNil(t, response)
	log.Println(response.Metadata.FeatureNames.Val)
	assert.True(t, false)

}
