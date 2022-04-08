package main

import (
	"context"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/test"
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

// Starts a new grpc server, registers the serving service and returns a client.
func getClient(ctx context.Context, basePath string) (serving.ServingServiceClient, func()) {
	buffer := 1024 * 1024
	listener := bufconn.Listen(buffer)

	server := grpc.NewServer()
	config, err := feast.NewRepoConfigFromFile(getRepoPath(basePath))
	if err != nil {
		panic(err)
	}
	fs, err := feast.NewFeatureStore(config, nil)
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
	ctx := context.Background()
	// Pregenerated using `feast init`.
	dir := "."
	err := test.SetupFeatureRepo(dir)
	assert.Nil(t, err)
	defer test.CleanUpRepo(dir)
	client, closer := getClient(ctx, dir)
	defer closer()
	response, err := client.GetFeastServingInfo(ctx, &serving.GetFeastServingInfoRequest{})
	assert.Nil(t, err)
	assert.Equal(t, feastServerVersion, response.Version)
}

func TestGetOnlineFeaturesSqlite(t *testing.T) {
	ctx := context.Background()
	// Pregenerated using `feast init`.
	dir := "."
	err := test.SetupFeatureRepo(dir)
	assert.Nil(t, err)
	defer test.CleanUpRepo(dir)
	client, closer := getClient(ctx, dir)
	defer closer()
	entities := make(map[string]*types.RepeatedValue)
	entities["driver_id"] = &types.RepeatedValue{
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
	expectedEntityValuesResp := []*types.Value{
		{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1003}},
		{Val: &types.Value_Int64Val{Int64Val: 1005}},
	}
	expectedFeatureNamesResp := []string{"driver_id", "conv_rate", "acc_rate", "avg_daily_trips"}
	assert.Nil(t, err)
	assert.NotNil(t, response)
	rows, err := test.ReadParquet(filepath.Join(dir, "feature_repo", "data", "driver_stats.parquet"))
	assert.Nil(t, err)
	entityKeys := map[int64]bool{1001: true, 1003: true, 1005: true}
	correctFeatures := test.GetLatestFeatures(rows, entityKeys)
	expectedConvRateValues := []*types.Value{}
	expectedAccRateValues := []*types.Value{}
	expectedAvgDailyTripsValues := []*types.Value{}

	for _, key := range []int64{1001, 1003, 1005} {
		expectedConvRateValues = append(expectedConvRateValues, &types.Value{Val: &types.Value_FloatVal{FloatVal: correctFeatures[key].Conv_rate}})
		expectedAccRateValues = append(expectedAccRateValues, &types.Value{Val: &types.Value_FloatVal{FloatVal: correctFeatures[key].Acc_rate}})
		expectedAvgDailyTripsValues = append(expectedAvgDailyTripsValues, &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(correctFeatures[key].Avg_daily_trips)}})
	}
	// Columnar so get in column format row by row should have column names of all features
	assert.Equal(t, len(response.Results), 4)

	assert.True(t, reflect.DeepEqual(response.Results[0].Values, expectedEntityValuesResp))
	assert.True(t, reflect.DeepEqual(response.Results[1].Values, expectedConvRateValues))
	assert.True(t, reflect.DeepEqual(response.Results[2].Values, expectedAccRateValues))
	assert.True(t, reflect.DeepEqual(response.Results[3].Values, expectedAvgDailyTripsValues))

	assert.True(t, reflect.DeepEqual(response.Metadata.FeatureNames.Val, expectedFeatureNamesResp))
}
