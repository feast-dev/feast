package main

import (
	"context"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/feast-dev/feast/go/cmd/server/logging"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
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
func getClient(ctx context.Context, basePath string, enableLogging bool) (serving.ServingServiceClient, func()) {
	buffer := 1024 * 1024
	listener := bufconn.Listen(buffer)

	server := grpc.NewServer()
	config, err := registry.NewRepoConfigFromFile(getRepoPath(basePath))

	//TODO(kevjumba): either add this officially or talk in design review about what the correct solution for what do with path.
	// Currently in python we use the path in FileSource but it is not specified in configuration unless it is using file_url?
	if enableLogging {
		if config.OfflineStore == nil {
			config.OfflineStore = map[string]interface{}{
				"path": ".",
			}
		} else {
			config.OfflineStore["path"] = "."
		}
	}

	if err != nil {
		panic(err)
	}
	fs, err := feast.NewFeatureStore(config, nil)
	if err != nil {
		panic(err)
	}
	loggingService, err := logging.NewLoggingService(fs, 1000, enableLogging)
	if err != nil {
		panic(err)
	}
	servingServiceServer := newServingServiceServer(fs, loggingService)

	serving.RegisterServingServiceServer(server, servingServiceServer)
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
	client, closer := getClient(ctx, dir, false)
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
	client, closer := getClient(ctx, dir, false)
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
	assert.Nil(t, err)
	assert.NotNil(t, response)
	expectedEntityValuesResp := []*types.Value{
		{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1003}},
		{Val: &types.Value_Int64Val{Int64Val: 1005}},
	}
	expectedFeatureNamesResp := []string{"driver_id", "conv_rate", "acc_rate", "avg_daily_trips"}
	rows, err := test.ReadParquet(filepath.Join(dir, "feature_repo", "data", "driver_stats.parquet"))
	assert.Nil(t, err)
	entityKeys := map[int64]bool{1001: true, 1003: true, 1005: true}
	correctFeatures := test.GetLatestFeatures(rows, entityKeys)
	expectedConvRateValues := []*types.Value{}
	expectedAccRateValues := []*types.Value{}
	expectedAvgDailyTripsValues := []*types.Value{}

	for _, key := range []int64{1001, 1003, 1005} {
		expectedConvRateValues = append(expectedConvRateValues, &types.Value{Val: &types.Value_FloatVal{FloatVal: correctFeatures[key].ConvRate}})
		expectedAccRateValues = append(expectedAccRateValues, &types.Value{Val: &types.Value_FloatVal{FloatVal: correctFeatures[key].AccRate}})
		expectedAvgDailyTripsValues = append(expectedAvgDailyTripsValues, &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(correctFeatures[key].AvgDailyTrips)}})
	}
	// Columnar so get in column format row by row should have column names of all features
	assert.Equal(t, len(response.Results), 4)

	assert.True(t, reflect.DeepEqual(response.Results[0].Values, expectedEntityValuesResp))
	assert.True(t, reflect.DeepEqual(response.Results[1].Values, expectedConvRateValues))
	assert.True(t, reflect.DeepEqual(response.Results[2].Values, expectedAccRateValues))
	assert.True(t, reflect.DeepEqual(response.Results[3].Values, expectedAvgDailyTripsValues))

	assert.True(t, reflect.DeepEqual(response.Metadata.FeatureNames.Val, expectedFeatureNamesResp))
	time.Sleep(1 * time.Second)
}

func TestGetOnlineFeaturesSqliteWithLogging(t *testing.T) {
	ctx := context.Background()
	// Pregenerated using `feast init`.
	dir := "."
	err := test.SetupFeatureRepo(dir)
	assert.Nil(t, err)
	defer test.CleanUpRepo(dir)
	client, closer := getClient(ctx, dir, true)
	defer closer()
	entities := make(map[string]*types.RepeatedValue)
	entities["driver_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1001}},
			{Val: &types.Value_Int64Val{Int64Val: 1003}},
			{Val: &types.Value_Int64Val{Int64Val: 1005}},
		},
	}
	featureNames := []string{"driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate", "driver_hourly_stats:avg_daily_trips"}
	expectedEntityValuesResp := []*types.Value{
		{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1003}},
		{Val: &types.Value_Int64Val{Int64Val: 1005}},
	}
	expectedFeatureNamesResp := []string{"conv_rate", "acc_rate", "avg_daily_trips"}

	request := &serving.GetOnlineFeaturesRequest{
		Kind: &serving.GetOnlineFeaturesRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNames,
			},
		},
		Entities: entities,
	}
	response, err := client.GetOnlineFeatures(ctx, request)
	assert.Nil(t, err)
	assert.NotNil(t, response)
	// Wait for logger to flush.
	// TODO(Change this when we add param for flush duration)
	time.Sleep(200 * time.Millisecond)
	expectedLogValues, expectedLogStatuses, expectedLogMillis := GetExpectedLogRows(featureNames, response.Results)
	// read from parquet log file
	fr, err := local.NewLocalFileReader("log.parquet")
	assert.Nil(t, err)

	pr, err := reader.NewParquetReader(fr, new(logging.ParquetLog), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num := int(pr.GetNumRows())
	assert.Equal(t, num, 3)
	logs := make([]logging.ParquetLog, 3) //read 10 rows
	err = pr.Read(&logs)
	assert.Nil(t, err)
	for i := 0; i < 3; i++ {
		assert.Equal(t, logs[i].EntityName, "driver_id")
		assert.Equal(t, logs[i].EntityValue, expectedEntityValuesResp[i].String())
		assert.True(t, reflect.DeepEqual(logs[i].FeatureNames, expectedFeatureNamesResp))
		numValues := len(expectedFeatureNamesResp)
		assert.Equal(t, numValues, len(logs[i].FeatureValues))
		assert.Equal(t, numValues, len(logs[i].EventTimestamps))
		assert.Equal(t, numValues, len(logs[i].FeatureStatuses))
		assert.True(t, reflect.DeepEqual(logs[i].FeatureValues, expectedLogValues[i]))
		assert.True(t, reflect.DeepEqual(logs[i].FeatureStatuses, expectedLogStatuses[i]))
		assert.True(t, reflect.DeepEqual(logs[i].EventTimestamps, expectedLogMillis[i]))
	}

	pr.ReadStop()
	fr.Close()

	err = os.Remove("log.parquet")
	assert.Nil(t, err)
}

func GetExpectedLogRows(featureNames []string, results []*serving.GetOnlineFeaturesResponse_FeatureVector) ([][]string, [][]bool, [][]int64) {
	numFeatures := len(featureNames)
	numRows := len(results[0].Values)
	featureValueLogRows := make([][]string, numRows)
	featureStatusLogRows := make([][]bool, numRows)
	eventTimestampLogRows := make([][]int64, numRows)

	for row_idx := 0; row_idx < numRows; row_idx++ {
		featureValueLogRows[row_idx] = make([]string, numFeatures)
		featureStatusLogRows[row_idx] = make([]bool, numFeatures)
		eventTimestampLogRows[row_idx] = make([]int64, numFeatures)
		for idx := 1; idx < len(results); idx++ {
			featureValueLogRows[row_idx][idx-1] = results[idx].Values[row_idx].String()
			if results[idx].Statuses[row_idx] == serving.FieldStatus_PRESENT {
				featureStatusLogRows[row_idx][idx-1] = true
			} else {
				featureStatusLogRows[row_idx][idx-1] = false
			}
			eventTimestampLogRows[row_idx][idx-1] = results[idx].EventTimestamps[row_idx].AsTime().UnixNano() / int64(time.Millisecond)
		}
	}

	return featureValueLogRows, featureStatusLogRows, eventTimestampLogRows
}
