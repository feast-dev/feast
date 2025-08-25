//go:build integration

package onlinestore

import (
	"context"
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var onlineStore *CassandraOnlineStore
var ctx = context.Background()

func TestMain(m *testing.M) {
	// Initialize the test environment
	dir, err := filepath.Abs("./../integration_tests/scylladb/")
	err = test.SetupInitializedRepo(dir)
	if err != nil {
		fmt.Printf("Failed to set up test environment: %v\n", err)
		os.Exit(1)
	}

	onlineStore, err = getCassandraOnlineStore(dir)
	if err != nil {
		fmt.Printf("Failed to create CassandraOnlineStore: %v\n", err)
		os.Exit(1)
	}

	// Run the tests
	exitCode := m.Run()

	// Clean up the test environment
	test.CleanUpInitializedRepo(dir)

	// Exit with the appropriate code
	if exitCode != 0 {
		fmt.Printf("CassandraOnlineStore Int Tests failed with exit code %d\n", exitCode)
	}
	os.Exit(exitCode)
}

func getCassandraOnlineStore(dir string) (*CassandraOnlineStore, error) {
	config, err := loadRepoConfig(dir)
	if err != nil {
		fmt.Printf("Failed to load repo config: %v\n", err)
		return nil, err
	}

	store, err := NewCassandraOnlineStore("feature_integration_repo", config, config.OnlineStore)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func loadRepoConfig(basePath string) (*registry.RepoConfig, error) {
	path, err := filepath.Abs(filepath.Join(basePath, "feature_repo"))
	config, err := registry.NewRepoConfigFromFile(path)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func TestCassandraOnlineStore_OnlineReadRange_withSingleEntityKey(t *testing.T) {
	entityKeys := []*types.EntityKey{{
		JoinKeys: []string{"index_id"},
		EntityValues: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
		},
	}}
	featureViewNames := []string{"all_dtypes_sorted"}
	featureNames := []string{"int_val", "long_val", "float_val", "double_val", "byte_val", "string_val", "timestamp_val", "boolean_val",
		"null_int_val", "null_long_val", "null_float_val", "null_double_val", "null_byte_val", "null_string_val", "null_timestamp_val", "null_boolean_val",
		"null_array_int_val", "null_array_long_val", "null_array_float_val", "null_array_double_val", "null_array_byte_val", "null_array_string_val",
		"null_array_boolean_val", "array_int_val", "array_long_val", "array_float_val", "array_double_val", "array_string_val", "array_boolean_val",
		"array_byte_val", "array_timestamp_val", "null_array_timestamp_val", "event_timestamp"}
	sortKeyFilters := []*model.SortKeyFilter{{
		SortKeyName: "event_timestamp",
		RangeStart:  time.Unix(1744769099, 0),
		RangeEnd:    time.Unix(1744779099, 0),
	}}

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         entityKeys,
		FeatureViewNames:   featureViewNames,
		FeatureNames:       featureNames,
		SortKeyFilters:     sortKeyFilters,
		Limit:              10,
		IsReverseSortOrder: false,
		SortKeyNames:       map[string]bool{"event_timestamp": true},
	}

	data, err := onlineStore.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)
	verifyResponseData(t, data, 1, time.Unix(1744769099, 0), time.Unix(1744779099, 0))
}

func TestCassandraOnlineStore_OnlineReadRange_withMultipleEntityKeys(t *testing.T) {
	entityKeys := []*types.EntityKey{
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 1}},
			},
		},
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 2}},
			},
		},
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 3}},
			},
		},
	}
	featureViewNames := []string{"all_dtypes_sorted"}
	featureNames := []string{"int_val", "long_val", "float_val", "double_val", "byte_val", "string_val", "timestamp_val", "boolean_val",
		"null_int_val", "null_long_val", "null_float_val", "null_double_val", "null_byte_val", "null_string_val", "null_timestamp_val", "null_boolean_val",
		"null_array_int_val", "null_array_long_val", "null_array_float_val", "null_array_double_val", "null_array_byte_val", "null_array_string_val",
		"null_array_boolean_val", "array_int_val", "array_long_val", "array_float_val", "array_double_val", "array_string_val", "array_boolean_val",
		"array_byte_val", "array_timestamp_val", "null_array_timestamp_val", "event_timestamp"}
	sortKeyFilters := []*model.SortKeyFilter{{
		SortKeyName: "event_timestamp",
		RangeStart:  time.Unix(1744769099, 0),
	}}

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         entityKeys,
		FeatureViewNames:   featureViewNames,
		FeatureNames:       featureNames,
		SortKeyFilters:     sortKeyFilters,
		Limit:              10,
		IsReverseSortOrder: false,
		SortKeyNames:       map[string]bool{"event_timestamp": true},
	}

	data, err := onlineStore.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)
	verifyResponseData(t, data, 3, time.Unix(1744769099, 0), time.Unix(17447690990, 0))
}

func TestCassandraOnlineStore_OnlineReadRange_withReverseSortOrder(t *testing.T) {
	entityKeys := []*types.EntityKey{
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 1}},
			},
		},
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 2}},
			},
		},
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 3}},
			},
		},
	}
	featureViewNames := []string{"all_dtypes_sorted"}
	featureNames := []string{"int_val", "long_val", "float_val", "double_val", "byte_val", "string_val", "timestamp_val", "boolean_val",
		"null_int_val", "null_long_val", "null_float_val", "null_double_val", "null_byte_val", "null_string_val", "null_timestamp_val", "null_boolean_val",
		"null_array_int_val", "null_array_long_val", "null_array_float_val", "null_array_double_val", "null_array_byte_val", "null_array_string_val",
		"null_array_boolean_val", "array_int_val", "array_long_val", "array_float_val", "array_double_val", "array_string_val", "array_boolean_val",
		"array_byte_val", "array_timestamp_val", "null_array_timestamp_val", "event_timestamp"}
	sortKeyFilters := []*model.SortKeyFilter{{
		SortKeyName: "event_timestamp",
		RangeStart:  time.Unix(1744769099, 0),
		// The SortKey is defined as DESC in the SortedFeatureView, so we need to set the reverse order of ASC
		Order: &model.SortOrder{Order: core.SortOrder_ASC},
	}}

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         entityKeys,
		FeatureViewNames:   featureViewNames,
		FeatureNames:       featureNames,
		SortKeyFilters:     sortKeyFilters,
		Limit:              10,
		IsReverseSortOrder: true,
		SortKeyNames:       map[string]bool{"event_timestamp": true},
	}

	data, err := onlineStore.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)
	verifyResponseData(t, data, 3, time.Unix(1744769099, 0), time.Unix(17447690990, 0))
}

func TestCassandraOnlineStore_OnlineReadRange_withNoSortKeyFilters(t *testing.T) {
	entityKeys := []*types.EntityKey{
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 1}},
			},
		},
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 2}},
			},
		},
		{
			JoinKeys: []string{"index_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 3}},
			},
		},
	}
	featureViewNames := []string{"all_dtypes_sorted"}
	featureNames := []string{"int_val", "long_val", "float_val", "double_val", "byte_val", "string_val", "timestamp_val", "boolean_val",
		"null_int_val", "null_long_val", "null_float_val", "null_double_val", "null_byte_val", "null_string_val", "null_timestamp_val", "null_boolean_val",
		"null_array_int_val", "null_array_long_val", "null_array_float_val", "null_array_double_val", "null_array_byte_val", "null_array_string_val",
		"null_array_boolean_val", "array_int_val", "array_long_val", "array_float_val", "array_double_val", "array_string_val", "array_boolean_val",
		"array_byte_val", "array_timestamp_val", "null_array_timestamp_val", "event_timestamp"}
	sortKeyFilters := []*model.SortKeyFilter{}

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         entityKeys,
		FeatureViewNames:   featureViewNames,
		FeatureNames:       featureNames,
		SortKeyFilters:     sortKeyFilters,
		Limit:              10,
		IsReverseSortOrder: true,
		SortKeyNames:       map[string]bool{"event_timestamp": true},
	}

	data, err := onlineStore.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)
	verifyResponseData(t, data, 3, time.Unix(0, 0), time.Unix(17447690990, 0))
}

func assertValueType(t *testing.T, actualValue interface{}, expectedType string) {
	require.IsType(t, &types.Value{}, actualValue, "Expected value to be of type *types.Value")
	assert.Equal(t, expectedType, fmt.Sprintf("%T", actualValue.(*types.Value).GetVal()), expectedType)
}

func verifyResponseData(t *testing.T, data [][]RangeFeatureData, numEntityKeys int, start time.Time, end time.Time) {
	assert.Equal(t, numEntityKeys, len(data))

	for i := 0; i < numEntityKeys; i++ {
		assert.Equal(t, 33, len(data[i]))

		assert.Equal(t, "int_val", data[i][0].FeatureName)
		assert.Equal(t, 10, len(data[i][0].Values))
		assert.NotNil(t, data[i][0].Values[0])
		assertValueType(t, data[i][0].Values[0], "*types.Value_Int32Val")

		assert.Equal(t, "long_val", data[i][1].FeatureName)
		assert.Equal(t, 10, len(data[i][1].Values))
		assert.NotNil(t, data[i][1].Values[0])
		assertValueType(t, data[i][1].Values[0], "*types.Value_Int64Val")

		assert.Equal(t, "float_val", data[i][2].FeatureName)
		assert.Equal(t, 10, len(data[i][2].Values))
		assert.NotNil(t, data[i][2].Values[0])
		assertValueType(t, data[i][2].Values[0], "*types.Value_FloatVal")

		assert.Equal(t, "double_val", data[i][3].FeatureName)
		assert.Equal(t, 10, len(data[i][3].Values))
		assert.NotNil(t, data[i][3].Values[0])
		assertValueType(t, data[i][3].Values[0], "*types.Value_DoubleVal")

		assert.Equal(t, "byte_val", data[i][4].FeatureName)
		assert.Equal(t, 10, len(data[i][4].Values))
		assert.NotNil(t, data[i][4].Values[0])
		assertValueType(t, data[i][4].Values[0], "*types.Value_BytesVal")

		assert.Equal(t, "string_val", data[i][5].FeatureName)
		assert.Equal(t, 10, len(data[i][5].Values))
		assert.NotNil(t, data[i][5].Values[0])
		assertValueType(t, data[i][5].Values[0], "*types.Value_StringVal")

		assert.Equal(t, "timestamp_val", data[i][6].FeatureName)
		assert.Equal(t, 10, len(data[i][6].Values))
		assert.NotNil(t, data[i][6].Values[0])
		assertValueType(t, data[i][6].Values[0], "*types.Value_UnixTimestampVal")

		assert.Equal(t, "boolean_val", data[i][7].FeatureName)
		assert.Equal(t, 10, len(data[i][7].Values))
		assert.NotNil(t, data[i][7].Values[0])
		assertValueType(t, data[i][7].Values[0], "*types.Value_BoolVal")

		assert.Equal(t, "null_int_val", data[i][8].FeatureName)
		assert.Equal(t, 10, len(data[i][8].Values))
		assert.Nil(t, data[i][8].Values[0])

		assert.Equal(t, "null_long_val", data[i][9].FeatureName)
		assert.Equal(t, 10, len(data[i][9].Values))
		assert.Nil(t, data[i][9].Values[0])

		assert.Equal(t, "null_float_val", data[i][10].FeatureName)
		assert.Equal(t, 10, len(data[i][10].Values))
		assert.Nil(t, data[i][10].Values[0])

		assert.Equal(t, "null_double_val", data[i][11].FeatureName)
		assert.Equal(t, 10, len(data[i][11].Values))
		assert.Nil(t, data[i][11].Values[0])

		assert.Equal(t, "null_byte_val", data[i][12].FeatureName)
		assert.Equal(t, 10, len(data[i][12].Values))
		assert.Nil(t, data[i][12].Values[0])

		assert.Equal(t, "null_string_val", data[i][13].FeatureName)
		assert.Equal(t, 10, len(data[i][13].Values))
		assert.Nil(t, data[i][13].Values[0])

		assert.Equal(t, "null_timestamp_val", data[i][14].FeatureName)
		assert.Equal(t, 10, len(data[i][14].Values))
		assert.Nil(t, data[i][14].Values[0])

		assert.Equal(t, "null_boolean_val", data[i][15].FeatureName)
		assert.Equal(t, 10, len(data[i][15].Values))
		assert.Nil(t, data[i][15].Values[0])

		assert.Equal(t, "null_array_int_val", data[i][16].FeatureName)
		assert.Equal(t, 10, len(data[i][16].Values))
		assert.Nil(t, data[i][16].Values[0])

		assert.Equal(t, "null_array_long_val", data[i][17].FeatureName)
		assert.Equal(t, 10, len(data[i][17].Values))
		assert.Nil(t, data[i][17].Values[0])

		assert.Equal(t, "null_array_float_val", data[i][18].FeatureName)
		assert.Equal(t, 10, len(data[i][18].Values))
		assert.Nil(t, data[i][18].Values[0])

		assert.Equal(t, "null_array_double_val", data[i][19].FeatureName)
		assert.Equal(t, 10, len(data[i][19].Values))
		assert.Nil(t, data[i][19].Values[0])

		assert.Equal(t, "null_array_byte_val", data[i][20].FeatureName)
		assert.Equal(t, 10, len(data[i][20].Values))
		assert.Nil(t, data[i][20].Values[0])

		assert.Equal(t, "null_array_string_val", data[i][21].FeatureName)
		assert.Equal(t, 10, len(data[i][21].Values))
		assert.Nil(t, data[i][21].Values[0])

		assert.Equal(t, "null_array_boolean_val", data[i][22].FeatureName)
		assert.Equal(t, 10, len(data[i][22].Values))
		assert.Nil(t, data[i][22].Values[0])

		assert.Equal(t, "array_int_val", data[i][23].FeatureName)
		assert.Equal(t, 10, len(data[i][23].Values))
		assert.NotNil(t, data[i][23].Values[0])
		assertValueType(t, data[i][23].Values[0], "*types.Value_Int32ListVal")

		assert.Equal(t, "array_long_val", data[i][24].FeatureName)
		assert.Equal(t, 10, len(data[i][24].Values))
		assert.NotNil(t, data[i][24].Values[0])
		assertValueType(t, data[i][24].Values[0], "*types.Value_Int64ListVal")

		assert.Equal(t, "array_float_val", data[i][25].FeatureName)
		assert.Equal(t, 10, len(data[i][25].Values))
		assert.NotNil(t, data[i][25].Values[0])
		assertValueType(t, data[i][25].Values[0], "*types.Value_FloatListVal")

		assert.Equal(t, "array_double_val", data[i][26].FeatureName)
		assert.Equal(t, 10, len(data[i][26].Values))
		assert.NotNil(t, data[i][26].Values[0])
		assertValueType(t, data[i][26].Values[0], "*types.Value_DoubleListVal")

		assert.Equal(t, "array_string_val", data[i][27].FeatureName)
		assert.Equal(t, 10, len(data[i][27].Values))
		assert.NotNil(t, data[i][27].Values[0])
		assertValueType(t, data[i][27].Values[0], "*types.Value_StringListVal")

		assert.Equal(t, "array_boolean_val", data[i][28].FeatureName)
		assert.Equal(t, 10, len(data[i][28].Values))
		assert.NotNil(t, data[i][28].Values[0])
		assertValueType(t, data[i][28].Values[0], "*types.Value_BoolListVal")

		assert.Equal(t, "array_byte_val", data[i][29].FeatureName)
		assert.Equal(t, 10, len(data[i][29].Values))
		assert.NotNil(t, data[i][29].Values[0])
		assertValueType(t, data[i][29].Values[0], "*types.Value_BytesListVal")

		assert.Equal(t, "array_timestamp_val", data[i][30].FeatureName)
		assert.Equal(t, 10, len(data[i][30].Values))
		assert.NotNil(t, data[i][30].Values[0])
		assertValueType(t, data[i][30].Values[0], "*types.Value_UnixTimestampListVal")

		assert.Equal(t, "null_array_timestamp_val", data[i][31].FeatureName)
		assert.Equal(t, 10, len(data[i][31].Values))
		assert.Nil(t, data[i][31].Values[0])

		assert.Equal(t, "event_timestamp", data[i][32].FeatureName)
		assert.Equal(t, 10, len(data[i][32].Values))
		assert.NotNil(t, data[i][32].Values[0])
		assert.IsType(t, time.Time{}, data[i][32].Values[0])
		for _, timestamp := range data[i][32].Values {
			assert.GreaterOrEqual(t, timestamp.(time.Time).Unix(), start.Unix(), "Timestamp should be greater than or equal to %v", start)
			assert.LessOrEqual(t, timestamp.(time.Time).Unix(), end.Unix(), "Timestamp should be less than or equal to %v", end)
		}
	}
}
