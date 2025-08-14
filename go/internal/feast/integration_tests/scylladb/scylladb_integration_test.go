//go:build integration

package scylladb

import (
	"context"
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast/server"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	ALL_SORTED_FEATURE_NAMES  = "int_val,long_val,float_val,double_val,byte_val,string_val,timestamp_val,boolean_val,array_int_val,array_long_val,array_float_val,array_double_val,array_byte_val,array_string_val,array_timestamp_val,array_boolean_val,null_int_val,null_long_val,null_float_val,null_double_val,null_byte_val,null_string_val,null_timestamp_val,null_boolean_val,null_array_int_val,null_array_long_val,null_array_float_val,null_array_double_val,null_array_byte_val,null_array_string_val,null_array_timestamp_val,null_array_boolean_val,event_timestamp"
	ALL_REGULAR_FEATURE_NAMES = "int_val,long_val,float_val,double_val,byte_val,string_val,timestamp_val,boolean_val,array_int_val,array_long_val,array_float_val,array_double_val,array_byte_val,array_string_val,array_timestamp_val,array_boolean_val,null_int_val,null_long_val,null_float_val,null_double_val,null_byte_val,null_string_val,null_timestamp_val,null_boolean_val,null_array_int_val,null_array_long_val,null_array_float_val,null_array_double_val,null_array_byte_val,null_array_string_val,null_array_timestamp_val,null_array_boolean_val"
)

var client serving.ServingServiceClient
var ctx context.Context

func TestMain(m *testing.M) {
	dir, err := filepath.Abs("./")
	if err != nil {
		fmt.Printf("Failed to get absolute path: %v\n", err)
		os.Exit(1)
	}
	err = test.SetupInitializedRepo(dir)
	if err != nil {
		fmt.Printf("Failed to set up test environment: %v\n", err)
		os.Exit(1)
	}

	ctx = context.Background()
	var closer func()

	client, closer = server.GetClient(ctx, dir, "")

	// Run the tests
	exitCode := m.Run()

	// Clean up the test environment
	test.CleanUpInitializedRepo(dir)
	closer()

	// Exit with the appropriate code
	if exitCode != 0 {
		fmt.Printf("CassandraOnlineStore Int Tests failed with exit code %d\n", exitCode)
	}
	os.Exit(exitCode)
}

func TestGetOnlineFeaturesRange(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	featureNames := getAllSortedFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assertResponseData(t, response, featureNames, 3, true)
}

func TestGetOnlineFeaturesRange_withOnlyEqualsFilter(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 2}},
		},
	}

	featureNames := getAllSortedFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Equals{
					Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1744769171}},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, 1, len(response.Entities))
	for i, featureResult := range response.Results {
		assert.Equal(t, 1, len(featureResult.Values))
		assert.Equal(t, 1, len(featureResult.Statuses))
		assert.Equal(t, 1, len(featureResult.EventTimestamps))
		for j, value := range featureResult.Values {
			assert.NotNil(t, value)
			assert.Equal(t, 1, len(value.Val))
			featureName := featureNames[i]
			if strings.Contains(featureName, "null") {
				// For null features, we expect the value to contain 1 entry with a nil value
				assert.Nil(t, value.Val[0].Val, "Feature %s should have a nil value", featureName)
				assert.Equal(t, serving.FieldStatus_NULL_VALUE, featureResult.Statuses[j].Status[0], "Feature %s should have a NULL_VALUE status but was %s", featureName, featureResult.Statuses[j].Status[0])
			} else {
				assert.NotNil(t, value.Val[0].Val, "Feature %s should have a non-nil value", featureName)
				assert.Equal(t, serving.FieldStatus_PRESENT, featureResult.Statuses[j].Status[0], "Feature %s should have a PRESENT status but was %s", featureName, featureResult.Statuses[j].Status[0])
			}
		}
	}
}

func TestGetOnlineFeaturesRange_forNonExistentEntityKey(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: -1}},
		},
	}

	featureNames := getAllRegularFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, 1, len(response.Entities))
	for _, featureResult := range response.Results {
		assert.Equal(t, 1, len(featureResult.Values))
		assert.Equal(t, 1, len(featureResult.Statuses))
		assert.Equal(t, 1, len(featureResult.EventTimestamps))
		for j, value := range featureResult.Values {
			assert.NotNil(t, value)
			assert.Equal(t, 1, len(value.Val))
			assert.Nil(t, value.Val[0].Val)
			assert.Equal(t, serving.FieldStatus_NOT_FOUND, featureResult.Statuses[j].Status[0])
		}
	}
}

func TestGetOnlineFeaturesRange_includesDuplicatedRequestedFeatures(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	featureNames := []string{"int_val", "int_val"}

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit: 10,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assertResponseData(t, response, featureNames, 3, false)
}

func TestGetOnlineFeaturesRange_withEmptySortKeyFilter(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	featureNames := getAllRegularFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities:       entities,
		SortKeyFilters: []*serving.SortKeyFilter{},
		Limit:          10,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assertResponseData(t, response, featureNames, 3, false)
}

func TestGetOnlineFeaturesRange_withFeatureService(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_FeatureService{
			FeatureService: "test_sorted_service",
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit: 10,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)

	featureNames := getAllSortedFeatureNames()
	assertResponseData(t, response, featureNames, 3, false)
}

func TestGetOnlineFeaturesRange_withFeatureViewThrowsError(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	featureNames := getAllRegularFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit: 10,
	}
	_, err := client.GetOnlineFeaturesRange(ctx, request)
	require.Error(t, err, "Expected an error due to regular feature view requested for range query")
	assert.Contains(t, err.Error(), "sorted feature view all_dtypes doesn't exist",
		"Expected error message for non-existent sorted feature view")
}

func assertResponseData(t *testing.T, response *serving.GetOnlineFeaturesRangeResponse, featureNames []string, entitiesRequested int, includeMetadata bool) {
	assert.NotNil(t, response)
	assert.Equal(t, 1, len(response.Entities), "Should have 1 list of entity")
	indexIdEntity, exists := response.Entities["index_id"]
	assert.True(t, exists, "Should have index_id entity")
	assert.NotNil(t, indexIdEntity)
	assert.Equal(t, entitiesRequested, len(indexIdEntity.Val), "Entity should have %d values", entitiesRequested)
	assert.Equal(t, len(featureNames), len(response.Results), "Should have expected number of features")

	for i, featureResult := range response.Results {
		assert.Equal(t, entitiesRequested, len(featureResult.Values))
		if includeMetadata {
			assert.Equal(t, entitiesRequested, len(featureResult.Statuses))
			assert.Equal(t, entitiesRequested, len(featureResult.EventTimestamps), "Feature %s should have %d event timestamps", featureNames[i], entitiesRequested)
		}
		for j, value := range featureResult.Values {
			featureName := featureNames[i]
			if strings.Contains(featureName, "null") {
				// For null features, we expect the value to contain 1 entry with a nil value
				assert.NotNil(t, value)
				assert.Equal(t, 10, len(value.Val), "Feature %s should have one value, got %d %s", featureName, len(value.Val), value.Val)
				assert.Nil(t, value.Val[0].Val, "Feature %s should have a nil value", featureName)
			} else {
				assert.NotNil(t, value)
				assert.Equal(t, 10, len(value.Val), "Feature %s should have 10 values, got %d", featureName, len(value.Val))
			}

			if includeMetadata {
				for k, _ := range value.Val {
					if strings.Contains(featureName, "null") {
						assert.Equal(t, serving.FieldStatus_NULL_VALUE, featureResult.Statuses[j].Status[k], "Feature %s should have NULL status", featureName)
					} else {
						assert.Equal(t, serving.FieldStatus_PRESENT, featureResult.Statuses[j].Status[k], "Feature %s should have PRESENT status", featureName)
					}
				}
			}
		}
	}
}

func getAllSortedFeatureNames() []string {
	return strings.Split(ALL_SORTED_FEATURE_NAMES, ",")
}

func getAllRegularFeatureNames() []string {
	return strings.Split(ALL_REGULAR_FEATURE_NAMES, ",")
}
