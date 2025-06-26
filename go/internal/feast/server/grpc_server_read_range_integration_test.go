//go:build integration

package server

import (
	"context"
	"fmt"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

var client serving.ServingServiceClient
var ctx context.Context

func TestMain(m *testing.M) {
	dir := "../../../integration_tests/scylladb/"
	err := test.SetupInitializedRepo(dir)
	if err != nil {
		fmt.Printf("Failed to set up test environment: %v\n", err)
		os.Exit(1)
	}

	ctx = context.Background()
	var closer func()

	client, closer = getClient(ctx, "", dir, "")

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

	featureNames := []string{"int_val", "long_val", "float_val", "double_val", "byte_val", "string_val", "timestamp_val", "boolean_val",
		"null_int_val", "null_long_val", "null_float_val", "null_double_val", "null_byte_val", "null_string_val", "null_timestamp_val", "null_boolean_val",
		"null_array_int_val", "null_array_long_val", "null_array_float_val", "null_array_double_val", "null_array_byte_val", "null_array_string_val",
		"null_array_boolean_val", "array_int_val", "array_long_val", "array_float_val", "array_double_val", "array_string_val", "array_boolean_val",
		"array_byte_val", "array_timestamp_val", "null_array_timestamp_val"}

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
	assert.NotNil(t, response)
	assert.Equal(t, 1, len(response.Entities), "Should have 1 entity")
	entityValues := response.Entities[0]
	assert.NotNil(t, entityValues)
	assert.Equal(t, 3, len(entityValues.Val), "Entity should have 3 values")
	assert.Equal(t, 32, len(response.Results), "Should have 32 features")

	for i, featureResult := range response.Results {
		assert.Equal(t, 3, len(featureResult.Values))
		for _, value := range featureResult.Values {
			featureName := featureNames[i]
			if strings.Contains(featureName, "null") {
				// For null features, we expect the value to contain 1 entry with a nil value
				assert.NotNil(t, value)
				assert.Equal(t, 1, len(value.Val), "Feature %s should have one values, got %d", featureName, len(value.Val))
				assert.Nil(t, value.Val[0].Val, "Feature %s should have a nil value", featureName)
			} else {
				assert.NotNil(t, value)
				assert.Equal(t, 10, len(value.Val), "Feature %s should have 10 values, got %d", featureName, len(value.Val))
			}
		}
	}
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

	featureNames := []string{"int_val", "long_val", "float_val", "double_val", "byte_val", "string_val", "timestamp_val", "boolean_val",
		"null_int_val", "null_long_val", "null_float_val", "null_double_val", "null_byte_val", "null_string_val", "null_timestamp_val", "null_boolean_val",
		"null_array_int_val", "null_array_long_val", "null_array_float_val", "null_array_double_val", "null_array_byte_val", "null_array_string_val",
		"null_array_boolean_val", "array_int_val", "array_long_val", "array_float_val", "array_double_val", "array_string_val", "array_boolean_val",
		"array_byte_val", "array_timestamp_val", "null_array_timestamp_val"}

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
	assert.NotNil(t, response)
	assert.Equal(t, 1, len(response.Entities), "Should have 1 entity")
	entityValues := response.Entities[0]
	assert.NotNil(t, entityValues)
	assert.Equal(t, 3, len(entityValues.Val), "Entity should have 3 values")
	assert.Equal(t, 32, len(response.Results), "Should have 32 features")

	for i, featureResult := range response.Results {
		assert.Equal(t, 3, len(featureResult.Values))
		for _, value := range featureResult.Values {
			featureName := featureNames[i]
			if strings.Contains(featureName, "null") {
				// For null features, we expect the value to contain 1 entry with a nil value
				assert.NotNil(t, value)
				assert.Equal(t, 1, len(value.Val), "Feature %s should have one values, got %d", featureName, len(value.Val))
				assert.Nil(t, value.Val[0].Val, "Feature %s should have a nil value", featureName)
			} else {
				assert.NotNil(t, value)
				assert.Equal(t, 10, len(value.Val), "Feature %s should have 10 values, got %d", featureName, len(value.Val))
			}
		}
	}
}
