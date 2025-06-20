//go:build integration

package server

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOnlineFeaturesValkey(t *testing.T) {
	ctx := context.Background()
	dir := "../../../integration_tests/valkey/"
	err := test.SetupInitializedRepo(dir)
	defer test.CleanUpInitializedRepo(dir)
	require.Nil(t, err)

	client, closer := getClient(ctx, "", dir, "")
	defer closer()

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
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRequest{
		Kind: &serving.GetOnlineFeaturesRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
	}
	response, err := client.GetOnlineFeatures(ctx, request)
	assert.Nil(t, err)
	assert.NotNil(t, response)

	expectedEntityValuesResp := []*types.Value{
		{Val: &types.Value_Int64Val{Int64Val: 1}},
		{Val: &types.Value_Int64Val{Int64Val: 2}},
		{Val: &types.Value_Int64Val{Int64Val: 3}},
	}
	expectedFeatureNamesResp := append([]string{"index_id"}, featureNames...)

	rows, err := test.ReadParquetDynamically(filepath.Join(dir, "feature_repo", "data.parquet"))
	assert.Nil(t, err)

	for featureIndex, feature := range featureNames {
		expectedResponse := []*types.Value{}

		for _, value := range entities["index_id"].Val {
			filteredRow := test.FilterRowsByColumn(rows, "index_id", value.GetInt64Val())
			if len(filteredRow) == 0 {
				if feature == "array_int_val" {
					expectedResponse = append(expectedResponse, &types.Value{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: nil}}})
				} else if feature == "array_long_val" {
					expectedResponse = append(expectedResponse, &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: nil}}})
				} else if feature == "array_float_val" {
					expectedResponse = append(expectedResponse, &types.Value{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: nil}}})
				} else if feature == "array_double_val" {
					expectedResponse = append(expectedResponse, &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: nil}}})
				} else if feature == "array_string_val" {
					expectedResponse = append(expectedResponse, &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: nil}}})
				} else if feature == "array_boolean_val" {
					expectedResponse = append(expectedResponse, &types.Value{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: nil}}})
				} else if feature == "array_byte_val" {
					expectedResponse = append(expectedResponse, &types.Value{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: nil}}})
				} else if feature == "array_timestamp_val" {
					expectedResponse = append(expectedResponse, &types.Value{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: nil}}})
				} else {
					expectedResponse = append(expectedResponse, &types.Value{})
				}
			} else {
				expectedResponse = append(expectedResponse, getValueType(filteredRow[0][feature], feature))
			}
		}
		assert.True(t, reflect.DeepEqual(response.Results[featureIndex+1].Values, expectedResponse), feature+" has mismatch")
	}
	assert.True(t, reflect.DeepEqual(response.Metadata.FeatureNames.Val, expectedFeatureNamesResp))
	assert.True(t, reflect.DeepEqual(response.Results[0].Values, expectedEntityValuesResp))
	// Columnar so get in column format row by row should have column names of all features
	assert.Equal(t, len(response.Results), len(featureNames)+1)
}

func TestGetOnlineFeaturesRange(t *testing.T) {
	ctx := context.Background()
	dir := "../../../integration_tests/scylladb/"
	err := test.SetupInitializedRepo(dir)
	defer test.CleanUpInitializedRepo(dir)
	require.NoError(t, err, "Failed to setup initialized repo with err: %v", err)

	client, closer := getClient(ctx, "", dir, "")
	defer closer()

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
	assert.Equal(t, 33, len(response.Results))

	for i, featureResult := range response.Results {
		assert.Equal(t, 3, len(featureResult.Values))
		for _, value := range featureResult.Values {
			if i == 0 {
				// The first result is the entity key which should only have 1 entry
				assert.NotNil(t, value)
				assert.Equal(t, 1, len(value.Val), "Entity Key should have 1 value, got %d", len(value.Val))
			} else {
				featureName := featureNames[i-1] // The first entry is the entity key
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
}

func getValueType(value interface{}, featureName string) *types.Value {
	if value == nil {
		if featureName == "timestamp_val" || featureName == "null_timestamp_val" {
			return &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: -9223372036854775808}}
		} else {
			return &types.Value{}
		}
	}
	switch value.(type) {
	case int32:
		return &types.Value{Val: &types.Value_Int32Val{Int32Val: value.(int32)}}
	case int64:
		// Check if featureName contains "timestamp"
		if strings.Contains(featureName, "timestamp") {
			return &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: value.(int64)}}
		} else {
			if value == nil {
				return &types.Value{}
			}
			return &types.Value{Val: &types.Value_Int64Val{Int64Val: value.(int64)}}
		}
	case float32:
		return &types.Value{Val: &types.Value_FloatVal{FloatVal: value.(float32)}}
	case float64:
		return &types.Value{Val: &types.Value_DoubleVal{DoubleVal: value.(float64)}}
	case bool:
		return &types.Value{Val: &types.Value_BoolVal{BoolVal: value.(bool)}}
	case []byte:
		return &types.Value{Val: &types.Value_BytesVal{BytesVal: value.([]uint8)}}
	case string:
		return &types.Value{Val: &types.Value_StringVal{StringVal: value.(string)}}

	case []interface{}:
		arrayInterface := value.([]interface{})

		switch arrayInterface[0].(type) {
		case int32:
			arrayValue := []int32{}
			for _, v := range arrayInterface {
				arrayValue = append(arrayValue, v.(int32))
			}
			return &types.Value{Val: &types.Value_Int32ListVal{&types.Int32List{Val: arrayValue}}}
		case int64:
			arrayValue := []int64{}
			if strings.Contains(featureName, "timestamp") {

				for _, v := range arrayInterface {
					if v.(int64) == 0 {
						arrayValue = append(arrayValue, -9223372036854775808)
					} else {
						arrayValue = append(arrayValue, v.(int64))
					}
				}
				return &types.Value{Val: &types.Value_UnixTimestampListVal{&types.Int64List{Val: arrayValue}}}
			} else {

				for _, v := range arrayInterface {
					arrayValue = append(arrayValue, v.(int64))
				}
				return &types.Value{Val: &types.Value_Int64ListVal{&types.Int64List{Val: arrayValue}}}
			}
		case float32:
			arrayValue := []float32{}
			for _, v := range arrayInterface {
				arrayValue = append(arrayValue, v.(float32))
			}
			return &types.Value{Val: &types.Value_FloatListVal{&types.FloatList{Val: arrayValue}}}
		case float64:
			arrayValue := []float64{}
			for _, v := range arrayInterface {
				arrayValue = append(arrayValue, v.(float64))
			}
			return &types.Value{Val: &types.Value_DoubleListVal{&types.DoubleList{Val: arrayValue}}}
		case bool:
			arrayValue := []bool{}
			for _, v := range arrayInterface {
				arrayValue = append(arrayValue, v.(bool))
			}
			return &types.Value{Val: &types.Value_BoolListVal{&types.BoolList{Val: arrayValue}}}
		case string:
			arrayValue := []string{}
			for _, v := range arrayInterface {
				arrayValue = append(arrayValue, v.(string))
			}
			return &types.Value{Val: &types.Value_StringListVal{&types.StringList{Val: arrayValue}}}
		case []byte:
			arrayValue := [][]uint8{}
			for _, v := range arrayInterface {
				arrayValue = append(arrayValue, v.([]uint8))
			}
			return &types.Value{Val: &types.Value_BytesListVal{&types.BytesList{Val: arrayValue}}}

		default:
			return &types.Value{}
		}

	default:
		return &types.Value{}
	}
}
