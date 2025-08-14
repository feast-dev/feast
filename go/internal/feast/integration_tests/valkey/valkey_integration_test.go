//go:build integration

package valkey

import (
	"context"
	fmt "fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/server"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
)

var client serving.ServingServiceClient
var ctx context.Context
var dir string

func TestMain(m *testing.M) {
	var err error
	dir, err = filepath.Abs("./")
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

func TestGetOnlineFeaturesValkey(t *testing.T) {
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

func getValueType(value interface{}, featureName string) *types.Value {
	if value == nil {
		return &types.Value{}
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
