//go:build integration

package valkey

import (
	"context"
	fmt "fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/server"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ALL_SORTED_FEATURE_NAMES = "int_val,long_val,float_val,double_val,byte_val,string_val,timestamp_val,boolean_val,array_int_val,array_long_val,array_float_val,array_double_val,array_byte_val,array_string_val,array_timestamp_val,array_boolean_val,null_int_val,null_long_val,null_float_val,null_double_val,null_byte_val,null_string_val,null_timestamp_val,null_boolean_val,null_array_int_val,null_array_long_val,null_array_float_val,null_array_double_val,null_array_byte_val,null_array_string_val,null_array_timestamp_val,null_array_boolean_val,event_timestamp"
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
		fmt.Printf("ValkeyOnlineStore Integration Tests failed with exit code %d\n", exitCode)
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

func getAllSortedFeatureNames() []string {
	return strings.Split(ALL_SORTED_FEATURE_NAMES, ",")
}

func sortRowsDescByEvent(rows []map[string]interface{}) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i]["event_timestamp"].(int64) > rows[j]["event_timestamp"].(int64)
	})
}

func convertParquetValue(v interface{}, featureName string) *types.Value {
	return getValueType(v, featureName)
}

func loadParquetRows(t *testing.T) []map[string]interface{} {
	rows, err := test.ReadParquetDynamically(filepath.Join(dir, "feature_repo", "data.parquet"))
	require.NoError(t, err)
	return rows
}

func buildEntities() map[string]*types.RepeatedValue {
	return map[string]*types.RepeatedValue{
		"index_id": {
			Val: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 1}},
				{Val: &types.Value_Int64Val{Int64Val: 2}},
				{Val: &types.Value_Int64Val{Int64Val: 3}},
			},
		},
	}
}

func buildFeatureRefs(featureNames []string) []string {
	out := make([]string, len(featureNames))
	for i, f := range featureNames {
		out[i] = "all_dtypes_sorted:" + f
	}
	return out
}

func expectSortedParquetRows(rows []map[string]interface{}, entityID int64) []map[string]interface{} {
	filtered := test.FilterRowsByColumn(rows, "index_id", entityID)
	sortRowsDescByEvent(filtered)
	return filtered
}

func assertRangeFeatureMatchesParquet(
	t *testing.T,
	resp *serving.GetOnlineFeaturesRangeResponse,
	rows []map[string]interface{},
	feature string,
	featureIndex int,
) {
	result := resp.Results[featureIndex]
	entCol := resp.Entities["index_id"]

	require.Equal(t, 3, len(result.Values)) // 3 entities

	for eIdx, entV := range entCol.Val {
		entityID := entV.GetInt64Val()
		expectedRows := expectSortedParquetRows(rows, entityID)

		// Respect LIMIT
		if len(expectedRows) > len(result.Values[eIdx].Val) {
			expectedRows = expectedRows[:len(result.Values[eIdx].Val)]
		}

		for i := range expectedRows {
			expVal := convertParquetValue(expectedRows[i][feature], feature)
			actVal := result.Values[eIdx].Val[i]

			assert.Equal(t, expVal, actVal,
				"Mismatch feature=%s entity=%d idx=%d", feature, entityID, i)

			if expectedRows[i][feature] == nil {
				assert.Equal(t, serving.FieldStatus_NULL_VALUE, result.Statuses[eIdx].Status[i])
			} else {
				assert.Equal(t, serving.FieldStatus_PRESENT, result.Statuses[eIdx].Status[i])
			}

			expectedTS := expectedRows[i]["event_timestamp"].(int64)
			actualTS := result.EventTimestamps[eIdx].Val[i].GetUnixTimestampVal()

			assert.Equal(t, expectedTS, actualTS)
		}
	}
}

func assertRangeResponseMatchesParquet(
	t *testing.T,
	resp *serving.GetOnlineFeaturesRangeResponse,
	featureNames []string,
) {
	rows := loadParquetRows(t)
	require.Equal(t, len(featureNames), len(resp.Results))

	for fIdx, feature := range featureNames {
		assertRangeFeatureMatchesParquet(t, resp, rows, feature, fIdx)
	}
}

func TestGetOnlineFeaturesRangeValkey(t *testing.T) {
	entities := buildEntities()

	featureNames := getAllSortedFeatureNames()
	fvNames := buildFeatureRefs(featureNames)

	req := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{Val: fvNames},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{
							Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0},
						},
					},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}

	resp, err := client.GetOnlineFeaturesRange(ctx, req)
	require.NoError(t, err)
	assertRangeResponseMatchesParquet(t, resp, featureNames)
}

func TestGetOnlineFeaturesRangeValkey_NoSortKeyFilter(t *testing.T) {
	entities := buildEntities()

	featureNames := getAllSortedFeatureNames()
	fvNames := buildFeatureRefs(featureNames)

	req := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{Val: fvNames},
		},
		Entities:        entities,
		SortKeyFilters:  []*serving.SortKeyFilter{},
		Limit:           10,
		IncludeMetadata: true,
	}

	resp, err := client.GetOnlineFeaturesRange(ctx, req)
	require.NoError(t, err)
	assertRangeResponseMatchesParquet(t, resp, featureNames)
}

func TestGetOnlineFeaturesRangeValkey_ReverseSortOrder(t *testing.T) {
	entities := map[string]*types.RepeatedValue{
		"index_id": {
			Val: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 1}},
				{Val: &types.Value_Int64Val{Int64Val: 2}},
				{Val: &types.Value_Int64Val{Int64Val: 3}},
			},
		},
	}

	featureNames := getAllSortedFeatureNames()

	fvNames := make([]string, 0, len(featureNames))
	for _, fn := range featureNames {
		fvNames = append(fvNames, "all_dtypes_sorted:"+fn)
	}

	req := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{Val: fvNames},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{
							Val: &types.Value_UnixTimestampVal{
								UnixTimestampVal: 0,
							},
						},
					},
				},
			},
		},
		ReverseSortOrder: true,
		Limit:            10,
		IncludeMetadata:  true,
	}

	resp, err := client.GetOnlineFeaturesRange(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)

	rows, err := test.ReadParquetDynamically(filepath.Join(dir, "feature_repo", "data.parquet"))
	require.NoError(t, err)

	entCol := resp.Entities["index_id"]
	require.Equal(t, 3, len(entCol.Val))

	require.Equal(t, len(featureNames), len(resp.Results))

	for fIdx, feature := range featureNames {
		result := resp.Results[fIdx]
		require.Equal(t, 3, len(result.Values))

		for eIdx, entVal := range entCol.Val {
			entityID := entVal.GetInt64Val()

			filtered := test.FilterRowsByColumn(rows, "index_id", entityID)

			sort.Slice(filtered, func(i, j int) bool {
				return filtered[i]["event_timestamp"].(int64) >
					filtered[j]["event_timestamp"].(int64)
			})

			expectedRows := filtered
			if len(expectedRows) > len(result.Values[eIdx].Val) {
				expectedRows = expectedRows[:len(result.Values[eIdx].Val)]
			}

			for i := range expectedRows {

				expVal := convertParquetValue(expectedRows[i][feature], feature)
				actVal := result.Values[eIdx].Val[i]

				assert.Equal(t, expVal, actVal,
					"feature=%s entity=%d idx=%d", feature, entityID, i)

				if expectedRows[i][feature] == nil {
					assert.Equal(t, serving.FieldStatus_NULL_VALUE,
						result.Statuses[eIdx].Status[i])
				} else {
					assert.Equal(t, serving.FieldStatus_PRESENT,
						result.Statuses[eIdx].Status[i])
				}

				expectedTS := expectedRows[i]["event_timestamp"].(int64)
				actualTS := result.EventTimestamps[eIdx].Val[i].GetUnixTimestampVal()

				assert.Equal(t, expectedTS, actualTS,
					"timestamp mismatch feature=%s entity=%d idx=%d",
					feature, entityID, i)
			}
		}
	}
}

func TestGetOnlineFeaturesRangeValkey_RangeStartInclusive(t *testing.T) {
	entities := map[string]*types.RepeatedValue{
		"index_id": {
			Val: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 1}},
			},
		},
	}

	featureNames := getAllSortedFeatureNames()
	fvNames := make([]string, len(featureNames))
	for i, fn := range featureNames {
		fvNames[i] = "all_dtypes_sorted:" + fn
	}

	start := int64(1700000000)

	req := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{Val: fvNames},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{
							Val: &types.Value_UnixTimestampVal{
								UnixTimestampVal: start,
							},
						},
						StartInclusive: true,
					},
				},
			},
		},
		Limit:           100,
		IncludeMetadata: true,
	}

	resp, err := client.GetOnlineFeaturesRange(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)

	rows, err := test.ReadParquetDynamically(filepath.Join(dir, "feature_repo", "data.parquet"))
	require.NoError(t, err)

	entCol := resp.Entities["index_id"]
	require.Equal(t, 1, len(entCol.Val))

	for fIdx, feature := range featureNames {
		result := resp.Results[fIdx]
		filtered := test.FilterRowsByColumn(rows, "index_id", 1)

		filtered = slices.DeleteFunc(filtered, func(r map[string]interface{}) bool {
			return r["event_timestamp"].(int64) < start
		})

		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i]["event_timestamp"].(int64) <
				filtered[j]["event_timestamp"].(int64)
		})

		expected := filtered[:min(len(filtered), len(result.Values[0].Val))]

		for i := range expected {
			expVal := convertParquetValue(expected[i][feature], feature)
			actVal := result.Values[0].Val[i]
			assert.Equal(t, expVal, actVal, "f=%s idx=%d", feature, i)

			expTS := expected[i]["event_timestamp"].(int64)
			actTS := result.EventTimestamps[0].Val[i].GetUnixTimestampVal()
			assert.Equal(t, expTS, actTS)
		}
	}
}
