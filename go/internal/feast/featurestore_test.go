package feast

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	types2 "github.com/feast-dev/feast/go/types"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/durationpb"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Return absolute path to the test_repo registry regardless of the working directory
func getRegistryPath() map[string]interface{} {
	// Get the file path of this source file, regardless of the working directory
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("couldn't find file path of the test file")
	}
	registry := map[string]interface{}{
		"path": filepath.Join(filename, "..", "..", "..", "feature_repo/data/registry.db"),
	}
	return registry
}

func TestNewFeatureStore(t *testing.T) {
	t.Skip("@todo(achals): feature_repo isn't checked in yet")
	config := registry.RepoConfig{
		Project:  "feature_repo",
		Registry: getRegistryPath(),
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type": "redis",
		},
	}
	fs, err := NewFeatureStore(&config, nil)
	assert.Nil(t, err)
	assert.IsType(t, &onlinestore.RedisOnlineStore{}, fs.onlineStore)
}

func TestGetOnlineFeaturesRedis(t *testing.T) {
	t.Skip("@todo(achals): feature_repo isn't checked in yet")
	config := registry.RepoConfig{
		Project:  "feature_repo",
		Registry: getRegistryPath(),
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type":              "redis",
			"connection_string": "localhost:6379",
		},
	}

	featureNames := []string{"driver_hourly_stats:conv_rate",
		"driver_hourly_stats:acc_rate",
		"driver_hourly_stats:avg_daily_trips",
	}
	entities := map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1002}},
		{Val: &types.Value_Int64Val{Int64Val: 1003}}}},
	}

	fs, err := NewFeatureStore(&config, nil)
	assert.Nil(t, err)
	ctx := context.Background()
	response, err := fs.GetOnlineFeatures(
		ctx, featureNames, nil, entities, map[string]*types.RepeatedValue{}, true)
	assert.Nil(t, err)
	assert.Len(t, response, 4) // 3 Features + 1 entity = 4 columns (feature vectors) in response
}

// MockOnlineStore implements the OnlineStore interface for testing without a real online store
type MockOnlineStore struct {
	mock.Mock
}

func (m *MockOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]onlinestore.FeatureData, error) {
	args := m.Called(ctx, entityKeys, featureViewNames, featureNames)
	return args.Get(0).([][]onlinestore.FeatureData), args.Error(1)
}

func (m *MockOnlineStore) OnlineReadRange(ctx context.Context, groupedRefs *model.GroupedRangeFeatureRefs) ([][]onlinestore.RangeFeatureData, error) {
	args := m.Called(ctx, groupedRefs)
	return args.Get(0).([][]onlinestore.RangeFeatureData), args.Error(1)
}

func (m *MockOnlineStore) Destruct() {
	m.Called()
}

func TestGetOnlineFeaturesRange(t *testing.T) {
	mockStore := new(MockOnlineStore)

	// Set up test entities and feature views without using a registry
	testEntity := &model.Entity{
		Name:    "driver",
		JoinKey: "driver_id",
	}

	sortKey := &model.SortKey{
		FieldName: "event_timestamp",
		ValueType: types.ValueType_UNIX_TIMESTAMP,
		Order:     model.NewSortOrderFromProto(core.SortOrder_ASC),
	}

	sortedFV := &model.SortedFeatureView{
		FeatureView: &model.FeatureView{
			Base: &model.BaseFeatureView{
				Name: "driver_stats",
			},
			EntityNames: []string{"driver"},
			Ttl:         &durationpb.Duration{Seconds: 86400},
		},
		SortKeys: []*model.SortKey{sortKey},
	}

	ctx := context.Background()
	featureRefs := []string{"driver_stats:conv_rate", "driver_stats:acc_rate"}

	entityValues := map[string]*types.RepeatedValue{
		"driver_id": {
			Val: []*types.Value{
				{Val: &types.Value_Int64Val{Int64Val: 1001}},
				{Val: &types.Value_Int64Val{Int64Val: 1002}},
			},
		},
	}

	now := time.Now()
	oneWeekAgo := now.AddDate(0, 0, -7)

	sortKeyProto := &serving.SortKeyFilter{
		SortKeyName: "event_timestamp",
		Query: &serving.SortKeyFilter_Range{
			Range: &serving.SortKeyFilter_RangeQuery{
				RangeStart: &types.Value{
					Val: &types.Value_UnixTimestampVal{UnixTimestampVal: oneWeekAgo.Unix()},
				},
				RangeEnd: &types.Value{
					Val: &types.Value_UnixTimestampVal{UnixTimestampVal: now.Unix()},
				},
				StartInclusive: true,
				EndInclusive:   true,
			},
		},
	}

	expectedFilter := model.NewSortKeyFilterFromProto(sortKeyProto, nil)
	filterMatcher := func(fs []*model.SortKeyFilter) bool {
		if len(fs) != 1 {
			return false
		}
		f := fs[0]
		sameBase :=
			f.SortKeyName == expectedFilter.SortKeyName &&
				f.StartInclusive == expectedFilter.StartInclusive &&
				f.EndInclusive == expectedFilter.EndInclusive &&
				fmt.Sprint(f.RangeStart) == fmt.Sprint(expectedFilter.RangeStart) &&
				fmt.Sprint(f.RangeEnd) == fmt.Sprint(expectedFilter.RangeEnd)

		if f.Order == nil && expectedFilter.Order == nil {
			return sameBase
		}
		return sameBase && f.Order != nil && expectedFilter.Order != nil &&
			f.Order.Order == expectedFilter.Order.Order
	}
	mockRangeFeatureData := [][]onlinestore.RangeFeatureData{
		{
			{
				FeatureView: "driver_stats",
				FeatureName: "conv_rate",
				Values:      []interface{}{0.85, 0.87, 0.89},
				EventTimestamps: []timestamp.Timestamp{
					{Seconds: now.Unix() - 86400*3},
					{Seconds: now.Unix() - 86400*2},
					{Seconds: now.Unix() - 86400*1},
				},
			},
			{
				FeatureView: "driver_stats",
				FeatureName: "acc_rate",
				Values:      []interface{}{0.91, 0.92, 0.94},
				EventTimestamps: []timestamp.Timestamp{
					{Seconds: now.Unix() - 86400*3},
					{Seconds: now.Unix() - 86400*2},
					{Seconds: now.Unix() - 86400*1},
				},
			},
		},
		{
			{
				FeatureView: "driver_stats",
				FeatureName: "conv_rate",
				Values:      []interface{}{0.78, 0.80},
				EventTimestamps: []timestamp.Timestamp{
					{Seconds: now.Unix() - 86400*3},
					{Seconds: now.Unix() - 86400*1},
				},
			},
			{
				FeatureView: "driver_stats",
				FeatureName: "acc_rate",
				Values:      []interface{}{0.85, 0.88},
				EventTimestamps: []timestamp.Timestamp{
					{Seconds: now.Unix() - 86400*3},
					{Seconds: now.Unix() - 86400*1},
				},
			},
		},
	}

	featureViewNamesMatcher := func(views []string) bool {
		for _, view := range views {
			if view != "driver_stats" {
				return false
			}
		}
		return len(views) > 0
	}

	entityKeysMatcher := func(keys []*types.EntityKey) bool {
		if len(keys) != len(entityValues["driver_id"].Val) {
			return false
		}
		for i, key := range keys {
			if len(key.JoinKeys) != 1 || key.JoinKeys[0] != "driver_id" {
				return false
			}
			if len(key.EntityValues) != 1 || key.EntityValues[0].GetInt64Val() != entityValues["driver_id"].Val[i].GetInt64Val() {
				return false
			}
		}
		return true
	}

	groupedRefsMatcher := mock.MatchedBy(func(ref *model.GroupedRangeFeatureRefs) bool {
		return entityKeysMatcher(ref.EntityKeys) &&
			featureViewNamesMatcher(ref.FeatureViewNames) &&
			len(ref.FeatureNames) == 2 &&
			ref.FeatureNames[0] == "conv_rate" &&
			ref.FeatureNames[1] == "acc_rate" &&
			filterMatcher(ref.SortKeyFilters) &&
			ref.Limit == 0 &&
			ref.IsReverseSortOrder == false
	})

	mockStore.On("OnlineReadRange",
		mock.Anything,
		groupedRefsMatcher,
	).Return(mockRangeFeatureData, nil)

	result, err := testGetOnlineFeaturesRange(
		ctx,
		mockStore,
		featureRefs,
		[]*model.Entity{testEntity},
		[]*model.SortedFeatureView{sortedFV},
		entityValues,
		[]*serving.SortKeyFilter{sortKeyProto},
		false,
		0,
		nil,
		true,
	)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result), "Should have 3 vectors (1 entity + 2 features)")
	var driverIdVector, accRateVector, convRateVector *onlineserving.RangeFeatureVector
	for _, r := range result {
		switch r.Name {
		case "driver_id":
			driverIdVector = r
		case "driver_stats__acc_rate":
			accRateVector = r
		case "driver_stats__conv_rate":
			convRateVector = r
		}
	}

	assert.NotNil(t, driverIdVector)
	assert.NotNil(t, accRateVector)
	assert.NotNil(t, convRateVector)

	accRateValues, err := types2.ArrowValuesToProtoValues(accRateVector.RangeValues)
	assert.NoError(t, err)
	convRateValues, err := types2.ArrowValuesToProtoValues(convRateVector.RangeValues)
	assert.NoError(t, err)
	assert.Equal(t, []float64{0.91, 0.92, 0.94}, accRateValues[0].GetDoubleListVal().Val)
	assert.Equal(t, []float64{0.85, 0.87, 0.89}, convRateValues[0].GetDoubleListVal().Val)
	assert.Equal(t, []float64{0.85, 0.88}, accRateValues[1].GetDoubleListVal().Val)
	assert.Equal(t, []float64{0.78, 0.80}, convRateValues[1].GetDoubleListVal().Val)

	assert.Equal(t, 3, len(accRateVector.RangeStatuses[0]))
	assert.Equal(t, 3, len(convRateVector.RangeStatuses[0]))
	assert.Equal(t, 2, len(accRateVector.RangeStatuses[1]))
	assert.Equal(t, 2, len(convRateVector.RangeStatuses[1]))

	mockStore.AssertExpectations(t)
}

// This is a test helper function that mimics the core logic of FeatureStore.GetOnlineFeaturesRange
// but accepts test data directly instead of using a registry
// TODO: Refactor to use the real online store when the OnlineReadRange method is implemented.
func testGetOnlineFeaturesRange(
	ctx context.Context,
	store onlinestore.OnlineStore,
	featureRefs []string,
	entities []*model.Entity,
	sortedViews []*model.SortedFeatureView,
	joinKeyToEntityValues map[string]*types.RepeatedValue,
	sortKeyFilters []*serving.SortKeyFilter,
	reverseSortOrder bool,
	limit int32,
	requestData map[string]*types.RepeatedValue,
	fullFeatureNames bool) ([]*onlineserving.RangeFeatureVector, error) {

	sortedFeatureViews := make([]*onlineserving.SortedFeatureViewAndRefs, 0)
	for _, view := range sortedViews {
		viewFeatures := make([]string, 0)
		for _, featureRef := range featureRefs {
			viewName, featureName, _ := onlineserving.ParseFeatureReference(featureRef)
			if viewName == view.Base.Name {
				viewFeatures = append(viewFeatures, featureName)
			}
		}

		if len(viewFeatures) > 0 {
			sortedFeatureViews = append(sortedFeatureViews, &onlineserving.SortedFeatureViewAndRefs{
				View:        view,
				FeatureRefs: viewFeatures,
			})
		}
	}

	entityNameToJoinKeyMap := make(map[string]string)
	for _, entity := range entities {
		entityNameToJoinKeyMap[entity.Name] = entity.JoinKey
	}
	expectedJoinKeysSet := make(map[string]interface{})
	for _, joinKey := range entityNameToJoinKeyMap {
		expectedJoinKeysSet[joinKey] = nil
	}

	numRows, err := onlineserving.ValidateEntityValues(joinKeyToEntityValues, requestData, expectedJoinKeysSet)
	if err != nil {
		return nil, err
	}

	err = onlineserving.ValidateSortKeyFilters(sortKeyFilters, sortedFeatureViews)
	if err != nil {
		return nil, err
	}

	arrowAllocator := memory.NewGoAllocator()
	entityColumns, err := onlineserving.EntitiesToRangeFeatureVectors(
		joinKeyToEntityValues, arrowAllocator, numRows)
	if err != nil {
		return nil, err
	}

	result := make([]*onlineserving.RangeFeatureVector, 0, len(entityColumns))
	result = append(result, entityColumns...)

	groupedRangeRefs, err := onlineserving.GroupSortedFeatureRefs(
		sortedFeatureViews,
		joinKeyToEntityValues,
		entityNameToJoinKeyMap,
		sortKeyFilters,
		reverseSortOrder,
		limit,
		fullFeatureNames)
	if err != nil {
		return nil, err
	}

	for _, groupRef := range groupedRangeRefs {
		featureData, err := store.OnlineReadRange(
			ctx,
			groupRef)
		if err != nil {
			return nil, err
		}

		vectors, err := onlineserving.TransposeRangeFeatureRowsIntoColumns(
			featureData,
			groupRef,
			sortedFeatureViews,
			arrowAllocator,
			numRows,
		)
		if err != nil {
			return nil, err
		}

		result = append(result, vectors...)
	}

	return result, nil
}

func assertValueTypes(t *testing.T, actualValues []*types.Value, expectedType string) {
	for _, value := range actualValues {
		assert.Equal(t, expectedType, fmt.Sprintf("%T", value.GetVal()), expectedType)
	}
}

func TestEntityTypeConversion_WithValidValues(t *testing.T) {
	entityMap := map[string]*types.RepeatedValue{
		"int32":         {Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1002}}, {Val: &types.Value_Int32Val{Int32Val: 2003}}}},
		"int64>int32":   {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}, {Val: &types.Value_Int64Val{Int64Val: 2002}}}},
		"float":         {Val: []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 1.23}}, {Val: &types.Value_FloatVal{FloatVal: 4.56}}}},
		"float64>float": {Val: []*types.Value{{Val: &types.Value_DoubleVal{DoubleVal: 10.32}}, {Val: &types.Value_DoubleVal{DoubleVal: 20.64}}}},
		"bytes":         {Val: []*types.Value{{Val: &types.Value_BytesVal{BytesVal: []byte("test")}}, {Val: &types.Value_BytesVal{BytesVal: []byte("data")}}}},
		"string>bytes":  {Val: []*types.Value{{Val: &types.Value_StringVal{StringVal: "test"}}, {Val: &types.Value_StringVal{StringVal: "data"}}}},
	}
	entityColumns := map[string]*model.Field{
		"int32":         {Name: "int32", Dtype: types.ValueType_INT32},
		"int64>int32":   {Name: "int64>int32", Dtype: types.ValueType_INT32},
		"float":         {Name: "float", Dtype: types.ValueType_FLOAT},
		"float64>float": {Name: "float64>float", Dtype: types.ValueType_FLOAT},
		"bytes":         {Name: "bytes", Dtype: types.ValueType_BYTES},
		"string>bytes":  {Name: "string>bytes", Dtype: types.ValueType_BYTES},
	}

	err := entityTypeConversion(entityMap, entityColumns)
	assert.NoError(t, err)
	assertValueTypes(t, entityMap["int32"].Val, "*types.Value_Int32Val")
	assertValueTypes(t, entityMap["int64>int32"].Val, "*types.Value_Int32Val")
	assertValueTypes(t, entityMap["float"].Val, "*types.Value_FloatVal")
	assertValueTypes(t, entityMap["float64>float"].Val, "*types.Value_FloatVal")
	assertValueTypes(t, entityMap["bytes"].Val, "*types.Value_BytesVal")
	assertValueTypes(t, entityMap["string>bytes"].Val, "*types.Value_BytesVal")
}

func TestEntityTypeConversion_WithInvalidValues(t *testing.T) {
	entityMaps := []map[string]*types.RepeatedValue{
		{"int32": {Val: []*types.Value{{Val: &types.Value_StringVal{StringVal: "invalid"}}}}},
		{"float": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}}}},
		{"bytes": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}}}},
	}
	entityColumns := []map[string]*model.Field{
		{"int32": {Name: "int32", Dtype: types.ValueType_INT32}},
		{"float": {Name: "float", Dtype: types.ValueType_FLOAT}},
		{"bytes": {Name: "bytes", Dtype: types.ValueType_BYTES}},
	}
	expectedErrors := []string{
		"error converting entity value for int32: unsupported value type for conversion: INT32 for actual value type: *types.Value_StringVal",
		"error converting entity value for float: unsupported value type for conversion: FLOAT for actual value type: *types.Value_Int64Val",
		"error converting entity value for bytes: unsupported value type for conversion: BYTES for actual value type: *types.Value_Int64Val",
	}

	for i, entityMap := range entityMaps {
		err := entityTypeConversion(entityMap, entityColumns[i])
		assert.Error(t, err)
		assert.Equal(t, expectedErrors[i], err.Error())
	}
}

func TestSortKeyFilterTypeConversion_WithValidValues(t *testing.T) {
	sortKeyFilters := []*serving.SortKeyFilter{
		{SortKeyName: "int32_equals", Query: &serving.SortKeyFilter_Equals{Equals: &types.Value{Val: &types.Value_Int64Val{Int64Val: 1001}}}},
		{SortKeyName: "int32_range", Query: &serving.SortKeyFilter_Range{Range: &serving.SortKeyFilter_RangeQuery{
			RangeStart: &types.Value{Val: &types.Value_Int64Val{Int64Val: 1000}},
			RangeEnd:   &types.Value{Val: &types.Value_Int64Val{Int64Val: 2000}},
		}}},
		{SortKeyName: "float32_equals", Query: &serving.SortKeyFilter_Equals{Equals: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.32}}}},
		{SortKeyName: "float32_range", Query: &serving.SortKeyFilter_Range{Range: &serving.SortKeyFilter_RangeQuery{
			RangeStart: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.32}},
			RangeEnd:   &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 20.64}},
		}}},
		{SortKeyName: "bytes_equals", Query: &serving.SortKeyFilter_Equals{Equals: &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}}},
		{SortKeyName: "bytes_range", Query: &serving.SortKeyFilter_Range{Range: &serving.SortKeyFilter_RangeQuery{
			RangeEnd: &types.Value{Val: &types.Value_StringVal{StringVal: "test"}},
		}}},
		{SortKeyName: "timestamp_equals", Query: &serving.SortKeyFilter_Equals{Equals: &types.Value{Val: &types.Value_Int64Val{Int64Val: time.Now().Unix()}}}},
		{SortKeyName: "timestamp_range", Query: &serving.SortKeyFilter_Range{Range: &serving.SortKeyFilter_RangeQuery{
			RangeStart: &types.Value{Val: &types.Value_Int64Val{Int64Val: time.Now().Unix()}},
		}}},
	}
	sortKeys := map[string]*model.SortKey{
		"int32_equals":     {FieldName: "int32_equals", ValueType: types.ValueType_INT32},
		"int32_range":      {FieldName: "int32_range", ValueType: types.ValueType_INT32},
		"float32_equals":   {FieldName: "float32_equals", ValueType: types.ValueType_FLOAT},
		"float32_range":    {FieldName: "float32_range", ValueType: types.ValueType_FLOAT},
		"bytes_equals":     {FieldName: "bytes_equals", ValueType: types.ValueType_BYTES},
		"bytes_range":      {FieldName: "bytes_range", ValueType: types.ValueType_BYTES},
		"timestamp_equals": {FieldName: "timestamp_equals", ValueType: types.ValueType_UNIX_TIMESTAMP},
		"timestamp_range":  {FieldName: "timestamp_range", ValueType: types.ValueType_UNIX_TIMESTAMP},
	}
	filters, err := sortKeyFilterTypeConversion(sortKeyFilters, sortKeys)
	assert.NoError(t, err)
	int32values := []*types.Value{
		filters[0].GetEquals(),
		filters[1].GetRange().GetRangeStart(),
		filters[1].GetRange().GetRangeEnd(),
	}
	assertValueTypes(t, int32values, "*types.Value_Int32Val")
	floatValues := []*types.Value{
		filters[2].GetEquals(),
		filters[3].GetRange().GetRangeStart(),
		filters[3].GetRange().GetRangeEnd(),
	}
	assertValueTypes(t, floatValues, "*types.Value_FloatVal")
	bytesValues := []*types.Value{
		filters[4].GetEquals(),
		filters[5].GetRange().GetRangeEnd(),
	}
	assertValueTypes(t, bytesValues, "*types.Value_BytesVal")
	assert.Nil(t, filters[5].GetRange().GetRangeStart())
	timestampValues := []*types.Value{
		filters[6].GetEquals(),
		filters[7].GetRange().GetRangeStart(),
	}
	assertValueTypes(t, timestampValues, "*types.Value_UnixTimestampVal")
	assert.Nil(t, filters[7].GetRange().GetRangeEnd())
}
