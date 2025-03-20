package feast

import (
	"context"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/durationpb"
	"path/filepath"
	"runtime"
	"sort"
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

func (m *MockOnlineStore) OnlineReadRange(ctx context.Context, entityRows []*types.EntityKey, featureViewNames []string, featureNames []string, sortKeyFilters []*serving.SortKeyFilter, reverseSortOrder bool, limit int32) ([][]onlinestore.RangeFeatureData, error) {
	args := m.Called(ctx, entityRows, featureViewNames, featureNames, sortKeyFilters, reverseSortOrder, limit)
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

	sortKeyFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "event_timestamp",
			RangeStart: &types.Value{
				Val: &types.Value_UnixTimestampVal{
					UnixTimestampVal: oneWeekAgo.Unix(),
				},
			},
			RangeEnd: &types.Value{
				Val: &types.Value_UnixTimestampVal{
					UnixTimestampVal: now.Unix(),
				},
			},
			StartInclusive: true,
			EndInclusive:   true,
		},
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

	featureViewNamesMatcher := mock.MatchedBy(func(views []string) bool {
		for _, view := range views {
			if view != "driver_stats" {
				return false
			}
		}
		return len(views) > 0
	})

	mockStore.On("OnlineReadRange",
		mock.Anything,
		mock.AnythingOfType("[]*types.EntityKey"),
		featureViewNamesMatcher,
		[]string{"conv_rate", "acc_rate"},
		sortKeyFilters,
		false,
		int32(0),
	).Return(mockRangeFeatureData, nil)

	result, err := testGetOnlineFeaturesRange(
		ctx,
		mockStore,
		featureRefs,
		[]*model.Entity{testEntity},
		[]*model.SortedFeatureView{sortedFV},
		entityValues,
		sortKeyFilters,
		false,
		0,
		nil,
		true,
	)

	// Sort the result by name, so we can assert by index consistently
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result), "Should have 3 vectors (1 entity + 2 features)")
	assert.Equal(t, "driver_id", result[0].Name)
	assert.Equal(t, "driver_stats__acc_rate", result[1].Name)
	assert.Equal(t, "driver_stats__conv_rate", result[2].Name)
	assert.Equal(t, 2, len(result[1].RangeStatuses))
	assert.Equal(t, 3, len(result[1].RangeStatuses[0]))
	assert.Equal(t, 2, len(result[1].RangeStatuses[1]))
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

	entityNameToJoinKeyMap := make(map[string]string)
	for _, entity := range entities {
		entityNameToJoinKeyMap[entity.Name] = entity.JoinKey
	}

	expectedJoinKeysSet := make(map[string]interface{})
	for _, joinKey := range entityNameToJoinKeyMap {
		expectedJoinKeysSet[joinKey] = nil
	}

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
			groupRef.EntityKeys,
			groupRef.FeatureViewNames,
			groupRef.FeatureNames,
			groupRef.SortKeyFilters,
			groupRef.ReverseSortOrder,
			groupRef.Limit)
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
