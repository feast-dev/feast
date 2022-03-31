package feast

import (
	"context"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"runtime"
	"testing"
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

func dummyTransformCallback(ODFVName string, inputArrPtr, inputSchemaPtr, outArrPtr, outSchemaPtr uintptr, fullFeatureNames bool) int {
	return 0
}

func TestNewFeatureStore(t *testing.T) {
	t.Skip("@todo(achals): feature_repo isn't checked in yet")
	config := RepoConfig{
		Project:  "feature_repo",
		Registry: getRegistryPath(),
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type": "redis",
		},
	}
	fs, err := NewFeatureStore(&config, dummyTransformCallback)
	assert.Nil(t, err)
	assert.IsType(t, &RedisOnlineStore{}, fs.onlineStore)
}

func TestGetOnlineFeaturesRedis(t *testing.T) {
	t.Skip("@todo(achals): feature_repo isn't checked in yet")
	config := RepoConfig{
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

	fs, err := NewFeatureStore(&config, dummyTransformCallback)
	assert.Nil(t, err)
	ctx := context.Background()
	response, err := fs.GetOnlineFeatures(
		ctx, featureNames, nil, entities, map[string]*types.RepeatedValue{}, true)
	assert.Nil(t, err)
	assert.Len(t, response, 4) // 3 Features + 1 entity = 4 columns (feature vectors) in response
}

func TestGroupingFeatureRefs(t *testing.T) {
	viewA := &FeatureView{
		Base: &BaseFeatureView{
			Name: "viewA",
			Projection: &FeatureViewProjection{
				NameAlias: "aliasViewA",
			},
		},
		Entities: map[string]struct{}{"driver": {}, "customer": {}},
	}
	viewB := &FeatureView{
		Base:     &BaseFeatureView{Name: "viewB"},
		Entities: map[string]struct{}{"driver": {}, "customer": {}},
	}
	viewC := &FeatureView{
		Base:     &BaseFeatureView{Name: "viewC"},
		Entities: map[string]struct{}{"driver": {}},
	}
	viewD := &FeatureView{
		Base:     &BaseFeatureView{Name: "viewD"},
		Entities: map[string]struct{}{"customer": {}},
	}
	refGroups, _ := groupFeatureRefs(
		[]*featureViewAndRefs{
			{view: viewA, featureRefs: []string{"featureA", "featureB"}},
			{view: viewB, featureRefs: []string{"featureC", "featureD"}},
			{view: viewC, featureRefs: []string{"featureE"}},
			{view: viewD, featureRefs: []string{"featureF"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		true,
	)

	assert.Len(t, refGroups, 3)

	// Group 1
	assert.Equal(t, []string{"featureA", "featureB", "featureC", "featureD"},
		refGroups["customer_id,driver_id"].featureNames)
	assert.Equal(t, []string{"viewA", "viewA", "viewB", "viewB"},
		refGroups["customer_id,driver_id"].featureViewNames)
	assert.Equal(t, []string{
		"aliasViewA__featureA", "aliasViewA__featureB",
		"viewB__featureC", "viewB__featureD"},
		refGroups["customer_id,driver_id"].aliasedFeatureNames)
	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["customer_id,driver_id"].indices, group)
	}

	// Group2
	assert.Equal(t, []string{"featureE"},
		refGroups["driver_id"].featureNames)
	for _, group := range [][]int{{0, 1}, {2, 3, 4}} {
		assert.Contains(t, refGroups["driver_id"].indices, group)
	}

	// Group3
	assert.Equal(t, []string{"featureF"},
		refGroups["customer_id"].featureNames)

	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["customer_id"].indices, group)
	}

}

func TestGroupingFeatureRefsWithJoinKeyAliases(t *testing.T) {
	viewA := &FeatureView{
		Base: &BaseFeatureView{
			Name: "viewA",
			Projection: &FeatureViewProjection{
				Name:       "viewA",
				JoinKeyMap: map[string]string{"location_id": "destination_id"},
			},
		},
		Entities: map[string]struct{}{"location": {}},
	}
	viewB := &FeatureView{
		Base:     &BaseFeatureView{Name: "viewB"},
		Entities: map[string]struct{}{"location": {}},
	}

	refGroups, _ := groupFeatureRefs(
		[]*featureViewAndRefs{
			{view: viewA, featureRefs: []string{"featureA", "featureB"}},
			{view: viewB, featureRefs: []string{"featureC", "featureD"}},
		},
		map[string]*types.RepeatedValue{
			"location_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"destination_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"location": "location_id",
		},
		true,
	)

	assert.Len(t, refGroups, 2)

	assert.Equal(t, []string{"featureA", "featureB"},
		refGroups["location_id[destination_id]"].featureNames)
	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["location_id[destination_id]"].indices, group)
	}

	assert.Equal(t, []string{"featureC", "featureD"},
		refGroups["location_id"].featureNames)
	for _, group := range [][]int{{0, 1}, {2, 3, 4}} {
		assert.Contains(t, refGroups["location_id"].indices, group)
	}

}

func TestGroupingFeatureRefsWithMissingKey(t *testing.T) {
	viewA := &FeatureView{
		Base: &BaseFeatureView{
			Name: "viewA",
			Projection: &FeatureViewProjection{
				Name:       "viewA",
				JoinKeyMap: map[string]string{"location_id": "destination_id"},
			},
		},
		Entities: map[string]struct{}{"location": {}},
	}

	_, err := groupFeatureRefs(
		[]*featureViewAndRefs{
			{view: viewA, featureRefs: []string{"featureA", "featureB"}},
		},
		map[string]*types.RepeatedValue{
			"location_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
			}},
		},
		map[string]string{
			"location": "location_id",
		},
		true,
	)
	assert.Errorf(t, err, "key destination_id is missing in provided entity rows")
}
