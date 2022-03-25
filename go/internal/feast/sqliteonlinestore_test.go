package feast

import (
	"context"
	"reflect"
	"testing"

	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
)

func TestSqliteSetup(t *testing.T) {
	dir := "../test/feature_repo"
	config, err := NewRepoConfigFromFile(dir)
	assert.Nil(t, err)
	assert.Equal(t, "feature_repo", config.Project)
	assert.Equal(t, "data/registry.db", config.GetRegistryConfig().Path)
	assert.Equal(t, "local", config.Provider)
	assert.Equal(t, map[string]interface{}{
		"type": "sqlite",
		"path": "data/online_store.db",
	}, config.OnlineStore)
	assert.Empty(t, config.OfflineStore)
	assert.Empty(t, config.FeatureServer)
	assert.Empty(t, config.Flags)
}

func TestSqliteOnlineRead(t *testing.T) {
	dir := "../test/feature_repo"
	config, err := NewRepoConfigFromFile(dir)
	assert.Nil(t, err)
	store, err := NewSqliteOnlineStore("feature_repo", config, config.OnlineStore)
	defer store.Destruct()
	assert.Nil(t, err)
	entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}
	entity_key2 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}},
	}
	entity_key3 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1003}}},
	}
	entityKeys := []*types.EntityKey{&entity_key1, &entity_key2, &entity_key3}
	tableNames := []string{"driver_hourly_stats"}
	featureNames := []string{"conv_rate", "acc_rate", "avg_daily_trips"}
	featureData, err := store.OnlineRead(context.Background(), entityKeys, tableNames, featureNames)
	assert.Nil(t, err)
	returnedFeatureValues := make([]*types.Value, 0)
	returnedFeatureNames := make([]string, 0)
	for _, featureVector := range featureData {
		for idx := range featureVector {
			returnedFeatureValues = append(returnedFeatureValues, &featureVector[idx].value)
			returnedFeatureNames = append(returnedFeatureNames, featureVector[idx].reference.FeatureName)
		}
	}
	expectedFeatureValues := []*types.Value{
		{Val: &types.Value_FloatVal{FloatVal: 0.78135854}},
		{Val: &types.Value_FloatVal{FloatVal: 0.38527268}},
		{Val: &types.Value_Int64Val{Int64Val: 755}},
		{Val: &types.Value_FloatVal{FloatVal: 0.49661186}},
		{Val: &types.Value_FloatVal{FloatVal: 0.9440974}},
		{Val: &types.Value_Int64Val{Int64Val: 169}},
		{Val: &types.Value_FloatVal{FloatVal: 0.80762655}},
		{Val: &types.Value_FloatVal{FloatVal: 0.71510273}},
		{Val: &types.Value_Int64Val{Int64Val: 545}},
	}
	expectedFeatureNames := []string{"conv_rate", "acc_rate", "avg_daily_trips", "conv_rate", "acc_rate", "avg_daily_trips", "conv_rate", "acc_rate", "avg_daily_trips"}
	assert.True(t, reflect.DeepEqual(expectedFeatureValues, returnedFeatureValues))
	assert.True(t, reflect.DeepEqual(expectedFeatureNames, returnedFeatureNames))
}
