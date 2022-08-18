package onlinestore

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/registry"

	"github.com/stretchr/testify/assert"

	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func TestSqliteAndFeatureRepoSetup(t *testing.T) {
	dir := t.TempDir()
	feature_repo_path := filepath.Join(dir, "my_project", "feature_repo")

	err := test.SetupCleanFeatureRepo(dir)
	assert.Nil(t, err)
	config, err := registry.NewRepoConfigFromFile(feature_repo_path)
	assert.Nil(t, err)
	assert.Equal(t, "my_project", config.Project)
	assert.Equal(t, "data/registry.db", config.GetRegistryConfig().Path)
	assert.Equal(t, "local", config.Provider)
	assert.Equal(t, map[string]interface{}{
		"path": "data/online_store.db",
		"type": "sqlite",
	}, config.OnlineStore)
	assert.Empty(t, config.OfflineStore)
	assert.Empty(t, config.FeatureServer)
	assert.Empty(t, config.Flags)
}

func TestSqliteOnlineRead(t *testing.T) {
	dir := t.TempDir()
	feature_repo_path := filepath.Join(dir, "my_project", "feature_repo")
	test.SetupCleanFeatureRepo(dir)
	config, err := registry.NewRepoConfigFromFile(feature_repo_path)
	assert.Nil(t, err)

	store, err := NewSqliteOnlineStore("my_project", config, config.OnlineStore)
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
			returnedFeatureValues = append(returnedFeatureValues, &featureVector[idx].Value)
			returnedFeatureNames = append(returnedFeatureNames, featureVector[idx].Reference.FeatureName)
		}
	}
	rows, err := test.ReadParquet(filepath.Join(feature_repo_path, "data", "driver_stats.parquet"))
	assert.Nil(t, err)
	entities := map[int64]bool{1005: true, 1001: true, 1003: true}
	correctFeatures := test.GetLatestFeatures(rows, entities)
	expectedFeatureValues := make([]*types.Value, 0)
	for _, key := range []int64{1005, 1001, 1003} {
		expectedFeatureValues = append(expectedFeatureValues, &types.Value{Val: &types.Value_FloatVal{FloatVal: correctFeatures[key].ConvRate}})
		expectedFeatureValues = append(expectedFeatureValues, &types.Value{Val: &types.Value_FloatVal{FloatVal: correctFeatures[key].AccRate}})
		expectedFeatureValues = append(expectedFeatureValues, &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(correctFeatures[key].AvgDailyTrips)}})
	}
	expectedFeatureNames := []string{"conv_rate", "acc_rate", "avg_daily_trips", "conv_rate", "acc_rate", "avg_daily_trips", "conv_rate", "acc_rate", "avg_daily_trips"}
	assert.True(t, reflect.DeepEqual(expectedFeatureValues, returnedFeatureValues))
	assert.True(t, reflect.DeepEqual(expectedFeatureNames, returnedFeatureNames))
}
