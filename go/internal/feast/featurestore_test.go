package feast

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/types"
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

	t.Run("valid config", func(t *testing.T) {
		config := &registry.RepoConfig{
			Project:  "feature_repo",
			Registry: getRegistryPath(),
			Provider: "local",
			OnlineStore: map[string]interface{}{
				"type": "redis",
			},
			FeatureServer: map[string]interface{}{
				"transformation_service_endpoint": "localhost:50051",
			},
		}
		fs, err := NewFeatureStore(config, nil)
		assert.Nil(t, err)
		assert.NotNil(t, fs)
		assert.IsType(t, &onlinestore.RedisOnlineStore{}, fs.onlineStore)
		assert.NotNil(t, fs.transformationService)
	})

	t.Run("missing transformation service endpoint", func(t *testing.T) {
		config := &registry.RepoConfig{
			Project:  "feature_repo",
			Registry: getRegistryPath(),
			Provider: "local",
			OnlineStore: map[string]interface{}{
				"type": "redis",
			},
		}
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
		}()
		NewFeatureStore(config, nil)
	})

	t.Run("invalid online store config", func(t *testing.T) {
		config := &registry.RepoConfig{
			Project:  "feature_repo",
			Registry: getRegistryPath(),
			Provider: "local",
			OnlineStore: map[string]interface{}{
				"type": "invalid_store",
			},
			FeatureServer: map[string]interface{}{
				"transformation_service_endpoint": "localhost:50051",
			},
		}
		fs, err := NewFeatureStore(config, nil)
		assert.NotNil(t, err)
		assert.Nil(t, fs)
	})
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
