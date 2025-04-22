package onlinestore

import (
	"os"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/types"

	"github.com/stretchr/testify/assert"
)

func TestBuildValkeyFeatureViewIndices(t *testing.T) {
	r := &ValkeyOnlineStore{}

	t.Run("test with empty featureViewNames and featureNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{}, []string{})
		assert.Equal(t, 0, len(featureViewIndices))
		assert.Equal(t, 0, len(indicesFeatureView))
		assert.Equal(t, 0, index)
	})

	t.Run("test with non-empty featureNames and empty featureViewNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{}, []string{"feature1", "feature2"})
		assert.Equal(t, 0, len(featureViewIndices))
		assert.Equal(t, 0, len(indicesFeatureView))
		assert.Equal(t, 2, index)
	})

	t.Run("test with non-empty featureViewNames and featureNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{"view1", "view2"}, []string{"feature1", "feature2"})
		assert.Equal(t, 2, len(featureViewIndices))
		assert.Equal(t, 2, len(indicesFeatureView))
		assert.Equal(t, 4, index)
		assert.Equal(t, "view1", indicesFeatureView[2])
		assert.Equal(t, "view2", indicesFeatureView[3])
	})

	t.Run("test with duplicate featureViewNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{"view1", "view1"}, []string{"feature1", "feature2"})
		assert.Equal(t, 1, len(featureViewIndices))
		assert.Equal(t, 1, len(indicesFeatureView))
		assert.Equal(t, 3, index)
		assert.Equal(t, "view1", indicesFeatureView[2])
	})
}

func TestBuildValkeyHsetKeys(t *testing.T) {
	r := &ValkeyOnlineStore{}

	t.Run("test with empty featureViewNames and featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildHsetKeys([]string{}, []string{}, map[int]string{}, 0)
		assert.Equal(t, 0, len(hsetKeys))
		assert.Equal(t, 0, len(featureNames))
	})

	t.Run("test with non-empty featureViewNames and featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildHsetKeys([]string{"view1", "view2"}, []string{"feature1", "feature2"}, map[int]string{2: "view1", 3: "view2"}, 4)
		assert.Equal(t, 4, len(hsetKeys))
		assert.Equal(t, 4, len(featureNames))
		assert.Equal(t, "_ts:view1", hsetKeys[2])
		assert.Equal(t, "_ts:view2", hsetKeys[3])
		assert.Contains(t, featureNames, "_ts:view1")
		assert.Contains(t, featureNames, "_ts:view2")
	})

	t.Run("test with more featureViewNames than featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildHsetKeys([]string{"view1", "view2", "view3"}, []string{"feature1", "feature2", "feature3"}, map[int]string{3: "view1", 4: "view2", 5: "view3"}, 6)
		assert.Equal(t, 6, len(hsetKeys))
		assert.Equal(t, 6, len(featureNames))
		assert.Equal(t, "_ts:view1", hsetKeys[3])
		assert.Equal(t, "_ts:view2", hsetKeys[4])
		assert.Equal(t, "_ts:view3", hsetKeys[5])
		assert.Contains(t, featureNames, "_ts:view1")
		assert.Contains(t, featureNames, "_ts:view2")
		assert.Contains(t, featureNames, "_ts:view3")
	})
}

func TestBuildValkeyKeys(t *testing.T) {
	r := &ValkeyOnlineStore{
		project: "test_project",
		config: &registry.RepoConfig{
			EntityKeySerializationVersion: 2,
		},
	}

	entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}

	entity_key2 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}},
	}

	error_entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id", "vehicle_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}

	t.Run("test with empty entityKeys", func(t *testing.T) {
		valkeyKeys, err := r.buildValkeyKeys([]*types.EntityKey{})
		assert.Nil(t, err)
		assert.Equal(t, 0, len(valkeyKeys))
	})

	t.Run("test with single entityKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&entity_key1}
		valkeyKeys, err := r.buildValkeyKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(valkeyKeys))
	})

	t.Run("test with multiple entityKeys", func(t *testing.T) {
		entityKeys := []*types.EntityKey{
			&entity_key1, &entity_key2,
		}
		valkeyKeys, err := r.buildValkeyKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(valkeyKeys))
	})

	t.Run("test with error in buildValkeyKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&error_entity_key1}
		_, err := r.buildValkeyKeys(entityKeys)
		assert.NotNil(t, err)
	})
}
func TestParseConnectionString(t *testing.T) {
	t.Run("Default connection string", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{}
		clientOption, err := parseConnectionString(onlineStoreConfig)
		assert.NoError(t, err)
		assert.Equal(t, []string{"localhost:6379"}, clientOption.InitAddress)
	})

	t.Run("Valid connection string with multiple addresses", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": "127.0.0.1:6379,192.168.1.1:6380",
		}
		clientOption, err := parseConnectionString(onlineStoreConfig)
		assert.NoError(t, err)
		assert.Equal(t, []string{"127.0.0.1:6379", "192.168.1.1:6380"}, clientOption.InitAddress)
	})

	t.Run("Connection string with password", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": "127.0.0.1:6379,password=secret",
		}
		clientOption, err := parseConnectionString(onlineStoreConfig)
		assert.NoError(t, err)
		assert.Equal(t, "secret", clientOption.Password)
	})

	t.Run("Connection string with SSL enabled", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": "127.0.0.1:6379,ssl=true",
		}
		clientOption, err := parseConnectionString(onlineStoreConfig)
		assert.NoError(t, err)
		assert.NotNil(t, clientOption.TLSConfig)
	})

	t.Run("Connection string with database selection", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": "127.0.0.1:6379,db=1",
		}
		clientOption, err := parseConnectionString(onlineStoreConfig)
		assert.NoError(t, err)
		assert.Equal(t, 1, clientOption.SelectDB)
	})

	t.Run("Invalid connection string format", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": "127.0.0.1:6379,invalid_option",
		}
		_, err := parseConnectionString(onlineStoreConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to parse part of connection_string")
	})

	t.Run("Unrecognized option in connection string", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": "127.0.0.1:6379,unknown=option",
		}
		_, err := parseConnectionString(onlineStoreConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unrecognized option in connection_string")
	})

	t.Run("Invalid SSL value", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": "127.0.0.1:6379,ssl=invalid",
		}
		_, err := parseConnectionString(onlineStoreConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid syntax")
	})

	t.Run("Invalid database value", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": "127.0.0.1:6379,db=invalid",
		}
		_, err := parseConnectionString(onlineStoreConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid syntax")
	})

	t.Run("Invalid connection_string type", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"connection_string": 12345,
		}
		_, err := parseConnectionString(onlineStoreConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to convert connection_string to string")
	})
}
func TestGetValkeyType(t *testing.T) {
	t.Run("Default valkey type", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{}
		valkeyType, err := getValkeyType(onlineStoreConfig)
		assert.NoError(t, err)
		assert.Equal(t, valkeyNode, valkeyType)
	})

	t.Run("Valid valkey type: valkey", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"valkey_type": "valkey",
		}
		valkeyType, err := getValkeyType(onlineStoreConfig)
		assert.NoError(t, err)
		assert.Equal(t, valkeyNode, valkeyType)
	})

	t.Run("Valid valkey type: valkey_cluster", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"valkey_type": "valkey_cluster",
		}
		valkeyType, err := getValkeyType(onlineStoreConfig)
		assert.NoError(t, err)
		assert.Equal(t, valkeyCluster, valkeyType)
	})

	t.Run("Invalid valkey type string", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"valkey_type": "invalid_type",
		}
		_, err := getValkeyType(onlineStoreConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to convert valkey_type to enum")
	})

	t.Run("Invalid valkey type format", func(t *testing.T) {
		onlineStoreConfig := map[string]interface{}{
			"valkey_type": 12345,
		}
		_, err := getValkeyType(onlineStoreConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to convert valkey_type to string")
	})
}
func TestGetValkeyTraceServiceName(t *testing.T) {
	t.Run("Default service name when DD_SERVICE is not set", func(t *testing.T) {
		// Clear the DD_SERVICE environment variable
		os.Unsetenv("DD_SERVICE")

		// Call the function
		serviceName := getValkeyTraceServiceName()

		// Assert the default service name
		assert.Equal(t, "valkey.client", serviceName)
	})

	t.Run("Custom service name when DD_SERVICE is set", func(t *testing.T) {
		// Set the DD_SERVICE environment variable
		os.Setenv("DD_SERVICE", "custom-service")

		// Call the function
		serviceName := getValkeyTraceServiceName()

		// Assert the custom service name
		assert.Equal(t, "custom-service-valkey", serviceName)

		// Clean up the environment variable
		os.Unsetenv("DD_SERVICE")
	})

	t.Run("Empty DD_SERVICE results in default service name", func(t *testing.T) {
		// Set the DD_SERVICE environment variable to an empty string
		os.Setenv("DD_SERVICE", "")

		// Call the function
		serviceName := getValkeyTraceServiceName()

		// Assert the default service name
		assert.Equal(t, "valkey.client", serviceName)

		// Clean up the environment variable
		os.Unsetenv("DD_SERVICE")
	})
}
