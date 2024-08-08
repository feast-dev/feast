package registry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/stretchr/testify/assert"
)

func TestNewRepoConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", "feature_repo_*")
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, os.RemoveAll(dir))
	}()
	filePath := filepath.Join(dir, "feature_store.yaml")
	data := []byte(`
project: feature_repo
registry: "data/registry.db"
provider: local
online_store:
 type: redis
 connection_string: "localhost:6379"
`)
	err = os.WriteFile(filePath, data, 0666)
	assert.Nil(t, err)
	config, err := NewRepoConfigFromFile(dir)
	registryConfig, err := config.GetRegistryConfig()
	assert.Nil(t, err)
	assert.Equal(t, "feature_repo", config.Project)
	assert.Equal(t, dir, config.RepoPath)
	assert.Equal(t, "data/registry.db", registryConfig.Path)
	assert.Equal(t, "local", config.Provider)
	assert.Equal(t, map[string]interface{}{
		"type":              "redis",
		"connection_string": "localhost:6379",
	}, config.OnlineStore)
	assert.Empty(t, config.OfflineStore)
	assert.Empty(t, config.FeatureServer)
	assert.Empty(t, config.Flags)
}

func TestNewRepoConfigWithEnvironmentVariables(t *testing.T) {
	dir, err := os.MkdirTemp("", "feature_repo_*")
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, os.RemoveAll(dir))
	}()
	filePath := filepath.Join(dir, "feature_store.yaml")
	data := []byte(`
project: feature_repo
registry: "data/registry.db"
provider: local
online_store:
 type: redis
 connection_string: ${REDIS_CONNECTION_STRING}
`)
	err = os.WriteFile(filePath, data, 0666)
	assert.Nil(t, err)
	os.Setenv("REDIS_CONNECTION_STRING", "localhost:6380")
	config, err := NewRepoConfigFromFile(dir)
	registryConfig, err := config.GetRegistryConfig()
	assert.Nil(t, err)
	assert.Equal(t, "feature_repo", config.Project)
	assert.Equal(t, dir, config.RepoPath)
	assert.Equal(t, "data/registry.db", registryConfig.Path)
	assert.Equal(t, "local", config.Provider)
	assert.Equal(t, map[string]interface{}{
		"type":              "redis",
		"connection_string": "localhost:6380",
	}, config.OnlineStore)
	assert.Empty(t, config.OfflineStore)
	assert.Empty(t, config.FeatureServer)
	assert.Empty(t, config.Flags)
}

func TestNewRepoConfigRegistryMap(t *testing.T) {
	dir, err := os.MkdirTemp("", "feature_repo_*")
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, os.RemoveAll(dir))
	}()
	filePath := filepath.Join(dir, "feature_store.yaml")
	data := []byte(`
registry:
 path: data/registry.db
 client_id: "test_client_id"
project: feature_repo
provider: local
online_store:
 type: redis
 connection_string: "localhost:6379"
`)
	err = os.WriteFile(filePath, data, 0666)
	assert.Nil(t, err)
	config, err := NewRepoConfigFromFile(dir)
	registryConfig, err := config.GetRegistryConfig()
	assert.Nil(t, err)
	assert.Equal(t, "feature_repo", config.Project)
	assert.Equal(t, dir, config.RepoPath)
	assert.Equal(t, "data/registry.db", registryConfig.Path)
	assert.Equal(t, "test_client_id", registryConfig.ClientId)
	assert.Equal(t, "local", config.Provider)
	assert.Equal(t, map[string]interface{}{
		"type":              "redis",
		"connection_string": "localhost:6379",
	}, config.OnlineStore)
	assert.Empty(t, config.OfflineStore)
	assert.Empty(t, config.FeatureServer)
	assert.Empty(t, config.Flags)
}

func TestNewRepoConfigRegistryConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", "feature_repo_*")
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, os.RemoveAll(dir))
	}()
	filePath := filepath.Join(dir, "feature_store.yaml")
	data := []byte(`
registry:
 path: data/registry.db
 client_id: "test_client_id"
project: feature_repo
provider: local
online_store:
 type: redis
 connection_string: "localhost:6379"
`)
	err = os.WriteFile(filePath, data, 0666)
	assert.Nil(t, err)
	config, err := NewRepoConfigFromFile(dir)
	registryConfig, err := config.GetRegistryConfig()
	assert.Nil(t, err)
	assert.Equal(t, dir, config.RepoPath)
	assert.Equal(t, "data/registry.db", registryConfig.Path)
	assert.Equal(t, "test_client_id", registryConfig.ClientId)
}
func TestNewRepoConfigFromJSON(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "feature_repo_*")
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, os.RemoveAll(dir))
	}()

	// Define a JSON string for the test
	registry_path := filepath.Join(dir, "data/registry.db")

	configJSON := `{
        "project": "feature_repo",
        "registry": "$REGISTRY_PATH",
        "provider": "local",
        "online_store": {
            "type": "redis",
            "connection_string": "localhost:6379"
        }
    }`

	replacements := map[string]string{
		"$REGISTRY_PATH": registry_path,
	}

	// Replace the variables in the JSON string
	for variable, replacement := range replacements {
		configJSON = strings.ReplaceAll(configJSON, variable, replacement)
	}

	// Call the function under test
	config, err := NewRepoConfigFromJSON(dir, configJSON)
	registryConfig, err := config.GetRegistryConfig()
	// Assert that there was no error and that the config was correctly parsed
	assert.Nil(t, err)
	assert.Equal(t, "feature_repo", config.Project)
	assert.Equal(t, filepath.Join(dir, "data/registry.db"), registryConfig.Path)
	assert.Equal(t, "local", config.Provider)
	assert.Equal(t, map[string]interface{}{
		"type":              "redis",
		"connection_string": "localhost:6379",
	}, config.OnlineStore)
	assert.Empty(t, config.OfflineStore)
	assert.Empty(t, config.FeatureServer)
	assert.Empty(t, config.Flags)
}

func TestGetRegistryConfig_Map(t *testing.T) {
	// Create a RepoConfig with a map Registry
	config := &RepoConfig{
		Registry: map[string]interface{}{
			"path":                "data/registry.db",
			"registry_store_type": "local",
			"client_id":           "test_client_id",
			"cache_ttl_seconds":   60,
		},
	}

	// Call the method under test
	registryConfig, _ := config.GetRegistryConfig()

	// Assert that the method correctly processed the map
	assert.Equal(t, "data/registry.db", registryConfig.Path)
	assert.Equal(t, "local", registryConfig.RegistryStoreType)
	assert.Equal(t, int64(60), registryConfig.CacheTtlSeconds)
	assert.Equal(t, "test_client_id", registryConfig.ClientId)
}

func TestGetRegistryConfig_String(t *testing.T) {
	// Create a RepoConfig with a string Registry
	config := &RepoConfig{
		Registry: "data/registry.db",
	}

	// Call the method under test
	registryConfig, _ := config.GetRegistryConfig()

	// Assert that the method correctly processed the string
	assert.Equal(t, "data/registry.db", registryConfig.Path)
	assert.Equal(t, defaultClientID, registryConfig.ClientId)
	println(registryConfig.CacheTtlSeconds)
	assert.Empty(t, registryConfig.RegistryStoreType)
	assert.Equal(t, defaultCacheTtlSeconds, registryConfig.CacheTtlSeconds)
}

func TestGetRegistryConfig_CacheTtlSecondsTypes(t *testing.T) {
	// Create RepoConfigs with different types for cache_ttl_seconds
	configs := []*RepoConfig{
		{
			Registry: map[string]interface{}{
				"cache_ttl_seconds": float64(60),
			},
		},
		{
			Registry: map[string]interface{}{
				"cache_ttl_seconds": int32(60),
			},
		},
		{
			Registry: map[string]interface{}{
				"cache_ttl_seconds": int64(60),
			},
		},
	}

	for _, config := range configs {
		// Call the method under test
		registryConfig, _ := config.GetRegistryConfig()

		// Assert that the method correctly processed cache_ttl_seconds
		assert.Equal(t, int64(60), registryConfig.CacheTtlSeconds)
	}
}

func TestGetLoggingOptions_Defaults(t *testing.T) {
	config := RepoConfig{
		FeatureServer: map[string]interface{}{
			"feature_logging": map[string]interface{}{},
		},
	}
	options, err := config.GetLoggingOptions()
	assert.Nil(t, err)
	assert.Equal(t, logging.DefaultOptions, *options)
}

func TestGetLoggingOptions_QueueCapacity(t *testing.T) {
	config := RepoConfig{
		FeatureServer: map[string]interface{}{
			"feature_logging": map[string]interface{}{
				"queue_capacity": 100,
			},
		},
	}
	expected := logging.DefaultOptions
	expected.ChannelCapacity = 100
	options, err := config.GetLoggingOptions()
	assert.Nil(t, err)
	assert.Equal(t, expected, *options)
}

func TestGetLoggingOptions_EmitTimeoutMicroSecs(t *testing.T) {
	config := RepoConfig{
		FeatureServer: map[string]interface{}{
			"feature_logging": map[string]interface{}{
				"emit_timeout_micro_secs": 500,
			},
		},
	}
	expected := logging.DefaultOptions
	expected.EmitTimeout = 500 * time.Microsecond
	options, err := config.GetLoggingOptions()
	assert.Nil(t, err)
	assert.Equal(t, expected, *options)
}

func TestGetLoggingOptions_WriteToDiskIntervalSecs(t *testing.T) {
	config := RepoConfig{
		FeatureServer: map[string]interface{}{
			"feature_logging": map[string]interface{}{
				"write_to_disk_interval_secs": 10,
			},
		},
	}
	expected := logging.DefaultOptions
	expected.WriteInterval = 10 * time.Second
	options, err := config.GetLoggingOptions()
	assert.Nil(t, err)
	assert.Equal(t, expected, *options)
}

func TestGetLoggingOptions_FlushIntervalSecs(t *testing.T) {
	config := RepoConfig{
		FeatureServer: map[string]interface{}{
			"feature_logging": map[string]interface{}{
				"flush_interval_secs": 15,
			},
		},
	}
	expected := logging.DefaultOptions
	expected.FlushInterval = 15 * time.Second
	options, err := config.GetLoggingOptions()
	assert.Nil(t, err)
	assert.Equal(t, expected, *options)
}

func TestGetLoggingOptions_InvalidType(t *testing.T) {
	config := RepoConfig{
		FeatureServer: map[string]interface{}{
			"feature_logging": map[string]interface{}{
				"queue_capacity": "invalid",
			},
		},
	}
	options, err := config.GetLoggingOptions()
	assert.Nil(t, err)
	assert.Equal(t, logging.DefaultOptions, *options)
}
