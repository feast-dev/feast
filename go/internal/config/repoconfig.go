package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ghodss/yaml"
	"os"
	"path/filepath"
)

type RepoConfig struct {
	// Feast project name
	Project string `json:"project"`
	// Feast provider name
	Provider string `json:"provider"`
	// Path to the registry. Custom registry loaders are not yet supported
	// Registry string `json:"registry"`
	Registry interface{} `json:"registry"`
	// Online store config
	OnlineStore map[string]interface{} `json:"online_store"`
	// Offline store config
	OfflineStore map[string]interface{} `json:"offline_store"`
	// Feature server config (currently unrelated to Go server)
	FeatureServer map[string]interface{} `json:"feature_server"`
	// Feature flags for experimental features
	Flags map[string]interface{} `json:"flags"`
	// RepoPath
	RepoPath string `json:"repo_path"`
}

type RegistryConfig struct {
	RegistryStoreType string `json:"registry_store_type"`
	Path              string `json:"path"`
	CacheTtlSeconds   int64  `json:"cache_ttl_seconds" default:"600"`
}

// NewRepoConfig reads file <repoPath>/feature_store.yaml and converts the YAML format into RepoConfig struct.
// It first uses repoPath to read feature_store.yaml if it exists, however if it doesn't then it checks configJSON and tries parsing that.
func NewRepoConfigFromJson(repoPath, configJSON string) (*RepoConfig, error) {

	config := RepoConfig{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, err
	}
	repoPath, err := filepath.Abs(repoPath)
	if err != nil {
		return nil, err
	}
	config.RepoPath = repoPath
	return &config, nil
}

func NewRepoConfigFromFile(repoPath string) (*RepoConfig, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, "feature_store.yaml"))
	repoPath, err = filepath.Abs(repoPath)
	if err != nil {
		return nil, err
	}

	config := RepoConfig{}
	if err != nil {
		return nil, errors.New(fmt.Sprintf("FEAST_REPO_PATH: %s not found", repoPath))
	} else {
		if err = yaml.Unmarshal(data, &config); err != nil {
			return nil, err
		}
	}

	config.RepoPath = repoPath
	return &config, nil
}

func (r *RepoConfig) GetRegistryConfig() *RegistryConfig {
	if registryConfigMap, ok := r.Registry.(map[string]interface{}); ok {
		// Default CacheTtlSeconds to 600
		registryConfig := RegistryConfig{CacheTtlSeconds: 600}
		for k, v := range registryConfigMap {
			switch k {
			case "path":
				if value, ok := v.(string); ok {
					registryConfig.Path = value
				}
			case "registry_store_type":
				if value, ok := v.(string); ok {
					registryConfig.RegistryStoreType = value
				}
			case "cache_ttl_seconds":
				if value, ok := v.(int64); ok {
					registryConfig.CacheTtlSeconds = value
				}
			}
		}
		return &registryConfig
	} else {
		return &RegistryConfig{Path: r.Registry.(string)}
	}
}
