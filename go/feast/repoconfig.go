package feast

import (
	"encoding/json"
	"github.com/ghodss/yaml"
	"os"
	"path/filepath"
	"errors"
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
}

// NewRepoConfig reads file <repoPath>/feature_store.yaml
// and converts the YAML format into RepoConfig struct.
// It first uses repoPath to read feature_store.yaml if it exists,
// however if it doesn't then it checks configJSON and tries parsing that.
func NewRepoConfig(repoPath string, configJSON string) (*RepoConfig, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, "feature_store.yaml"))
	config := RepoConfig{}
	if err != nil {
		if err = json.Unmarshal([]byte(configJSON), &config); err != nil {
			return nil, err
		}
	} else {
		if err = yaml.Unmarshal(data, &config); err != nil {
			return nil, err
		}
	}

	if path, ok := config.Registry.(string); ok {
		if !filepath.IsAbs(path) {
			config.Registry = filepath.Join(repoPath, path)
		}
	} else {
		if registryInterface, ok := config.Registry.(map[string]interface{}); !ok {
			return nil, errors.New("Registry must be either a string or a map")
		} else {
			if pathInterface, ok := registryInterface["path"]; ok {
				if path, ok := pathInterface.(string); ok {
					if !filepath.IsAbs(path) {
						config.Registry.(map[string]interface{})["path"] = filepath.Join(repoPath, path)
					}
				} else {
					return nil, errors.New("Registry path must be a string")
				}
				
			} else {
				return nil, errors.New("Registry path not found")
			}
		}
	}

	return &config, nil
}

func (r *RepoConfig) getRegistryPath() string {
	if path, ok := r.Registry.(string); ok {
		return path
	} else {
		if registryInterface, ok := r.Registry.(map[string]interface{}); ok {
			if pathInterface, ok := registryInterface["path"]; ok {
				if path, ok := pathInterface.(string); ok {
					return path
				}
				
			}
		}
	}
	return ""
}
