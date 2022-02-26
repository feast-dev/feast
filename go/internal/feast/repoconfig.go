package feast

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
}

// NewRepoConfig reads file <repoPath>/feature_store.yaml
// and converts the YAML format into RepoConfig struct.
// It first uses repoPath to read feature_store.yaml if it exists,
// however if it doesn't then it checks configJSON and tries parsing that.
func NewRepoConfigFromJson(repoPath, configJSON string) (*RepoConfig, error) {
	config := RepoConfig{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, err
	}

	if err := config.setParsedConfigRegistryAbsolutePath(repoPath); err != nil {
		return nil, err
	}

	return &config, nil
}

func NewRepoConfigFromFile(repoPath string) (*RepoConfig, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, "feature_store.yaml"))
	config := RepoConfig{}
	if err != nil {
		return nil, errors.New(fmt.Sprintf("FEAST_REPO_PATH: %s not found", repoPath))
	} else {
		if err = yaml.Unmarshal(data, &config); err != nil {
			return nil, err
		}
	}

	if err := config.setParsedConfigRegistryAbsolutePath(repoPath); err != nil {
		return nil, err
	}

	return &config, nil
}

func (r *RepoConfig) setParsedConfigRegistryAbsolutePath(repoPath string) error {
	if path, ok := r.Registry.(string); ok {
		if !filepath.IsAbs(path) {
			r.Registry = filepath.Join(repoPath, path)
		}
	} else {
		if registryInterface, ok := r.Registry.(map[string]interface{}); !ok {
			return errors.New("Registry must be either a string or a map")
		} else {
			if pathInterface, ok := registryInterface["path"]; ok {
				if path, ok := pathInterface.(string); ok {
					if !filepath.IsAbs(path) {
						r.Registry.(map[string]interface{})["path"] = filepath.Join(repoPath, path)
					}
				} else {
					return errors.New("Registry path must be a string")
				}

			} else {
				return errors.New("Registry path not found")
			}
		}
	}
	return nil
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
