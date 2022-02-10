package feast

import (
	"encoding/json"
	"github.com/ghodss/yaml"
	"os"
	"path/filepath"
)

type RepoConfig struct {
	// Path to the registry. Custom registry loaders are not yet supported
	Registry string `json:"registry"`
	// Feast project name
	Project string `json:"project"`
	// Feast provider name
	Provider string `json:"provider"`
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
	// If the registry points to a relative path, convert it into absolute path
	// because otherwise RepoConfig struct doesn't contain enough information
	// about how to locate the registry
	if !filepath.IsAbs(config.Registry) {
		config.Registry = filepath.Join(repoPath, config.Registry)
	}
	return &config, nil
}
