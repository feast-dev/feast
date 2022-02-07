package feast

import (
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
// and converts the YAML format into RepoConfig struct
func NewRepoConfig(repoPath string) (*RepoConfig, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, "feature_store.yaml"))
	if err != nil {
		return nil, err
	}
	config := RepoConfig{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
