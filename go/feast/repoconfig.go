package feast

import (
	"encoding/json"
	"github.com/ghodss/yaml"
	"os"
	"path/filepath"
	"errors"
	"log"
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

// type RepoConfigInterfaceRegistry struct {
// 	// Feast project name
// 	Project string `json:"project"`
// 	// Feast provider name
// 	Provider string `json:"provider"`
// 	// Path to the registry. Custom registry loaders are not yet supported
// 	// Registry string `json:"registry"`
// 	Registry interface{} `json:"registry"`
// 	// Online store config
// 	OnlineStore map[string]interface{} `json:"online_store"`
// 	// Offline store config
// 	OfflineStore map[string]interface{} `json:"offline_store"`
// 	// Feature server config (currently unrelated to Go server)
// 	FeatureServer map[string]interface{} `json:"feature_server"`
// 	// Feature flags for experimental features
// 	Flags map[string]interface{} `json:"flags"`
// }

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

	log.Println(config.Registry)
	// for k,v := range config.Registry {
	// 	log.Println(k, v)
	// }
	
		
	// }
	// repoConfig := RepoConfig{
	// 	Project: config.Project,
	// 	Provider: config.Provider,
	// 	//Skip Registry
	// 	OnlineStore: config.OnlineStore,
	// 	OfflineStore: config.OfflineStore,
	// 	FeatureServer: config.FeatureServer,
	// 	Flags: config.Flags,
	// }

	if path, ok := config.Registry.(string); ok {
		// repoConfig.Registry = make(map[string]interface{})
		// if !filepath.IsAbs(path) {
		// 	repoConfig.Registry["path"] = filepath.Join(repoPath, path)
		// } else {
		// 	repoConfig.Registry["path"] = path
		// }
		if !filepath.IsAbs(path) {
			config.Registry = filepath.Join(repoPath, path)
		}
	} else {
		if registryInterface, ok := config.Registry.(map[string]interface{}); !ok {
			return nil, errors.New("Registry must be either a string or a map")
		} else {
			// repoConfig.Registry = registryInterface
			if pathInterface, ok := registryInterface["path"]; ok {
				if path, ok := pathInterface.(string); ok {
					if !filepath.IsAbs(path) {
						config.Registry.(map[string]interface{})["path"] = filepath.Join(repoPath, path)
						// registryInterface["path"] = filepath.Join(repoPath, path)
					}
				} else {
					return nil, errors.New("Registry path must be a string")
				}
				
			} else {
				return nil, errors.New("Registry path not found")
			}
		}
	}

	log.Println("parsed registry config successfully")
	return &config, nil
}

func (r *RepoConfig) getRegistryPath() string {
	log.Println(r.Registry)
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
	// return r.Registry["path"].(string)
	return ""
}
