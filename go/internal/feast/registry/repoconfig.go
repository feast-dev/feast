package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/ghodss/yaml"
)

const (
	defaultCacheTtlSeconds             = int64(600)
	defaultClientID                    = "Unknown"
	defaultMySQLMaxOpenConns           = 20
	defaultMySQLMaxIdleConns           = 10
	defaultMySQLConnMaxLifetimeSeconds = int64(300)
	defaultMySQLQueryTimeoutSeconds    = int64(30)
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
	// EntityKeySerializationVersion
	EntityKeySerializationVersion int64 `json:"entity_key_serialization_version"`
}

type RegistryConfig struct {
	RegistryStoreType           string `json:"registry_store_type"`
	Path                        string `json:"path"`
	ClientId                    string `json:"client_id" default:"Unknown"`
	CacheTtlSeconds             int64  `json:"cache_ttl_seconds" default:"600"`
	MySQLMaxOpenConns           int    `json:"mysql_max_open_conns"`
	MySQLMaxIdleConns           int    `json:"mysql_max_idle_conns"`
	MySQLConnMaxLifetimeSeconds int64  `json:"mysql_conn_max_lifetime_seconds"`
	MySQLQueryTimeoutSeconds    int64  `json:"mysql_query_timeout_seconds"`
}

// NewRepoConfigFromJSON converts a JSON string into a RepoConfig struct and also sets the repo path.
func NewRepoConfigFromJSON(repoPath, configJSON string) (*RepoConfig, error) {
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

// NewRepoConfigFromFile reads the `feature_store.yaml` file in the repo path and converts it
// into a RepoConfig struct.
func NewRepoConfigFromFile(repoPath string) (*RepoConfig, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, "feature_store.yaml"))
	if err != nil {
		return nil, err
	}
	repoPath, err = filepath.Abs(repoPath)
	if err != nil {
		return nil, err
	}

	repoConfigWithEnv := os.ExpandEnv(string(data))

	config := RepoConfig{}
	if err = yaml.Unmarshal([]byte(repoConfigWithEnv), &config); err != nil {
		return nil, err
	}
	config.RepoPath = repoPath
	return &config, nil
}

func (r *RepoConfig) GetLoggingOptions() (*logging.LoggingOptions, error) {
	loggingOptions := logging.LoggingOptions{}
	if loggingOptionsMap, ok := r.FeatureServer["feature_logging"].(map[string]interface{}); ok {
		loggingOptions = logging.DefaultOptions
		for k, v := range loggingOptionsMap {
			switch k {
			case "queue_capacity":
				if value, ok := v.(int); ok {
					loggingOptions.ChannelCapacity = value
				}
			case "emit_timeout_micro_secs":
				if value, ok := v.(int); ok {
					loggingOptions.EmitTimeout = time.Duration(value) * time.Microsecond
				}
			case "write_to_disk_interval_secs":
				if value, ok := v.(int); ok {
					loggingOptions.WriteInterval = time.Duration(value) * time.Second
				}
			case "flush_interval_secs":
				if value, ok := v.(int); ok {
					loggingOptions.FlushInterval = time.Duration(value) * time.Second
				}
			}
		}
	}
	return &loggingOptions, nil
}

func (r *RepoConfig) GetRegistryConfig() (*RegistryConfig, error) {
	if registryConfigMap, ok := r.Registry.(map[string]interface{}); ok {
		registryConfig := RegistryConfig{
			CacheTtlSeconds:             defaultCacheTtlSeconds,
			ClientId:                    defaultClientID,
			MySQLMaxOpenConns:           defaultMySQLMaxOpenConns,
			MySQLMaxIdleConns:           defaultMySQLMaxIdleConns,
			MySQLConnMaxLifetimeSeconds: defaultMySQLConnMaxLifetimeSeconds,
			MySQLQueryTimeoutSeconds:    defaultMySQLQueryTimeoutSeconds,
		}
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
			case "client_id":
				if value, ok := v.(string); ok {
					registryConfig.ClientId = value
				}
			case "cache_ttl_seconds":
				parsed, err := parseInt64Field("cache_ttl_seconds", v)
				if err != nil {
					return nil, err
				}
				registryConfig.CacheTtlSeconds = parsed
			case "mysql_max_open_conns":
				parsed, err := parseIntField("mysql_max_open_conns", v)
				if err != nil {
					return nil, err
				}
				registryConfig.MySQLMaxOpenConns = parsed
			case "mysql_max_idle_conns":
				parsed, err := parseIntField("mysql_max_idle_conns", v)
				if err != nil {
					return nil, err
				}
				registryConfig.MySQLMaxIdleConns = parsed
			case "mysql_conn_max_lifetime_seconds":
				parsed, err := parseInt64Field("mysql_conn_max_lifetime_seconds", v)
				if err != nil {
					return nil, err
				}
				registryConfig.MySQLConnMaxLifetimeSeconds = parsed
			case "mysql_query_timeout_seconds":
				parsed, err := parseInt64Field("mysql_query_timeout_seconds", v)
				if err != nil {
					return nil, err
				}
				registryConfig.MySQLQueryTimeoutSeconds = parsed
			}
		}
		return &registryConfig, nil
	} else {
		return &RegistryConfig{
			Path:                        r.Registry.(string),
			ClientId:                    defaultClientID,
			CacheTtlSeconds:             defaultCacheTtlSeconds,
			MySQLMaxOpenConns:           defaultMySQLMaxOpenConns,
			MySQLMaxIdleConns:           defaultMySQLMaxIdleConns,
			MySQLConnMaxLifetimeSeconds: defaultMySQLConnMaxLifetimeSeconds,
			MySQLQueryTimeoutSeconds:    defaultMySQLQueryTimeoutSeconds,
		}, nil
	}
}

func parseInt64Field(field string, value interface{}) (int64, error) {
	switch parsed := value.(type) {
	case float64:
		return int64(parsed), nil
	case int:
		return int64(parsed), nil
	case int32:
		return int64(parsed), nil
	case int64:
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected type %T for %s", value, field)
	}
}

func parseIntField(field string, value interface{}) (int, error) {
	parsed, err := parseInt64Field(field, value)
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}
