package logging

import (
	"errors"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/feast-dev/feast/go/internal/feast/registry"
)

type OfflineLogStoreConfig struct {
	storeType string
	project   string
	path      string
}

type OfflineLogStorage interface {
	// Todo: Maybe we can add a must implement function that retrieves the correct config based on type
	FlushToStorage(array.Table) error
}

func getOfflineStoreType(offlineStoreConfig map[string]interface{}) (string, bool) {
	if onlineStoreType, ok := offlineStoreConfig["type"]; !ok {
		// Assume file for case of no specified.
		return "file", true
	} else {
		result, ok := onlineStoreType.(string)
		return result, ok
	}
}

func NewOfflineStore(config *registry.RepoConfig) (OfflineLogStorage, error) {
	onlineStoreType, _ := getOfflineStoreType(config.OfflineStore)
	if onlineStoreType == "file" {
		fileConfig, err := GetFileConfig(config)
		if err != nil {
			return nil, err
		}
		offlineStore, err := NewFileOfflineStore(config.Project, fileConfig)
		return offlineStore, err
	} else {
		return nil, errors.New("no offline storage besides file is currently supported")
	}
}
