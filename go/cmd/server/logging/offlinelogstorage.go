package logging

import (
	"errors"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/feast-dev/feast/go/internal/feast"
)

type OfflineLogStorage interface {
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

func NewOfflineStore(config *feast.RepoConfig) (OfflineLogStorage, error) {
	onlineStoreType, _ := getOfflineStoreType(config.OfflineStore)
	if onlineStoreType == "file" {
		offlineStore, err := NewFileOfflineStore(config.Project, config.OfflineStore)
		return offlineStore, err
	} else {
		return nil, errors.New("no offline storage besides file is currently supported")
	}
}
