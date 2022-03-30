package feast

// import "github.com/feast-dev/feast/go/cmd/server/logging"

// type OfflineLogStorage interface {
// 	FlushToStorage(logging.MemoryBuffer)
// 	// Destruct must be call once user is done using OnlineStore
// 	// This is to comply with the Connector since we have to close the plugin
// }

// func getOfflineStoreType(offlineStoreConfig map[string]interface{}) (string, bool) {
// 	if onlineStoreType, ok := offlineStoreConfig["type"]; !ok {
// 		// Assume file for case of no specified.
// 		return "file", true
// 	} else {
// 		result, ok := onlineStoreType.(string)
// 		return result, ok
// 	}
// }

// func NewOfflineStore(config *RepoConfig) (OfflineLogStorage, error) {
// 	onlineStoreType, ok := getOfflineStoreType(config.OfflineStore)
// 	if !ok {
// 		onlineStore, err := NewSqliteOnlineStore(config.Project, config, config.OnlineStore)
// 		return onlineStore, err
// 	}
// }
