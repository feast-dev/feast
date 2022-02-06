package feast

import (
	"errors"
	"fmt"
)

func getOnlineStore(config map[string]interface{}) (OnlineStore, error) {
	onlineStoreConfig, ok := getOnlineStoreConfig(config)
	if !ok {
		return nil, errors.New(fmt.Sprintf("could not get online store config from config: %+v", config))
	}
	onlineStoreType, ok := getOnlineStoreType(onlineStoreConfig)
	if !ok {
		return nil, errors.New(fmt.Sprintf("could not get online store type from online store config: %+v", onlineStoreConfig))
	}
	if onlineStoreType == "redis" {
		onlineStore, err := NewRedisOnlineStore(onlineStoreConfig)
		return onlineStore, err
	} else {
		// TODO(willem): Python connectors here
		return nil, errors.New("not implemented")
	}
}
