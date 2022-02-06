package feast

import (
	"errors"
	"github.com/feast-dev/feast/go/protos/feast/serving"
)

type FeatureStore struct {
	config      map[string]interface{}
	onlineStore OnlineStore
}

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config map[string]interface{}) *FeatureStore {
	return &FeatureStore{
		config:      config,
		onlineStore: nil, // TODO: implement this
	}
}

// GetOnlineFeatures retrieves the latest online feature data
func (fs *FeatureStore) GetOnlineFeatures(request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {
	return nil, errors.New("not implemented")
}
