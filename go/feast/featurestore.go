package feast

import (
	"errors"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
)

type FeatureStore struct {
	config      *RepoConfig
	registry    *core.Registry
	onlineStore OnlineStore
}

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config *RepoConfig) (*FeatureStore, error) {
	onlineStore, err := getOnlineStore(config)
	if err != nil {
		return nil, err
	}
	return &FeatureStore{
		config:      config,
		registry:    nil, // TODO: not implemented
		onlineStore: onlineStore,
	}, nil
}

// GetOnlineFeatures retrieves the latest online feature data
func (fs *FeatureStore) GetOnlineFeatures(request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {
	return nil, errors.New("not implemented")
}
