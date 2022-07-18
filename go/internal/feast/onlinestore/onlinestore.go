package onlinestore

import (
	"context"
	"fmt"

	"github.com/feast-dev/feast/go/internal/feast/registry"

	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type FeatureData struct {
	Reference serving.FeatureReferenceV2
	Timestamp timestamp.Timestamp
	Value     types.Value
}

type OnlineStore interface {
	// OnlineRead reads multiple features (specified in featureReferences) for multiple
	// entity keys (specified in entityKeys) and returns an array of array of features,
	// where each feature contains 3 fields:
	//   1. feature Reference
	//   2. feature event timestamp
	//   3. feature value
	// The inner array will have the same size as featureReferences,
	// while the outer array will have the same size as entityKeys.

	// TODO: Can we return [][]FeatureData, []timstamps, error
	// instead and remove timestamp from FeatureData struct to mimic Python's code
	// and reduces repeated memory storage for the same timstamp (which is stored as value and not as a pointer).
	// Should each attribute in FeatureData be stored as a pointer instead since the current
	// design forces value copied in OnlineRead + GetOnlineFeatures
	// (array is destructed so we cannot use the same fields in each
	// Feature object as pointers in GetOnlineFeaturesResponse)
	// => allocate memory for each field once in OnlineRead
	// and reuse them in GetOnlineFeaturesResponse?
	OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error)
	// Destruct must be call once user is done using OnlineStore
	// This is to comply with the Connector since we have to close the plugin
	Destruct()
}

func getOnlineStoreType(onlineStoreConfig map[string]interface{}) (string, bool) {
	if onlineStoreType, ok := onlineStoreConfig["type"]; !ok {
		// If online store type isn't specified, default to sqlite
		return "sqlite", true
	} else {
		result, ok := onlineStoreType.(string)
		return result, ok
	}
}

func NewOnlineStore(config *registry.RepoConfig) (OnlineStore, error) {
	onlineStoreType, ok := getOnlineStoreType(config.OnlineStore)
	if !ok {
		return nil, fmt.Errorf("could not get online store type from online store config: %+v", config.OnlineStore)
	} else if onlineStoreType == "sqlite" {
		onlineStore, err := NewSqliteOnlineStore(config.Project, config, config.OnlineStore)
		return onlineStore, err
	} else if onlineStoreType == "redis" {
		onlineStore, err := NewRedisOnlineStore(config.Project, config, config.OnlineStore)
		return onlineStore, err
	} else {
		return nil, fmt.Errorf("%s online store type is currently not supported; only redis and sqlite are supported", onlineStoreType)
	}
}
