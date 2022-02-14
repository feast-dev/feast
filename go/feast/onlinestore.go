package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type Feature struct {
	reference serving.FeatureReferenceV2
	timestamp timestamp.Timestamp
	value     types.Value
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
	OnlineRead(entityKeys []types.EntityKey, view string, features []string) ([][]Feature, error)
	// Destruct must be call once user is done using OnlineStore
	Destruct()
}

func getOnlineStoreType(onlineStoreConfig map[string]interface{}) (string, bool) {
	if onlineStoreType, ok := onlineStoreConfig["type"]; !ok {
		return "", false
	} else {
		result, ok := onlineStoreType.(string)
		return result, ok
	}
}

