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
	OnlineRead(entityKeys []types.EntityKey, featureReferences []serving.FeatureReferenceV2) [][]Feature
}
