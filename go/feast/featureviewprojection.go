package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type FeatureViewProjection struct {
	name       string
	nameAlias  string
	features   []*Feature
	joinKeyMap map[string]string
}

func (fv *FeatureViewProjection) nameToUse() string {
	if len(fv.nameAlias) == 0 {
		return fv.name
	}
	return fv.nameAlias
}

func NewFeatureViewProjectionFromProto(proto *core.FeatureViewProjection) *FeatureViewProjection {
	featureProjection := &FeatureViewProjection{name: proto.FeatureViewName,
		nameAlias:  proto.FeatureViewNameAlias,
		joinKeyMap: proto.JoinKeyMap,
	}

	features := make([]*Feature, len(proto.FeatureColumns))
	for index, featureSpecV2 := range proto.FeatureColumns {
		features[index] = NewFeatureFromProto(featureSpecV2)
	}
	featureProjection.features = features
	return featureProjection
}

func NewFeatureViewProjectionFromDefinition(base *BaseFeatureView) *FeatureViewProjection {
	return &FeatureViewProjection{name: base.name,
		nameAlias:  "",
		features:   base.features,
		joinKeyMap: make(map[string]string),
	}
}
