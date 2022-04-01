package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type FeatureViewProjection struct {
	name       string
	nameAlias  string
	Features   []*Feature
	JoinKeyMap map[string]string
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
		JoinKeyMap: proto.JoinKeyMap,
	}

	features := make([]*Feature, len(proto.FeatureColumns))
	for index, featureSpecV2 := range proto.FeatureColumns {
		features[index] = NewFeatureFromProto(featureSpecV2)
	}
	featureProjection.Features = features
	return featureProjection
}

func NewFeatureViewProjectionFromDefinition(base *BaseFeatureView) *FeatureViewProjection {
	return &FeatureViewProjection{name: base.name,
		nameAlias:  "",
		Features:   base.features,
		JoinKeyMap: make(map[string]string),
	}
}
