package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type FeatureViewProjection struct {
	Name       string
	NameAlias  string
	Features   []*Feature
	JoinKeyMap map[string]string
}

func (fv *FeatureViewProjection) nameToUse() string {
	if len(fv.NameAlias) == 0 {
		return fv.Name
	}
	return fv.NameAlias
}

func NewFeatureViewProjectionFromProto(proto *core.FeatureViewProjection) *FeatureViewProjection {
	featureProjection := &FeatureViewProjection{Name: proto.FeatureViewName,
		NameAlias:  proto.FeatureViewNameAlias,
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
	return &FeatureViewProjection{Name: base.name,
		NameAlias:  "",
		Features:   base.features,
		JoinKeyMap: make(map[string]string),
	}
}

func NewFeatureViewProjection(name string, nameAlias string, features []*Feature, joinKeyMap map[string]string) *FeatureViewProjection {
	return &FeatureViewProjection{Name: name,
		NameAlias:  nameAlias,
		Features:   features,
		JoinKeyMap: joinKeyMap,
	}
}
