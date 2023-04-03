package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type FeatureViewProjection struct {
	Name       string
	NameAlias  string
	Features   []*Field
	JoinKeyMap map[string]string
}

func (fv *FeatureViewProjection) NameToUse() string {
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

	features := make([]*Field, len(proto.FeatureColumns))
	for index, featureSpecV2 := range proto.FeatureColumns {
		features[index] = NewFieldFromProto(featureSpecV2)
	}
	featureProjection.Features = features
	return featureProjection
}

func NewFeatureViewProjectionFromDefinition(base *BaseFeatureView) *FeatureViewProjection {
	return &FeatureViewProjection{Name: base.Name,
		NameAlias:  "",
		Features:   base.Features,
		JoinKeyMap: make(map[string]string),
	}
}
