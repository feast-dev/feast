package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type FeatureViewProjection struct {
	name string
	nameAlias string
	features []*core.FeatureSpecV2
	joinKeyMap map[string]string
}

func (fv *FeatureViewProjection) nameToUse() string {
	if len(fv.nameAlias) == 0 {
		return fv.name
	}
	return fv.nameAlias
}

func NewFeatureViewProjectionFromProto(proto *core.FeatureViewProjection) *FeatureViewProjection {
	return &FeatureViewProjection 	{	name: proto.FeatureViewName,
										nameAlias: proto.FeatureViewNameAlias,
										features: proto.FeatureColumns,
										joinKeyMap: proto.JoinKeyMap,
									}
}

func NewFeatureViewProjectionFromDefinition(base *BaseFeatureView) *FeatureViewProjection {
	return &FeatureViewProjection 	{ 	name: base.name,
										nameAlias: "",
										features: base.features,
										joinKeyMap: make(map[string]string),
									}
}