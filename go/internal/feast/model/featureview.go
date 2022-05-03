package model

import (
	durationpb "google.golang.org/protobuf/types/known/durationpb"

	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

const (
	DUMMY_ENTITY_ID   = "__dummy_id"
	DUMMY_ENTITY_NAME = "__dummy"
	DUMMY_ENTITY_VAL  = ""
)

var DUMMY_ENTITY_VALUE types.Value = types.Value{Val: &types.Value_StringVal{StringVal: DUMMY_ENTITY_VAL}}

type FeatureView struct {
	Base          *BaseFeatureView
	Ttl           *durationpb.Duration
	EntityNames   []string
	EntityColumns []*Feature
}

func NewFeatureViewFromProto(proto *core.FeatureView) *FeatureView {
	featureView := &FeatureView{Base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
		Ttl: &(*proto.Spec.Ttl),
	}
	if len(proto.Spec.Entities) == 0 {
		featureView.EntityNames = []string{DUMMY_ENTITY_NAME}
	} else {
		featureView.EntityNames = proto.Spec.Entities
	}
	entityColumns := make([]*Feature, len(proto.Spec.EntityColumns))
	for i, entityColumn := range proto.Spec.EntityColumns {
		entityColumns[i] = NewFeatureFromProto(entityColumn)
	}
	featureView.EntityColumns = entityColumns
	return featureView
}

func (fv *FeatureView) NewFeatureViewFromBase(base *BaseFeatureView) *FeatureView {
	ttl := durationpb.Duration{Seconds: fv.Ttl.Seconds, Nanos: fv.Ttl.Nanos}
	featureView := &FeatureView{Base: base,
		Ttl:         &ttl,
		EntityNames: fv.EntityNames,
	}
	return featureView
}

func (fv *FeatureView) HasEntity(name string) bool {
	for _, entityName := range fv.EntityNames {
		if entityName == name {
			return true
		}
	}
	return false
}

func (fv *FeatureView) GetEntityType(joinKey string) types.ValueType_Enum {
	for _, entityColumn := range fv.EntityColumns {
		if entityColumn.Name == joinKey {
			return entityColumn.Dtype
		}
	}
	return types.ValueType_INVALID
}
