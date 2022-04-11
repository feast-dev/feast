package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

const (
	DUMMY_ENTITY_ID   = "__dummy_id"
	DUMMY_ENTITY_NAME = "__dummy"
	DUMMY_ENTITY_VAL  = ""
)

var DUMMY_ENTITY types.Value = types.Value{Val: &types.Value_StringVal{StringVal: DUMMY_ENTITY_VAL}}

type FeatureView struct {
	Base *BaseFeatureView
	Ttl  *durationpb.Duration
	// Make entities set so that search for dummy entity is faster
	Entities map[string]struct{}
}

func NewFeatureViewFromProto(proto *core.FeatureView) *FeatureView {
	featureView := &FeatureView{Base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
		Ttl: &(*proto.Spec.Ttl),
	}
	if len(proto.Spec.Entities) == 0 {
		featureView.Entities = map[string]struct{}{DUMMY_ENTITY_NAME: {}}
	} else {
		featureView.Entities = make(map[string]struct{})
		for _, entityName := range proto.Spec.Entities {
			featureView.Entities[entityName] = struct{}{}
		}
	}
	return featureView
}

func (fs *FeatureView) NewFeatureViewFromBase(base *BaseFeatureView) *FeatureView {
	ttl := durationpb.Duration{Seconds: fs.Ttl.Seconds, Nanos: fs.Ttl.Nanos}
	featureView := &FeatureView{Base: base,
		Ttl:      &ttl,
		Entities: fs.Entities,
	}
	return featureView
}

func CreateFeatureView(base *BaseFeatureView, ttl *durationpb.Duration, entities map[string]struct{}) *FeatureView {
	return &FeatureView{
		Base:     base,
		Ttl:      ttl,
		Entities: entities,
	}
}
