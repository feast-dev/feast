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
	EntitiesMap map[string]struct{}
	Entities    []string
}

func NewFeatureViewFromProto(proto *core.FeatureView) *FeatureView {
	featureView := &FeatureView{Base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
		Ttl: &(*proto.Spec.Ttl),
	}
	if len(proto.Spec.Entities) == 0 {
		featureView.EntitiesMap = map[string]struct{}{DUMMY_ENTITY_NAME: {}}
		featureView.Entities = []string{}
	} else {
		featureView.EntitiesMap = make(map[string]struct{})
		for _, entityName := range proto.Spec.Entities {
			featureView.EntitiesMap[entityName] = struct{}{}
		}
		featureView.Entities = proto.Spec.Entities
	}
	return featureView
}

func (fs *FeatureView) NewFeatureViewFromBase(base *BaseFeatureView) *FeatureView {
	ttl := durationpb.Duration{Seconds: fs.Ttl.Seconds, Nanos: fs.Ttl.Nanos}
	featureView := &FeatureView{Base: base,
		Ttl:         &ttl,
		EntitiesMap: fs.EntitiesMap,
		Entities:    fs.Entities,
	}
	return featureView
}
