package feast

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
	base *BaseFeatureView
	ttl  *durationpb.Duration
	// Make entities set so that search for dummy entity is faster
	entities map[string]struct{}
}

func NewFeatureViewFromProto(proto *core.FeatureView) *FeatureView {
	featureView := &FeatureView{base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
		ttl: &(*proto.Spec.Ttl),
	}
	if len(proto.Spec.Entities) == 0 {
		featureView.entities = map[string]struct{}{DUMMY_ENTITY_NAME: {}}
	} else {
		featureView.entities = make(map[string]struct{})
		for _, entityName := range proto.Spec.Entities {
			featureView.entities[entityName] = struct{}{}
		}
	}
	return featureView
}

func (fs *FeatureView) NewFeatureViewFromBase(base *BaseFeatureView) *FeatureView {
	ttl := durationpb.Duration{Seconds: fs.ttl.Seconds, Nanos: fs.ttl.Nanos}
	featureView := &FeatureView{base: base,
		ttl:      &ttl,
		entities: fs.entities,
	}
	return featureView
}
