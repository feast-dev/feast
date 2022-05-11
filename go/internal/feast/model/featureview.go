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

var DUMMY_ENTITY types.Value = types.Value{Val: &types.Value_StringVal{StringVal: DUMMY_ENTITY_VAL}}

type FeatureView struct {
	Base     *BaseFeatureView
	Ttl      *durationpb.Duration
	Entities []string
}

func NewFeatureViewFromProto(proto *core.FeatureView) *FeatureView {
	featureView := &FeatureView{Base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
		Ttl: &(*proto.Spec.Ttl),
	}
	if len(proto.Spec.Entities) == 0 {
		featureView.Entities = []string{DUMMY_ENTITY_NAME}
	} else {
		featureView.Entities = proto.Spec.Entities
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

func (fs *FeatureView) HasEntity(lookup string) bool {
	for _, entityName := range fs.Entities {
		if entityName == lookup {
			return true
		}
	}
	return false
}
