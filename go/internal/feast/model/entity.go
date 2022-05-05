package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Entity struct {
	Name      string
	ValueType types.ValueType_Enum
	JoinKey   string
}

func NewEntityFromProto(proto *core.Entity) *Entity {
	return &Entity{Name: proto.Spec.Name,
		ValueType: proto.Spec.ValueType,
		JoinKey:   proto.Spec.JoinKey,
	}
}
