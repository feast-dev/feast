package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Entity struct {
	name      string
	valueType types.ValueType_Enum
	joinKey   string
}

func NewEntityFromProto(proto *core.Entity) *Entity {
	return &Entity{name: proto.Spec.Name,
		valueType: proto.Spec.ValueType,
		joinKey:   proto.Spec.JoinKey,
	}
}
