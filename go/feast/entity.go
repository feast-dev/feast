package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type Entity struct {
	name string
	valueType types.ValueType_Enum
	joinKey string

}

func NewEntityFromProto(proto *core.Entity) *Entity {
	return &Entity 	{	name: proto.Spec.Name,
						valueType: proto.Spec.ValueType,
						joinKey: proto.Spec.JoinKey,
					}
}