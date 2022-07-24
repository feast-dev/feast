package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type Entity struct {
	Name    string
	JoinKey string
}

func NewEntityFromProto(proto *core.Entity) *Entity {
	return &Entity{
		Name:    proto.Spec.Name,
		JoinKey: proto.Spec.JoinKey,
	}
}
