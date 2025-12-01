package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type Entity struct {
	Name     string
	JoinKeys []string
}

// JoinKey returns the first join key for backward compatibility
func (e *Entity) JoinKey() string {
	if len(e.JoinKeys) > 0 {
		return e.JoinKeys[0]
	}
	return e.Name
}

func NewEntityFromProto(proto *core.Entity) *Entity {
	var joinKeys []string

	// Handle backward compatibility: prioritize join_keys, fall back to join_key
	if len(proto.Spec.JoinKeys) > 0 {
		joinKeys = append(joinKeys, proto.Spec.JoinKeys...)
	} else if proto.Spec.JoinKey != "" {
		joinKeys = []string{proto.Spec.JoinKey}
	} else {
		joinKeys = []string{proto.Spec.Name}
	}

	return &Entity{
		Name:     proto.Spec.Name,
		JoinKeys: joinKeys,
	}
}
