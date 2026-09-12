package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Field struct {
	Name  string
	Dtype types.ValueType_Enum
	// Substituted when the feature is missing or null. Nil means no default.
	DefaultValue *types.Value
}

func NewFieldFromProto(proto *core.FeatureSpecV2) *Field {
	return &Field{
		Name:         proto.Name,
		Dtype:        proto.ValueType,
		DefaultValue: proto.DefaultValue,
	}
}
