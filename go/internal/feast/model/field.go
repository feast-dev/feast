package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Field struct {
	Name  string
	Dtype types.ValueType_Enum
}

func NewFieldFromProto(proto *core.FeatureSpecV2) *Field {
	return &Field{
		Name:  proto.Name,
		Dtype: proto.ValueType,
	}
}
