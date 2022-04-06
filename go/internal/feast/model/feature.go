package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Feature struct {
	Name  string
	Dtype types.ValueType_Enum
}

func NewFeatureFromProto(proto *core.FeatureSpecV2) *Feature {
	return &Feature{Name: proto.Name,
		Dtype: proto.ValueType,
	}
}

func NewFeature(name string, dtype types.ValueType_Enum) *Feature {
	return &Feature{Name: name,
		Dtype: dtype,
	}
}
