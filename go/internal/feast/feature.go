package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Feature struct {
	name  string
	dtype types.ValueType_Enum
}

func NewFeatureFromProto(proto *core.FeatureSpecV2) *Feature {
	return &Feature{name: proto.Name,
		dtype: proto.ValueType,
	}
}
