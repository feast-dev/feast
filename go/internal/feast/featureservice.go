package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type FeatureService struct {
	name                 string
	project              string
	createdTimestamp     *timestamppb.Timestamp
	lastUpdatedTimestamp *timestamppb.Timestamp
	projections          []*FeatureViewProjection
}

func NewFeatureServiceFromProto(proto *core.FeatureService) *FeatureService {
	projections := make([]*FeatureViewProjection, len(proto.Spec.Features))
	for index, projectionProto := range proto.Spec.Features {
		projections[index] = NewFeatureViewProjectionFromProto(projectionProto)
	}
	return &FeatureService{name: proto.Spec.Name,
		project:              proto.Spec.Project,
		createdTimestamp:     proto.Meta.CreatedTimestamp,
		lastUpdatedTimestamp: proto.Meta.LastUpdatedTimestamp,
		projections:          projections,
	}
}
