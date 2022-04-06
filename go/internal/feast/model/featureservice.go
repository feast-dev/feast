package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type FeatureService struct {
	Name                 string
	Project              string
	CreatedTimestamp     *timestamppb.Timestamp
	LastUpdatedTimestamp *timestamppb.Timestamp
	Projections          []*FeatureViewProjection
}

func NewFeatureServiceFromProto(proto *core.FeatureService) *FeatureService {
	projections := make([]*FeatureViewProjection, len(proto.Spec.Features))
	for index, projectionProto := range proto.Spec.Features {
		projections[index] = NewFeatureViewProjectionFromProto(projectionProto)
	}
	return &FeatureService{Name: proto.Spec.Name,
		Project:              proto.Spec.Project,
		CreatedTimestamp:     proto.Meta.CreatedTimestamp,
		LastUpdatedTimestamp: proto.Meta.LastUpdatedTimestamp,
		Projections:          projections,
	}
}

func NewFeatureService(name string, project string, createdTimestamp *timestamppb.Timestamp, lastUpdatedTimestamp *timestamppb.Timestamp, projections []*FeatureViewProjection) *FeatureService {
	return &FeatureService{
		name:                 name,
		project:              project,
		createdTimestamp:     createdTimestamp,
		lastUpdatedTimestamp: lastUpdatedTimestamp,
		Projections:          projections,
	}
}
