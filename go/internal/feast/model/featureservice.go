package model

import (
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

type FeatureService struct {
	Name                 string
	Project              string
	CreatedTimestamp     *timestamppb.Timestamp
	LastUpdatedTimestamp *timestamppb.Timestamp
	Projections          []*FeatureViewProjection
	LoggingConfig        *FeatureServiceLoggingConfig
}

type FeatureServiceLoggingConfig struct {
	SampleRate float32
}

func NewFeatureServiceFromProto(proto *core.FeatureService) *FeatureService {
	projections := make([]*FeatureViewProjection, len(proto.Spec.Features))
	for index, projectionProto := range proto.Spec.Features {
		projections[index] = NewFeatureViewProjectionFromProto(projectionProto)
	}
	var loggingConfig *FeatureServiceLoggingConfig
	if proto.GetSpec().GetLoggingConfig() != nil {
		loggingConfig = &FeatureServiceLoggingConfig{
			SampleRate: proto.GetSpec().GetLoggingConfig().SampleRate,
		}
	}
	return &FeatureService{Name: proto.Spec.Name,
		Project:              proto.Spec.Project,
		CreatedTimestamp:     proto.Meta.CreatedTimestamp,
		LastUpdatedTimestamp: proto.Meta.LastUpdatedTimestamp,
		Projections:          projections,
		LoggingConfig:        loggingConfig,
	}
}
