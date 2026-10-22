package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type OnDemandFeatureView struct {
	Base                         *BaseFeatureView
	SourceFeatureViewProjections map[string]*FeatureViewProjection
	SourceRequestDataSources     map[string]*core.DataSource_RequestDataOptions
}

func NewOnDemandFeatureViewFromProto(proto *core.OnDemandFeatureView) *OnDemandFeatureView {
	onDemandFeatureView := &OnDemandFeatureView{Base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
		SourceFeatureViewProjections: make(map[string]*FeatureViewProjection),
		SourceRequestDataSources:     make(map[string]*core.DataSource_RequestDataOptions),
	}
	for sourceName, onDemandSource := range proto.Spec.Sources {
		if onDemandSourceFeatureView, ok := onDemandSource.Source.(*core.OnDemandSource_FeatureView); ok {
			featureViewProto := onDemandSourceFeatureView.FeatureView
			featureView := NewFeatureViewFromProto(featureViewProto)
			onDemandFeatureView.SourceFeatureViewProjections[sourceName] = featureView.Base.Projection
		} else if onDemandSourceFeatureViewProjection, ok := onDemandSource.Source.(*core.OnDemandSource_FeatureViewProjection); ok {
			featureProjectionProto := onDemandSourceFeatureViewProjection.FeatureViewProjection
			onDemandFeatureView.SourceFeatureViewProjections[sourceName] = NewFeatureViewProjectionFromProto(featureProjectionProto)
		} else if onDemandSourceRequestFeatureView, ok := onDemandSource.Source.(*core.OnDemandSource_RequestDataSource); ok {

			if dataSourceRequestOptions, ok := onDemandSourceRequestFeatureView.RequestDataSource.Options.(*core.DataSource_RequestDataOptions_); ok {
				onDemandFeatureView.SourceRequestDataSources[sourceName] = dataSourceRequestOptions.RequestDataOptions
			}
		}
	}

	return onDemandFeatureView
}

func (fs *OnDemandFeatureView) NewWithProjection(projection *FeatureViewProjection) (*OnDemandFeatureView, error) {
	projectedBase, err := fs.Base.WithProjection(projection)
	if err != nil {
		return nil, err
	}
	featureView := &OnDemandFeatureView{
		Base:                         projectedBase,
		SourceFeatureViewProjections: fs.SourceFeatureViewProjections,
		SourceRequestDataSources:     fs.SourceRequestDataSources,
	}
	return featureView, nil
}

func NewOnDemandFeatureViewFromBase(base *BaseFeatureView) *OnDemandFeatureView {
	featureView := &OnDemandFeatureView{
		Base:                         base,
		SourceFeatureViewProjections: map[string]*FeatureViewProjection{},
		SourceRequestDataSources:     map[string]*core.DataSource_RequestDataOptions{}}
	return featureView
}

func (fs *OnDemandFeatureView) ProjectWithFeatures(featureNames []string) (*OnDemandFeatureView, error) {
	return fs.NewWithProjection(fs.Base.ProjectWithFeatures(featureNames))
}

func (fs *OnDemandFeatureView) GetRequestDataSchema() map[string]types.ValueType_Enum {
	schema := make(map[string]types.ValueType_Enum)
	for _, requestDataSource := range fs.SourceRequestDataSources {
		for _, featureSpec := range requestDataSource.Schema {
			schema[featureSpec.Name] = featureSpec.ValueType
		}
	}
	return schema
}
