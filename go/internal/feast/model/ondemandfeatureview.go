package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type OnDemandFeatureView struct {
	Base                         *BaseFeatureView
<<<<<<< HEAD:go/internal/feast/model/ondemandfeatureview.go
	SourceFeatureViewProjections map[string]*FeatureViewProjection
	SourceRequestDataSources     map[string]*core.DataSource_RequestDataOptions
=======
	sourceFeatureViewProjections map[string]*FeatureViewProjection
	sourceRequestDataSources     map[string]*core.DataSource_RequestDataOptions
>>>>>>> 51b50ae5 (Fix):go/internal/feast/ondemandfeatureview.go
}

func NewOnDemandFeatureViewFromProto(proto *core.OnDemandFeatureView) *OnDemandFeatureView {
	onDemandFeatureView := &OnDemandFeatureView{Base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
<<<<<<< HEAD:go/internal/feast/model/ondemandfeatureview.go
		SourceFeatureViewProjections: make(map[string]*FeatureViewProjection),
		SourceRequestDataSources:     make(map[string]*core.DataSource_RequestDataOptions),
=======
		sourceFeatureViewProjections: make(map[string]*FeatureViewProjection),
		sourceRequestDataSources:     make(map[string]*core.DataSource_RequestDataOptions),
>>>>>>> 51b50ae5 (Fix):go/internal/feast/ondemandfeatureview.go
	}
	for sourceName, onDemandSource := range proto.Spec.Sources {
		if onDemandSourceFeatureView, ok := onDemandSource.Source.(*core.OnDemandSource_FeatureView); ok {
			featureViewProto := onDemandSourceFeatureView.FeatureView
			featureView := NewFeatureViewFromProto(featureViewProto)
<<<<<<< HEAD:go/internal/feast/model/ondemandfeatureview.go
			onDemandFeatureView.SourceFeatureViewProjections[sourceName] = featureView.Base.Projection
=======
			onDemandFeatureView.sourceFeatureViewProjections[sourceName] = featureView.Base.Projection
>>>>>>> 51b50ae5 (Fix):go/internal/feast/ondemandfeatureview.go
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

<<<<<<< HEAD:go/internal/feast/model/ondemandfeatureview.go
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
=======
func (fs *OnDemandFeatureView) NewOnDemandFeatureViewFromBase(base *BaseFeatureView) *OnDemandFeatureView {

	featureView := &OnDemandFeatureView{Base: base}
	return featureView
>>>>>>> 51b50ae5 (Fix):go/internal/feast/ondemandfeatureview.go
}

func (fs *OnDemandFeatureView) ProjectWithFeatures(featureNames []string) (*OnDemandFeatureView, error) {
	return fs.NewWithProjection(fs.Base.ProjectWithFeatures(featureNames))
}

func (fs *OnDemandFeatureView) GetRequestDataSchema() map[string]types.ValueType_Enum {
	schema := make(map[string]types.ValueType_Enum)
	for _, requestDataSource := range fs.SourceRequestDataSources {
		for fieldName, fieldValueType := range requestDataSource.Schema {
			schema[fieldName] = fieldValueType
		}
	}
	return schema
}
