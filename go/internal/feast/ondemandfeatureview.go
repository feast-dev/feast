package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type OnDemandFeatureView struct {
	base                         *BaseFeatureView
	sourceFeatureViewProjections map[string]*FeatureViewProjection
	sourceRequestDataSources     map[string]*core.DataSource_RequestDataOptions
}

func NewOnDemandFeatureViewFromProto(proto *core.OnDemandFeatureView) *OnDemandFeatureView {
	onDemandFeatureView := &OnDemandFeatureView{base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
		sourceFeatureViewProjections: make(map[string]*FeatureViewProjection),
		sourceRequestDataSources:     make(map[string]*core.DataSource_RequestDataOptions),
	}
	for sourceName, onDemandSource := range proto.Spec.Sources {
		if onDemandSourceFeatureView, ok := onDemandSource.Source.(*core.OnDemandSource_FeatureView); ok {
			featureViewProto := onDemandSourceFeatureView.FeatureView
			featureView := NewFeatureViewFromProto(featureViewProto)
			onDemandFeatureView.sourceFeatureViewProjections[sourceName] = featureView.base.projection
		} else if onDemandSourceFeatureViewProjection, ok := onDemandSource.Source.(*core.OnDemandSource_FeatureViewProjection); ok {
			featureProjectionProto := onDemandSourceFeatureViewProjection.FeatureViewProjection
			onDemandFeatureView.sourceFeatureViewProjections[sourceName] = NewFeatureViewProjectionFromProto(featureProjectionProto)
		} else if onDemandSourceRequestFeatureView, ok := onDemandSource.Source.(*core.OnDemandSource_RequestDataSource); ok {

			if dataSourceRequestOptions, ok := onDemandSourceRequestFeatureView.RequestDataSource.Options.(*core.DataSource_RequestDataOptions_); ok {
				onDemandFeatureView.sourceRequestDataSources[sourceName] = dataSourceRequestOptions.RequestDataOptions
			}
		}
	}

	return onDemandFeatureView
}

func (fs *OnDemandFeatureView) NewOnDemandFeatureViewFromBase(base *BaseFeatureView) *OnDemandFeatureView {

	featureView := &OnDemandFeatureView{base: base}
	return featureView
}

func (fs *OnDemandFeatureView) getRequestDataSchema() map[string]types.ValueType_Enum {
	schema := make(map[string]types.ValueType_Enum)
	for _, requestDataSource := range fs.sourceRequestDataSources {
		for fieldName, fieldValueType := range requestDataSource.Schema {
			schema[fieldName] = fieldValueType
		}
	}
	return schema
}
