package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type OnDemandFeatureView struct {
	base                        *BaseFeatureView
	inputFeatureViewProjections map[string]*FeatureViewProjection
	inputRequestDataSources     map[string]*core.DataSource_RequestDataOptions
}

func NewOnDemandFeatureViewFromProto(proto *core.OnDemandFeatureView) *OnDemandFeatureView {
	onDemandFeatureView := &OnDemandFeatureView{base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features),
		inputFeatureViewProjections: make(map[string]*FeatureViewProjection),
		inputRequestDataSources:     make(map[string]*core.DataSource_RequestDataOptions),
	}
	for inputName, onDemandInput := range proto.Spec.Inputs {
		if onDemandInputFeatureView, ok := onDemandInput.Input.(*core.OnDemandInput_FeatureView); ok {
			featureViewProto := onDemandInputFeatureView.FeatureView
			featureView := NewFeatureViewFromProto(featureViewProto)
			onDemandFeatureView.inputFeatureViewProjections[inputName] = featureView.base.projection
		} else if onDemandInputFeatureViewProjection, ok := onDemandInput.Input.(*core.OnDemandInput_FeatureViewProjection); ok {
			featureProjectionProto := onDemandInputFeatureViewProjection.FeatureViewProjection
			onDemandFeatureView.inputFeatureViewProjections[inputName] = NewFeatureViewProjectionFromProto(featureProjectionProto)
		} else if onDemandInputRequestFeatureView, ok := onDemandInput.Input.(*core.OnDemandInput_RequestDataSource); ok {

			if dataSourceRequestOptions, ok := onDemandInputRequestFeatureView.RequestDataSource.Options.(*core.DataSource_RequestDataOptions_); ok {
				onDemandFeatureView.inputRequestDataSources[inputName] = dataSourceRequestOptions.RequestDataOptions
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
	for _, requestDataSource := range fs.inputRequestDataSources {
		for fieldName, fieldValueType := range requestDataSource.Schema {
			schema[fieldName] = fieldValueType
		}
	}
	return schema
}
