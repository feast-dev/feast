package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	// "github.com/feast-dev/feast/go/protos/feast/types"
)

// TODO (Ly): parse attributes of proto into RequestFeatureView
// as needed
type RequestFeatureView struct {
	base *BaseFeatureView
	// schema map[string]types.ValueType_Enum
}

func NewRequestFeatureViewFromProto(proto *core.RequestFeatureView) *RequestFeatureView {
	requestFeatureView := &RequestFeatureView{}
	if dataSourceRequestOptions, ok := proto.Spec.RequestDataSource.Options.(*core.DataSource_RequestDataOptions_); !ok {
		return nil
	} else {
		numFeatures := len(dataSourceRequestOptions.RequestDataOptions.Schema)
		features := make([]*core.FeatureSpecV2, numFeatures)
		index := 0
		for featureName, valueType := range dataSourceRequestOptions.RequestDataOptions.Schema {
			features[index] = &core.FeatureSpecV2{ 	Name: featureName,
													ValueType: valueType,
													}
		}
		// requestFeatureView.schema = dataSourceRequestOptions.RequestDataOptions.Schema
		requestFeatureView.base = NewBaseFeatureView(proto.Spec.Name, features)
		return requestFeatureView
	}
}

func (fs *RequestFeatureView) NewRequestFeatureViewFromBase(base *BaseFeatureView) *RequestFeatureView {

	featureView := &RequestFeatureView	{	base: base }
	return featureView
}
