package transformation

import (
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/core"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func createFeature(name string, valueType prototypes.ValueType_Enum) *core.FeatureSpecV2 {
	return &core.FeatureSpecV2{
		Name:      name,
		ValueType: valueType,
	}
}

func createOnDemandFeatureView(name string, featureSources map[string][]*core.FeatureSpecV2, features ...*core.FeatureSpecV2) *model.OnDemandFeatureView {
	sources := make(map[string]*core.OnDemandSource)
	for viewName, features := range featureSources {
		sources[viewName] = &core.OnDemandSource{
			Source: &core.OnDemandSource_FeatureViewProjection{
				FeatureViewProjection: &core.FeatureViewProjection{
					FeatureViewName: viewName,
					FeatureColumns:  features,
					JoinKeyMap:      map[string]string{},
				},
			},
		}
	}

	proto := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name:     name,
			Sources:  sources,
			Features: features,
		},
	}
	return model.NewOnDemandFeatureViewFromProto(proto)
}

func TestCallTransformationsFailsWithError(t *testing.T) {
	featASpec := createFeature("featA", prototypes.ValueType_INT32)
	featBSpec := createFeature("featB", prototypes.ValueType_INT32)
	onDemandFeature1 := createFeature("featC", prototypes.ValueType_FLOAT)
	odfv := createOnDemandFeatureView("odfv",
		map[string][]*core.FeatureSpecV2{"viewA": {featASpec}, "viewB": {featBSpec}},
		onDemandFeature1)

	retrievedFeatures := make(map[string]arrow.Array)
	requestContextArrow := make(map[string]arrow.Array)

	_, err := CallTransformations(odfv, retrievedFeatures, requestContextArrow, func(ODFVName string, inputArrPtr, inputSchemaPtr, outArrPtr, outSchemaPtr uintptr, fullFeatureNames bool) int {
		return 1
	}, 1, false)
	assert.Error(t, err)
}
