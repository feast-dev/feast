package feast

import (
	"fmt"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

type BaseFeatureView struct {
	Name       string
	Features   []*Feature
	Projection *FeatureViewProjection
}

func NewBaseFeatureView(name string, featureProtos []*core.FeatureSpecV2) *BaseFeatureView {
	base := &BaseFeatureView{Name: name}
	features := make([]*Feature, len(featureProtos))
	for index, featureSpecV2 := range featureProtos {
		features[index] = NewFeatureFromProto(featureSpecV2)
	}
	base.Features = features
	base.Projection = NewFeatureViewProjectionFromDefinition(base)
	return base
}

func (fv *BaseFeatureView) withProjection(projection *FeatureViewProjection) (*BaseFeatureView, error) {
	if projection.Name != fv.Name {
		return nil, fmt.Errorf("the projection for the %s FeatureView cannot be applied because it differs "+
			"in name; the projection is named %s and the name indicates which "+
			"FeatureView the projection is for", fv.Name, projection.Name)
	}
	features := make(map[string]bool)
	for _, feature := range fv.Features {
		features[feature.Name] = true
	}
	for _, feature := range projection.Features {
		if _, ok := features[feature.Name]; !ok {
			return nil, fmt.Errorf("the projection for %s cannot be applied because it contains %s which the "+
				"FeatureView doesn't have", projection.Name, feature.Name)
		}
	}
	return &BaseFeatureView{Name: fv.Name, Features: fv.Features, Projection: projection}, nil
}

func CreateBaseFeatureView(name string, features []*Feature, projection *FeatureViewProjection) *BaseFeatureView {
	return &BaseFeatureView{
		Name:       name,
		Features:   features,
		Projection: projection,
	}
}
