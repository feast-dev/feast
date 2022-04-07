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
		features[feature.name] = true
	}
	for _, feature := range projection.Features {
		if _, ok := features[feature.name]; !ok {
			return nil, fmt.Errorf("the projection for %s cannot be applied because it contains %s which the "+
				"FeatureView doesn't have", projection.Name, feature.name)
		}
	}
	return &BaseFeatureView{Name: fv.Name, Features: fv.Features, Projection: projection}, nil
}

func (fv *BaseFeatureView) projectWithFeatures(featureNames []string) *FeatureViewProjection {
	features := make([]*Feature, 0)
	for _, feature := range fv.Features {
		for _, allowedFeatureName := range featureNames {
			if feature.name == allowedFeatureName {
				features = append(features, feature)
			}
		}
	}

	return &FeatureViewProjection{
		Name:     fv.Name,
		Features: features,
	}
}
