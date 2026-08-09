package model

import (
	"fmt"
	"sync"

	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type BaseFeatureView struct {
	Name       string
	Features   []*Field
	Projection *FeatureViewProjection
	// Built on first use rather than at construction, because BaseFeatureView is also
	// assembled as a struct literal; a constructor-only map would silently be empty.
	featureDefaultsOnce sync.Once
	featureDefaults     map[string]*types.Value
}

// GetDefaultValue returns the configured default for a feature, or nil. Serving calls
// this per feature per request, so the scan happens once rather than on every request.
func (fv *BaseFeatureView) GetDefaultValue(featureName string) *types.Value {
	fv.featureDefaultsOnce.Do(func() {
		for _, feature := range fv.Features {
			if feature.DefaultValue == nil {
				continue
			}
			if fv.featureDefaults == nil {
				fv.featureDefaults = make(map[string]*types.Value, len(fv.Features))
			}
			fv.featureDefaults[feature.Name] = feature.DefaultValue
		}
	})
	if fv.featureDefaults == nil {
		return nil
	}
	return fv.featureDefaults[featureName]
}

func NewBaseFeatureView(name string, featureProtos []*core.FeatureSpecV2) *BaseFeatureView {
	base := &BaseFeatureView{Name: name}
	features := make([]*Field, len(featureProtos))
	for index, featureSpecV2 := range featureProtos {
		features[index] = NewFieldFromProto(featureSpecV2)
	}
	base.Features = features
	base.Projection = NewFeatureViewProjectionFromDefinition(base)
	return base
}

func (fv *BaseFeatureView) WithProjection(projection *FeatureViewProjection) (*BaseFeatureView, error) {
	if projection.Name != fv.Name {
		return nil, fmt.Errorf("the projection for the %s FeatureView cannot be applied because it differs "+
			"in Name; the projection is named %s and the Name indicates which "+
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

func (fv *BaseFeatureView) ProjectWithFeatures(featureNames []string) *FeatureViewProjection {
	features := make([]*Field, 0)
	for _, feature := range fv.Features {
		for _, allowedFeatureName := range featureNames {
			if feature.Name == allowedFeatureName {
				features = append(features, feature)
			}
		}
	}

	return &FeatureViewProjection{
		Name:     fv.Name,
		Features: features,
	}
}
