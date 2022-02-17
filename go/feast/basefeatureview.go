package feast

import (
	"errors"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"fmt"
)

type BaseFeatureView struct {
	name string
	features []*core.FeatureSpecV2
	projection *FeatureViewProjection
}

func NewBaseFeatureView(name string, features []*core.FeatureSpecV2) *BaseFeatureView {
	base := &BaseFeatureView{name: name, features: features}
	base.projection = NewFeatureViewProjectionFromDefinition(base)
	return base
}

func (fv *BaseFeatureView) withProjection(projection *FeatureViewProjection) (*BaseFeatureView, error) {
	if projection.name != fv.name {
		return nil, errors.New(fmt.Sprintf("The projection for the %s FeatureView cannot be applied because it differs in name. " +
									"The projection is named %s and the name indicates which " +
									"FeatureView the projection is for.", fv.name, projection.name))
	}
	features := make(map[string]bool)
	for _, feature := range fv.features {
		features[feature.Name] = true
	}
	for _, feature := range projection.features {
		if _, ok := features[feature.Name]; !ok {
			return nil, errors.New(fmt.Sprintf("The projection for %s cannot be applied because it contains %s which the " +
												"FeatureView doesn't have.", projection.name, feature.Name))
		}
	}
	return &BaseFeatureView{name: fv.name, features: fv.features, projection: projection}, nil
}