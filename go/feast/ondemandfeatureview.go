package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

// TODO (Ly): parse attributes of proto into OnDemandFeatureView
type OnDemandFeatureView struct {
	base *BaseFeatureView
}

// TODO (Ly): Potential memory error for features
func NewOnDemandFeatureViewFromProto(proto *core.OnDemandFeatureView) *OnDemandFeatureView {
	onDemandFeatureView := &OnDemandFeatureView{base: NewBaseFeatureView(proto.Spec.Name, proto.Spec.Features) }
	return onDemandFeatureView
}

func (fs *OnDemandFeatureView) NewOnDemandFeatureViewFromBase(base *BaseFeatureView) *OnDemandFeatureView {

	featureView := &OnDemandFeatureView{	base: base }
	return featureView
}