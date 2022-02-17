package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

// Wrapper around core.FeatureView to add projection
type FeatureView struct {
	base *BaseFeatureView
	ttl *durationpb.Duration
	entities []string
}

func NewFeatureViewFromProto(proto *core.FeatureView) *FeatureView {
	featureView := &FeatureView{	base: NewBaseFeatureView(	proto.Spec.Name, proto.Spec.Features),
									ttl: &(*proto.Spec.Ttl),
									entities: proto.Spec.Entities,
								}
	return featureView
}

func (fs *FeatureView) NewFeatureViewFromBase(base *BaseFeatureView) *FeatureView {
	ttl := *fs.ttl
	featureView := &FeatureView{	base: base,
									ttl: &ttl,
									entities: fs.entities,
								}
	return featureView
}

