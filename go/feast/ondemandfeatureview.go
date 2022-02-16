package feast

import (
	"errors"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/golang/protobuf/proto"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"io/ioutil"
	"strings"
	// "sort"
	"fmt"
)

// TODO (Ly): parse attributes of proto into OnDemandFeatureView
type OnDemandFeatureView struct {
	proto *core.OnDemandFeatureView
	projection *FeatureViewProjection
}

func NewOnDemandFeatureViewFromProto(proto *core.OnDemandFeatureView) *OnDemandFeatureView {
	return &OnDemandFeatureView{proto: proto, projection:nil }
}

func (fv *OnDemandFeatureView) withProjection(projection *FeatureViewProjection) (*OnDemandFeatureView, error) {
	if projection.proto.FeatureViewName != fv.proto.GetSpec().GetName() {
		return nil, errors.New(fmt.Sprintf("The projection for the %s FeatureView cannot be applied because it differs in name. "
									"The projection is named %s and the name indicates which "
									"FeatureView the projection is for.", fv.proto.GetSpec().GetName(), projection.proto.GetFeatureViewName()))
	}
	features := map[*core.FeatureReferenceV2]bool
	for _, feature := range fv.proto.GetSpec().GetFeatures() {
		features[feature] = true
	}
	for _, feature := range projection.proto.GetFeatureColumns() {
		if _, ok := features[feature]; !ok {
			return nil, errors.New(fmt.Sprintf("The projection for %s cannot be applied because it contains %s which the "
												"FeatureView doesn't have.", projection.proto.GetFeatureViewName(), feature.GetFeatureViewName()))
		}
	}
	return &OnDemandFeatureView{proto: fv.proto, projection: projection}, nil
}
