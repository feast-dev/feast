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

// TODO (Ly): parse attributes of proto into RequestFeatureView
type RequestFeatureView struct {
	proto *core.RequestFeatureView
	projection *FeatureViewProjection
}

func NewRequestFeatureViewFromProto(proto *core.RequestFeatureView) *RequestFeatureView {
	return &RequestFeatureView{proto: proto, projection: nil }
}

func (fv *RequestFeatureView) withProjection(projection *FeatureViewProjection) (*RequestFeatureView, error) {
	if projection.proto.FeatureViewName != fv.proto.GetSpec().GetName() {
		return nil, errors.New(fmt.Sprintf("The projection for the %s FeatureView cannot be applied because it differs in name. "
									"The projection is named %s and the name indicates which "
									"FeatureView the projection is for.", fv.proto.GetSpec().GetName(), projection.proto.GetFeatureViewName()))
	}
	// TODO (Ly): add features to RequestFeatureView
	// and check for error here
	return &RequestFeatureView{proto: fv.proto, projection: projection}, nil
}
