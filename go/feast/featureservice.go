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

// Wrapper around core.FeatureView to add projection
type FeatureService struct {
	name string
	project string
	createdTimestamp *timestamppb.Timestamp
	lastUpdatedTimestamp *timestamppb.Timestamp
	projections []*FeatureViewProjection
}

func NewFeatureServiceFromProto(proto *core.FeatureService) *FeatureService {
	projections := make([]*FeatureViewProjection, len(proto.GetFeatures()))
	for index, projection := range proto.GetFeatures() {
		projections[index] = &FeatureViewProjection{proto: projection}
	}
	return 	&FeatureService	{	name: proto.GetSpec().GetName(),
								project: proto.GetSpec().GetName(),
								createdTimestamp: proto.GetMeta().GetCreatedTimestamp(),
								lastUpdatedTimestamp: proto.GetMeta().GetLastUpdatedTimestamp(),
								projections: projections,
							}
}