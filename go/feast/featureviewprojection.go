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

type FeatureViewProjection struct {
	featureViewName string
	featureViewNameAlias string
	featureColumns []*core.FeatureSpecV2
	joinKeyMap map[string]string
}

func (fv *FeatureViewProjection) nameToUse() string {
	if len(fv.featureViewName) == 0 {
		return fv.featureViewNameAlias
	}
	return fv.featureViewName
}

func NewFeatureViewProjectionFromProto(proto *core.FeatureViewProjection) *FeatureViewProjection {
	return &FeatureViewProjection 	{	featureViewName: proto.FeatureViewName,
										featureViewNameAlias: proto.FeatureViewNameAlias,
										featureColumns: proto.FeatureColumns,
										joinKeyMap: proto.JoinKeyMap,
									}
}

func NewFeatureViewProjectionFromDefinition(featureView *FeatureView) *FeatureViewProjection {
	return &FeatureViewProjection 	{ 	featureViewName: featureView.proto.GetSpec().GetName(),
										featureViewNameAlias: "",
										featureColumns: featureView.proto.GetSpec().GetFeatures(),
										joinKeyMap: make(map[string]string),
									}
}