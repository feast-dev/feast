package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/timestamp"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalidFeatureName       = "Invalid feature name %s provided, feature names must be in the format featureSet:version:featureName."
)

// OnlineFeaturesRequest wrapper on feast.serving.GetOnlineFeaturesRequest.
type OnlineFeaturesRequest struct {

	// Features is the list of features to obtain from Feast. Each feature must be given by its fully qualified ID,
	// in the format featureSet:version:featureName.
	Features      []string

	// MaxAgeSeconds is the maximum allowed staleness of the features in seconds. This max age will be applied to all
	// featureSets in this request.
	// Setting this value to 0 will cause feast to default to the max age specified on the feature set spec, if any.
	MaxAgeSeconds int

	// Entities is the list of entity rows to retrieve features on. Each row is a map of entity name to entity value.
	Entities      []Row
}

// Builds the feast-specified request payload from the wrapper.
func (r OnlineFeaturesRequest) buildRequest() (*serving.GetOnlineFeaturesRequest, error) {
	featureSets, err := buildFeatureSets(r.Features, r.MaxAgeSeconds)
	if err != nil {
		return nil, err
	}
	entityRows := make([]*serving.GetOnlineFeaturesRequest_EntityRow, len(r.Entities))

	for i := range r.Entities {
		entityRows[i] = &serving.GetOnlineFeaturesRequest_EntityRow{
			EntityTimestamp: &timestamp.Timestamp{Seconds: time.Now().Unix()},
			Fields:          r.Entities[i],
		}
	}
	return &serving.GetOnlineFeaturesRequest{
		FeatureSets: featureSets,
		EntityRows:  entityRows,
	}, nil
}

func buildFeatureSets(features []string, maxAgeSeconds int) ([]*serving.GetOnlineFeaturesRequest_FeatureSet, error) {
	featureSetMap := map[string]*serving.GetOnlineFeaturesRequest_FeatureSet{}
	for _, feature := range features {
		split := strings.Split(feature, ":")
		if len(split) != 3 {
			return nil, fmt.Errorf(ErrInvalidFeatureName, feature)
		}
		key := split[0] + ":" + split[1]
		if fs, ok := featureSetMap[key]; !ok {
			version, err := strconv.Atoi(split[1])
			if err != nil {
				return nil, fmt.Errorf(ErrInvalidFeatureName, feature)
			}
			featureSetMap[key] = &serving.GetOnlineFeaturesRequest_FeatureSet{
				Name:         split[0],
				Version:      int32(version),
				FeatureNames: []string{split[2]},
				MaxAge:       &duration.Duration{Seconds: int64(maxAgeSeconds)},
			}
		} else {
			fs.FeatureNames = append(fs.GetFeatureNames(), split[2])
		}
	}
	var featureSets []*serving.GetOnlineFeaturesRequest_FeatureSet
	for _, featureSet := range featureSetMap {
		featureSets = append(featureSets, featureSet)
	}
	return featureSets, nil
}
