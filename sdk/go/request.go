package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"strconv"
	"strings"
)

var (
	ErrInvalidFeatureName       = "Invalid feature name %s provided, feature names must be in the format featureSet:version:featureName."
)

// OnlineFeaturesRequest wrapper on feast.serving.GetOnlineFeaturesRequest.
type OnlineFeaturesRequest struct {

	// Features is the list of features to obtain from Feast. Each feature must be given by its fully qualified ID,
	// in the format featureSet:version:featureName.
	Features      []string

	// Entities is the list of entity rows to retrieve features on. Each row is a map of entity name to entity value.
	Entities      []Row
}

// Builds the feast-specified request payload from the wrapper.
func (r OnlineFeaturesRequest) buildRequest() (*serving.GetOnlineFeaturesRequest, error) {
	featureSets, err := buildFeatureSets(r.Features)
	if err != nil {
		return nil, err
	}

	entityRows := make([]*serving.GetOnlineFeaturesRequest_EntityRow, len(r.Entities))

	for i := range r.Entities {
		entityRows[i] = &serving.GetOnlineFeaturesRequest_EntityRow{
			Fields:          r.Entities[i],
		}
	}
	return &serving.GetOnlineFeaturesRequest{
		FeatureSets: featureSets,
		EntityRows:  entityRows,
	}, nil
}

func buildFeatureSets(features []string) ([]*serving.GetOnlineFeaturesRequest_FeatureSet, error) {
	featureSetMap := map[string]*serving.GetOnlineFeaturesRequest_FeatureSet{}
	for _, feature := range features {
		split := strings.Split(feature, ":")
		if len(split) != 3 {
			return nil, fmt.Errorf(ErrInvalidFeatureName, feature)
		}
		featureSetName, featureSetVersion, featureName := split[0], split[1], split[2]
		key := featureSetName + ":" + featureSetVersion
		if fs, ok := featureSetMap[key]; !ok {
			version, err := strconv.Atoi(featureSetVersion)
			if err != nil {
				return nil, fmt.Errorf(ErrInvalidFeatureName, feature)
			}
			featureSetMap[key] = &serving.GetOnlineFeaturesRequest_FeatureSet{
				Name:         featureSetName,
				Version:      int32(version),
				FeatureNames: []string{featureName},
			}
		} else {
			fs.FeatureNames = append(fs.GetFeatureNames(), featureName)
		}
	}
	var featureSets []*serving.GetOnlineFeaturesRequest_FeatureSet
	for _, featureSet := range featureSetMap {
		featureSets = append(featureSets, featureSet)
	}
	return featureSets, nil
}
