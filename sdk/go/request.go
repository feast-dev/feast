package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"strconv"
	"strings"
)

var (
	ErrInvalidFeatureName       = "invalid feature name %s provided, feature names must be in the format featureSet:version:featureName"
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

// buildFeatureSets create a slice of FeatureSetRequest object from
// a slice of "feature_set:version:feature_name" string.
//
// It returns an error when "feature_set:version:feature_name" string
// has an invalid format.
func buildFeatureSets(features []string) ([]*serving.FeatureSetRequest, error) {
	var requests []*serving.FeatureSetRequest

	// Map of "feature_set_name:version" to "FeatureSetRequest" pointer
	// to reference existing FeatureSetRequest, if any.
	fsNameVersionToRequest := make(map[string]*serving.FeatureSetRequest)

	for _, feature := range features {
		splits := strings.Split(feature, ":")
		if len(splits) != 3 {
			return nil, fmt.Errorf(ErrInvalidFeatureName, feature)
		}

		featureSetName, featureSetVersionString, featureName := splits[0], splits[1], splits[2]
		featureSetVersion, err := strconv.Atoi(featureSetVersionString)
		if err != nil {
			return nil, fmt.Errorf(ErrInvalidFeatureName, feature)
		}

		fsNameVersion := featureSetName + ":" + featureSetVersionString
		if request, ok := fsNameVersionToRequest[fsNameVersion]; !ok {
			request = &serving.FeatureSetRequest{
				Name:         featureSetName,
				Version:      int32(featureSetVersion),
				FeatureNames: []string{featureName},
			}
			fsNameVersionToRequest[fsNameVersion] = request
			// Adding FeatureSetRequest in this step ensures the order of
			// FeatureSetRequest in the slice follows the order of feature sets
			// in the "features" argument in buildFeatureSets method.
			requests = append(requests, request)
		} else {
			request.FeatureNames = append(request.FeatureNames, featureName)
		}
	}

	return requests, nil
}