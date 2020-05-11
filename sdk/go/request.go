package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"strings"
)

var (
	// ErrInvalidFeatureName indicates that the user has provided a feature reference with the wrong structure or contents
	ErrInvalidFeatureName = "invalid feature references %s provided, feature names must be in the format <project>/<feature>"
)

// OnlineFeaturesRequest wrapper on feast.serving.GetOnlineFeaturesRequest.
type OnlineFeaturesRequest struct {
	// Features is the list of features to obtain from Feast. Each feature can be given as
	// <feature-name>
	// <project-name>/<feature-name>
	// The only required components are the feature name and project.
	Features []string

	// Entities is the list of entity rows to retrieve features on. Each row is a map of entity name to entity value.
	Entities []Row

	// Project is the default project to use when looking up features. This is only used when a project is not found
	// within the feature id.
	Project string
}

// Builds the feast-specified request payload from the wrapper.
func (r OnlineFeaturesRequest) buildRequest() (*serving.GetOnlineFeaturesRequest, error) {
	features, err := buildFeatures(r.Features, r.Project)
	if err != nil {
		return nil, err
	}

	entityRows := make([]*serving.GetOnlineFeaturesRequest_EntityRow, len(r.Entities))

	for i := range r.Entities {
		entityRows[i] = &serving.GetOnlineFeaturesRequest_EntityRow{
			Fields: r.Entities[i],
		}
	}
	return &serving.GetOnlineFeaturesRequest{
		Features:   features,
		EntityRows: entityRows,
	}, nil
}

// buildFeatures create a slice of FeatureReferences from a slice of "<project>/<feature_name>"
// It returns an error when the format is invalid
func buildFeatures(featureReferences []string, defaultProject string) ([]*serving.FeatureReference, error) {
	var features []*serving.FeatureReference

	for _, featureRef := range featureReferences {
		var project string
		var name string

		projectSplit := strings.Split(featureRef, "/")

		if len(projectSplit) == 2 {
			project = projectSplit[0]
			name = projectSplit[1]
		} else if len(projectSplit) == 1 {
			project = defaultProject
			name = projectSplit[0]
		} else {
			return nil, fmt.Errorf(ErrInvalidFeatureName, featureRef)
		}

		if project == "" || name == "" {
			return nil, fmt.Errorf(ErrInvalidFeatureName, featureRef)
		}

		features = append(features, &serving.FeatureReference{
			Name:    name,
			Project: project,
		})
	}

	return features, nil
}
