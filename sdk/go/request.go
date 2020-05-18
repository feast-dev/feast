package feast

import (
	"fmt"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"strings"
)

var (
	// ErrInvalidFeatureRef indicates that the user has provided a feature reference
	// with the wrong structure or contents
	ErrInvalidFeatureRef = "Invalid Feature Reference %s provided, " +
		"feature reference must be in the format [featureset:]name"
)

// OnlineFeaturesRequest wrapper on feast.serving.GetOnlineFeaturesRequest.
type OnlineFeaturesRequest struct {
	// Features is the list of features to obtain from Feast. Each feature can be given as
	// the format feature_set:feature, where "feature_set" & "feature" are feature set name
	// and feature name respectively. The only required components is feature name.
	Features []string

	// Entities is the list of entity rows to retrieve features on. Each row is a map of entity name to entity value.
	Entities []Row

	// Project specifies the project would contain the feature sets where the requested features belong to.
	Project string
}

// Builds the feast-specified request payload from the wrapper.
func (r OnlineFeaturesRequest) buildRequest() (*serving.GetOnlineFeaturesRequest, error) {
	featureRefs, err := buildFeatureRefs(r.Features, r.Project)
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
		Features:   featureRefs,
		EntityRows: entityRows,
	}, nil
}

// Creates a slice of FeatureReferences from string representation in
// the format featureset:feature.
// featureRefStrs - string feature references to parse.
// project - Optionally sets the project in parsed FeatureReferences. Otherwise pass ""
// Returns parsed FeatureReferences.
// Returns an error when the format of the string feature reference is invalid
func buildFeatureRefs(featureRefStrs []string, project string) ([]*serving.FeatureReference, error) {
	var featureRefs []*serving.FeatureReference

	for _, featureRefStr := range featureRefStrs {
		featureRef, err := parseFeatureRef(featureRefStr, false)
		if err != nil {
			return nil, err
		}
		// apply project if specified
		if len(project) != 0 {
			featureRef.Project = project
		}
		featureRefs = append(featureRefs, featureRef)
	}
	return featureRefs, nil
}

// Parses a string FeatureReference into FeatureReference proto
// featureRefStr - the string feature reference to parse.
// ignoreProject - whether to ignore project in string
// Returns parsed FeatureReference.
// Returns an error when the format of the string feature reference is invalid
func parseFeatureRef(featureRefStr string, ignoreProject bool) (*serving.FeatureReference, error) {
	if len(featureRefStr) == 0 {
		return nil, fmt.Errorf(ErrInvalidFeatureRef, featureRefStr)
	}

	var featureRef serving.FeatureReference
	if strings.Contains(featureRefStr, "/") {
		if ignoreProject {
			projectSplit := strings.Split(featureRefStr, "/")
			featureRefStr = projectSplit[1]
		} else {
			return nil, fmt.Errorf(ErrInvalidFeatureRef, featureRefStr)
		}
	}
	// parse featureset if specified
	if strings.Contains(featureRefStr, ":") {
		refSplit := strings.Split(featureRefStr, ":")
		featureRef.FeatureSetName, featureRefStr = refSplit[0], refSplit[1]
	}
	featureRef.Name = featureRefStr

	return &featureRef, nil
}

// Renders a FeatureReference proto into a string
// featureRef - The FeatureReference to render as string
// Returns parsed FeatureReference.
// Returns an error when the format of the string feature reference is invalid
func renderFeatureRef(featureRef *serving.FeatureReference) string {
	refStr := ""
	// In protov3, unset string and default to ""
	if len(featureRef.FeatureSetName) > 0 {
		refStr += featureRef.FeatureSetName + ":"
	}
	refStr += featureRef.Name

	return refStr
}
