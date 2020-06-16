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

	// whether to omit the entities fields in the response.
	OmitEntities bool
}

// Builds the feast-specified request payload from the wrapper.
func (r OnlineFeaturesRequest) buildRequest() (*serving.GetOnlineFeaturesRequest, error) {
	featureRefs, err := buildFeatureRefs(r.Features, r.Project)
	if err != nil {
		return nil, err
	}

	// build request entity rows from native entities
	entityRows := make([]*serving.GetOnlineFeaturesRequest_EntityRow, len(r.Entities))
	for i, entity := range r.Entities {
		entityRows[i] = &serving.GetOnlineFeaturesRequest_EntityRow{
			Fields: entity,
		}
	}

	return &serving.GetOnlineFeaturesRequest{
		Features:                  featureRefs,
		EntityRows:                entityRows,
		OmitEntitiesInResponse:    r.OmitEntities,
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
// ignoreProject - if true would ignore if project is specified in the given featureRefStr
// 				   Otherwise, would return an error if project is detected in featureRefStr.
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
		featureRef.FeatureSet, featureRefStr = refSplit[0], refSplit[1]
	}
	featureRef.Name = featureRefStr

	return &featureRef, nil
}

// Converts a FeatureReference proto into a string
// featureRef - The FeatureReference to render as string
// Returns string representation of the given FeatureReference
func toFeatureRefStr(featureRef *serving.FeatureReference) string {
	refStr := ""
	// In protov3, unset string and default to ""
	if len(featureRef.FeatureSet) > 0 {
		refStr += featureRef.FeatureSet + ":"
	}
	refStr += featureRef.Name

	return refStr
}
