package feast

import (
	"fmt"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
	"strings"
)

var (
	// ErrInvalidFeatureRef indicates that the user has provided a feature reference
	// with the wrong structure or contents
	ErrInvalidFeatureRef = "Invalid Feature Reference %s provided, " +
		"feature reference must be in the format featureTableName:featureName"
)

// OnlineFeaturesRequest wrapper on feast.serving.GetOnlineFeaturesRequestV2.
type OnlineFeaturesRequest struct {
	// Features is the list of features to obtain from Feast. Each feature can be given as
	// the format feature_table:feature, where "feature_table" & "feature" are feature table name
	// and feature name respectively. The only required components is feature name.
	Features []string

	// Entities is the list of entity rows to retrieve features on. Each row is a map of entity name to entity value.
	Entities []Row

	// Project optionally specifies the project override. If specified, uses given project for retrieval.
	// Overrides the projects specified in Feature References if also specified.
	Project string
}

// Builds the feast-specified request payload from the wrapper.
func (r OnlineFeaturesRequest) buildRequest() (*serving.GetOnlineFeaturesRequest, error) {
	_, err := buildFeatureRefs(r.Features)
	if err != nil {
		return nil, err
	}
	if len(r.Entities) == 0 {
		return nil, fmt.Errorf("Entities must be provided")
	}

	firstRow := r.Entities[0]
	columnSize := len(firstRow)

	// build request entity rows from native entities
	entityColumns := make(map[string][]*types.Value, columnSize)
	for rowIdx, entityRow := range r.Entities {
		for name, val := range entityRow {
			if _, ok := entityColumns[name]; !ok {
				entityColumns[name] = make([]*types.Value, len(r.Entities))
			}

			entityColumns[name][rowIdx] = val
		}
	}

	entities := make(map[string]*types.RepeatedValue, len(entityColumns))
	for column, values := range entityColumns {
		entities[column] = &types.RepeatedValue{
			Val: values,
		}
	}

	return &serving.GetOnlineFeaturesRequest{
		Kind: &serving.GetOnlineFeaturesRequest_Features{
			Features: &serving.FeatureList{
				Val: r.Features,
			},
		},
		Entities: entities,
	}, nil
}

// Creates a slice of FeatureReferences from string representation in
// the format featuretable:feature.
// featureRefStrs - string feature references to parse.
// Returns parsed FeatureReferences.
// Returns an error when the format of the string feature reference is invalid
func buildFeatureRefs(featureRefStrs []string) ([]*serving.FeatureReferenceV2, error) {
	var featureRefs []*serving.FeatureReferenceV2

	for _, featureRefStr := range featureRefStrs {
		featureRef, err := parseFeatureRef(featureRefStr)
		if err != nil {
			return nil, err
		}
		featureRefs = append(featureRefs, featureRef)
	}
	return featureRefs, nil
}

// Parses a string FeatureReference into FeatureReference proto
// featureRefStr - the string feature reference to parse.
// Returns parsed FeatureReference.
// Returns an error when the format of the string feature reference is invalid
func parseFeatureRef(featureRefStr string) (*serving.FeatureReferenceV2, error) {
	if len(featureRefStr) == 0 {
		return nil, fmt.Errorf(ErrInvalidFeatureRef, featureRefStr)
	}

	var featureRef serving.FeatureReferenceV2
	if strings.Contains(featureRefStr, "/") || !strings.Contains(featureRefStr, ":") {
		return nil, fmt.Errorf(ErrInvalidFeatureRef, featureRefStr)
	}
	// parse featuretable if specified
	if strings.Contains(featureRefStr, ":") {
		refSplit := strings.Split(featureRefStr, ":")
		featureRef.FeatureViewName, featureRefStr = refSplit[0], refSplit[1]
	}
	featureRef.FeatureName = featureRefStr

	return &featureRef, nil
}
