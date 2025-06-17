package model

import "github.com/feast-dev/feast/go/protos/feast/types"

/*
We group all features from a single request by entities they attached to.
Thus, we will be able to call online range retrieval per entity and not per each feature View.
In this struct we collect all features and views that belongs to a group.
We store here projected entity keys (only ones that needed to retrieve these features)
and indexes to map result of retrieval into output response.
We also store range query parameters like sort key filters, reverse sort order and limit.
*/
type GroupedRangeFeatureRefs struct {
	// A list of requested feature references of the form featureViewName:featureName that share this entity set
	FeatureNames     []string
	FeatureViewNames []string
	// full feature references as they supposed to appear in response
	AliasedFeatureNames []string
	// Entity set as a list of EntityKeys to pass to OnlineReadRange
	EntityKeys []*types.EntityKey
	// Reversed mapping to project result of retrieval from storage to response
	Indices [][]int

	// Sort key filters to pass to OnlineReadRange
	SortKeyFilters []*SortKeyFilter
	// Limit to pass to OnlineReadRange
	Limit int32
	// Reverse sort order to pass to OnlineReadRange
	IsReverseSortOrder bool
}
