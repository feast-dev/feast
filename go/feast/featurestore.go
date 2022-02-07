package feast

import (
	"errors"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"strings"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"io/ioutil"
	"github.com/golang/protobuf/proto"
)

type FeatureStore struct {
	config      *RepoConfig
	registry    *core.Registry
	onlineStore OnlineStore
}

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config *RepoConfig) (*FeatureStore, error) {
	onlineStore, err := getOnlineStore(config)
	if err != nil {
		return nil, err
	}
	// Read the existing address book.
	in, err := ioutil.ReadFile(config.Registry)
	if err != nil {
		return nil, err
	}
	retristry := &core.Registry{}
	if err := proto.Unmarshal(in, retristry); err != nil {
		return nil, err
	}

	return &FeatureStore{
		config:      config,
		registry:    retristry, // TODO: not implemented
		onlineStore: onlineStore,
	}, nil
}

// GetOnlineFeatures retrieves the latest online feature data
func (fs *FeatureStore) GetOnlineFeatures(request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {
	featureList := request.GetFeatures()
	featureListVal := featureList.GetVal()
	all_features_per_feature_view := make(map[string][]string)
	for _, feature_name := range featureListVal {
		parsed_feature_name := strings.Split(feature_name, ":")
		if len(parsed_feature_name) < 2 {
			return nil, errors.New("FeatureName should be in the format: 'FeatureViewName:FeatureName'")
		}
		featureViewName := parsed_feature_name[0]
		featureName := parsed_feature_name[1]
		if _, ok := all_features_per_feature_view[featureViewName]; !ok {
			all_features_per_feature_view[featureViewName] = make([]string, 0)
		}
		features := all_features_per_feature_view[featureViewName]
		all_features_per_feature_view[featureViewName] = append(features, featureName)
	}
	
	request_entities := request.GetEntities() // map[string]*types.RepeatedValue
	registry_entities := fs.registry.GetEntities() //[]*Entity
	entities_in_registry := make(map[string]bool) // used for validation of requested entities versus registry entities
	entity_keys_lookup := make(map[string]*types.EntityKey)
	
	for _, registry_entity := range registry_entities {
		// append(entities_in_registry, registry_entity.Spec.Name)
		entities_in_registry[registry_entity.Spec.Name] = true
		// An entity in registry is found in request entity
		// Construct entityKey and add entityKey to entityKeys, the first param of OnlineRead
		if request_entity_values, ok := request_entities[registry_entity.Spec.Name]; ok {
			entityKey := types.EntityKey{	JoinKeys: []string{registry_entity.GetSpec().GetJoinKey()},
											EntityValues: request_entity_values.GetVal()}
			entity_keys_lookup[registry_entity.Spec.Name] = &entityKey
		}
	}
	// Validate that all entities in request_entities are found in registry
	for entity_name, _ := range request_entities {
		if _, ok := entities_in_registry[entity_name]; !ok {
			return nil, errors.New("Requested entity not found inside the registry")
		}
	}
	// Construct a map of all feature_views to validate later
	registry_feature_views := fs.registry.GetFeatureViews()
	feature_views_in_registry := make(map[string]*core.FeatureView)
	for _, registry_feature_view := range registry_feature_views {
		feature_views_in_registry[registry_feature_view.Spec.Name] = registry_feature_view
	}

	response := serving.GetOnlineFeaturesResponse{	Metadata: &serving.GetOnlineFeaturesResponseMetadata{FeatureNames: featureList},
													Results: make([]*serving.GetOnlineFeaturesResponse_FeatureVector, 0) }

	for feature_view_name, all_features := range all_features_per_feature_view {
		// Validate that all requested feature view exists inside registry
		if _, ok := feature_views_in_registry[feature_view_name]; !ok {
			return nil, errors.New("Requested feature_view not found inside the registry")
		}
		
		feature_view := feature_views_in_registry[feature_view_name]
		feature_view_spec := feature_view.GetSpec()
		// Obtain all join keys required by this feature view
		// and for each join key, create a EntityKey
		// and add to entity_keys
		joinKeys := feature_view_spec.GetEntities()
		entity_keys := make([]types.EntityKey, 0)
		for _, entity_name := range joinKeys {
			
			if _, ok := entity_keys_lookup[entity_name]; !ok {
				// When does this case happen ???
				// User wants a subset of feature_view which doesn't
				// contain the entity but the entity is a part of the feature_view?
				return nil, errors.New("Entity required for feature_view but not provided")
			}
			entity_keys = append(entity_keys, *entity_keys_lookup[entity_name])
		}
		features, err := fs.onlineStore.OnlineRead(entity_keys, feature_view_name, all_features)

		if err != nil {
			return nil, err
		}

		feature_vector := serving.GetOnlineFeaturesResponse_FeatureVector{	Values: make([]*types.Value, 0),
																			Statuses: make([]serving.FieldStatus, 0),
																			EventTimestamps: make([]*timestamppb.Timestamp, 0) }
		for _, feature_list := range features {
			
			for _, feature := range feature_list {
				status := serving.FieldStatus_PRESENT
				
				// if feature == nil {
				// 	status = serving.FieldStatus_NOT_FOUND
				// 	feature_vector.Values = append(feature_vector.Values, nil)
				// 	feature_vector.Statuses = append(feature_vector.Statuses, status)
				// 	feature_vector.EventTimestamps = append(feature_vector.EventTimestamps, nil)
				// 	continue
				// } else if feature.value == nil {
				// 	status = serving.FieldStatus_NULL_VALUE
				// 	feature_vector.Values = append(feature_vector.Values, nil)
				// 	feature_vector.Statuses = append(feature_vector.Statuses, status)
					
				// 	if feature.timestamp != nil {
				// 		feature_vector.EventTimestamps = append(feature_vector.EventTimestamps, timestamp.Timestamp(&feature.timestamp))
				// 	} else {
				// 		feature_vector.EventTimestamps = append(feature_vector.EventTimestamps, nil)
				// 	}
				// 	continue
				// } else if checkOutsideMaxAge(feature.EventTimestamps, timestamppb.Now(), feature_view_spec.GetTtl()) {
				// 	status = serving.FieldStatus_OUTSIDE_MAX_AGE
				// }

				feature_vector.Values = append(feature_vector.Values, &feature.value)
				feature_vector.Statuses = append(feature_vector.Statuses, status)
				feature_vector.EventTimestamps = append(feature_vector.EventTimestamps, &feature.timestamp)
				
			}
			response.Results = append(response.Results, &feature_vector)
		}
	}
	

	return &response, nil
}

func checkOutsideMaxAge(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	return currentTimestamp.GetSeconds() - featureTimestamp.GetSeconds() > ttl.Seconds
}
