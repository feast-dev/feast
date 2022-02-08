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
	registry := &core.Registry{}
	if err := proto.Unmarshal(in, registry); err != nil {
		return nil, err
	}

	return &FeatureStore{
		config:      config,
		registry:    registry,
		onlineStore: onlineStore,
	}, nil
}

// GetOnlineFeatures retrieves the latest online feature data
func (fs *FeatureStore) GetOnlineFeatures(request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {
	featureList := request.GetFeatures()
	featureListVal := featureList.GetVal()
	allFeaturesPerFeatureView := make(map[string][]string)
	for _, featureName := range featureListVal {
		parsedFeatureName := strings.Split(featureName, ":")
		if len(parsedFeatureName) < 2 {
			return nil, errors.New("FeatureName should be in the format: 'FeatureViewName:FeatureName'")
		}
		featureViewName := parsedFeatureName[0]
		featureName := parsedFeatureName[1]
		if _, ok := allFeaturesPerFeatureView[featureViewName]; !ok {
			allFeaturesPerFeatureView[featureViewName] = make([]string, 0)
		}
		features := allFeaturesPerFeatureView[featureViewName]
		allFeaturesPerFeatureView[featureViewName] = append(features, featureName)
	}

	requestEntities := request.GetEntities()      // map[string]*types.RepeatedValue
	registryEntities := fs.registry.GetEntities() //[]*Entity
	entitiesInRegistry := make(map[string]bool)   // used for validation of requested entities versus registry entities
	var requestEntitiesRowLength int
	
	for _, values := range requestEntities {
		requestEntitiesRowLength = len(values.GetVal())
		break
	}
	for _, registryEntity := range registryEntities {
		// append(entities_in_registry, registry_entity.Spec.Name)
		entitiesInRegistry[registryEntity.Spec.Name] = true
	}
	joinKeyIndex := 0
	joinKeyToIndex := make(map[string]int)
	// Validate that all entities in request_entities are found in registry
	for entityName, values := range requestEntities {
		if _, ok := entitiesInRegistry[entityName]; !ok {
			return nil, errors.New("Requested entity not found inside the registry")
		}
		if len(values.GetVal()) != requestEntitiesRowLength {
			return nil, errors.New("Values of each Entity must have the same length")
		}
		joinKeyToIndex[entityName] = joinKeyIndex
		joinKeyIndex += 1
	}
	// Construct a map of all feature_views to validate later
	registryFeatureViews := fs.registry.GetFeatureViews()
	featureViewsInRegistry := make(map[string]*core.FeatureView)
	for _, registryFeatureView := range registryFeatureViews {
		featureViewsInRegistry[registryFeatureView.Spec.Name] = registryFeatureView
	}
	numRequestJoinKeys := len(requestEntities)
	entityKeys := make([]types.EntityKey, requestEntitiesRowLength)
	for index, _ := range entityKeys {
		entityKey := types.EntityKey{	JoinKeys: make([]string, numRequestJoinKeys),
										EntityValues: make([]*types.Value, numRequestJoinKeys)}
		entityKeys[index] = entityKey
	}
	// Building entity keys
	for joinKey, values := range requestEntities {
		for rowEntityKeyIndex, value := range values.GetVal() {
			joinKeyIndex := joinKeyToIndex[joinKey]
			entityKeys[rowEntityKeyIndex].JoinKeys[joinKeyIndex] = joinKey
			entityKeys[rowEntityKeyIndex].EntityValues[joinKeyIndex] = value
		}
	}

	response := serving.GetOnlineFeaturesResponse{Metadata: &serving.GetOnlineFeaturesResponseMetadata{FeatureNames: featureList},
		Results: make([]*serving.GetOnlineFeaturesResponse_FeatureVector, 0)}

	for featureViewName, allFeatures := range allFeaturesPerFeatureView {
		// Validate that all requested feature view exists inside registry
		if _, ok := featureViewsInRegistry[featureViewName]; !ok {
			return nil, errors.New("Requested featureView not found inside the registry")
		}

		featureView := featureViewsInRegistry[featureViewName]
		featureViewSpec := featureView.GetSpec()
		// Obtain all join keys required by this feature view
		// and for each join key, create a EntityKey
		// and add to entity_keys
		entitiesRequired := featureViewSpec.GetEntities()
		for _, entityName := range entitiesRequired {
			if _, ok := requestEntities[entityName]; !ok {
				return nil, errors.New("All entities inside FeatureView must be provided")
			}
		}
		
		features, err := fs.onlineStore.OnlineRead(entityKeys, featureViewName, allFeatures)

		if err != nil {
			return nil, err
		}

		featureVector := serving.GetOnlineFeaturesResponse_FeatureVector{Values: make([]*types.Value, 0),
			Statuses:        make([]serving.FieldStatus, 0),
			EventTimestamps: make([]*timestamppb.Timestamp, 0)}
		for _, featureList := range features {

			for _, feature := range featureList {
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

				featureVector.Values = append(featureVector.Values, &feature.value)
				featureVector.Statuses = append(featureVector.Statuses, status)
				featureVector.EventTimestamps = append(featureVector.EventTimestamps, &feature.timestamp)

			}
			response.Results = append(response.Results, &featureVector)
		}
	}

	return &response, nil
}

func checkOutsideMaxAge(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	return currentTimestamp.GetSeconds()-featureTimestamp.GetSeconds() > ttl.Seconds
}
