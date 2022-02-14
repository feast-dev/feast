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
	// "sort"
	"fmt"
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
	
	requestEntities := request.GetEntities()      // map[string]*types.RepeatedValue
	var requestEntitiesRowLength int
	for _, values := range requestEntities {
		requestEntitiesRowLength = len(values.GetVal())
	}

	response := serving.GetOnlineFeaturesResponse{Metadata: &serving.GetOnlineFeaturesResponseMetadata{FeatureNames : &serving.FeatureList{Val : make([]string, 0) }},
		Results: make([]*serving.GetOnlineFeaturesResponse_FeatureVector, requestEntitiesRowLength)}
	for i := 0; i < requestEntitiesRowLength; i++ {
		featureVector := serving.GetOnlineFeaturesResponse_FeatureVector{Values: make([]*types.Value, 0),
			Statuses:        make([]serving.FieldStatus, 0),
			EventTimestamps: make([]*timestamppb.Timestamp, 0)}
		response.Results[i] = &featureVector
	}
	populateResultRowsFromColumnar(&response, requestEntities)

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
		featureMetaData := featureName
		if request.FullFeatureNames {
			featureMetaData = fmt.Sprintf("%s__%s", featureViewName, featureName)
		}
		response.Metadata.FeatureNames.Val = append(response.Metadata.FeatureNames.Val, featureMetaData)
	}

	// Construct a map of all feature_views to validate later
	registryFeatureViews := fs.registry.GetFeatureViews()
	featureViewsInRegistry := make(map[string]*core.FeatureView)
	for _, registryFeatureView := range registryFeatureViews {
		featureViewsInRegistry[registryFeatureView.Spec.Name] = registryFeatureView
	}

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
		entitiesInFeatureView := featureViewSpec.GetEntities()
		featuresInFeatureView := featureViewSpec.GetFeatures()
		// Validate that all features asked for are inside this feature view
		featuresInFeatureViewMap := make(map[string]bool)
		for _, featureRef := range featuresInFeatureView {
			featuresInFeatureViewMap[featureRef.GetName()] = true
		}

		for _, featureName := range allFeatures {
			if _, ok := featuresInFeatureViewMap[featureName]; !ok {
				return nil, errors.New(fmt.Sprintf("FeatureView: %s doesn't contain feature: %s\n", featureViewName, featureName))
			}
		}

		var entityKeys []types.EntityKey
		// Construct EntityKeys
		if len(entitiesInFeatureView) > 0 {

			if _, ok := requestEntities[entitiesInFeatureView[0]]; !ok {
				return nil, errors.New(fmt.Sprintf("EntityKey: %s is required for feature view: %s\n", entitiesInFeatureView[0], featureViewName))
			}
			
			numJoinKeysInFeatureView := len(entitiesInFeatureView)
			entityKeys = make([]types.EntityKey, requestEntitiesRowLength)
			for index, _ := range entityKeys {
				entityKeys[index] = types.EntityKey{	JoinKeys: make([]string, numJoinKeysInFeatureView),
									EntityValues: make([]*types.Value, numJoinKeysInFeatureView)}
			}
			// Building entity keys for required for each Feature View from the Feature View's Spec
			for joinKeyIndex, joinKey := range entitiesInFeatureView {
				if values, ok := requestEntities[joinKey]; !ok {
					return nil, errors.New(fmt.Sprintf("EntityKey: %s is required for feature view: %s\n", joinKey, featureViewName))
				} else {
					// All requested entities must have the same number of rows
					if len(values.GetVal()) != requestEntitiesRowLength {
						return nil, errors.New("Values of each Entity must have the same length")
					}
					for rowEntityKeyIndex, value := range values.GetVal() {
						entityKeys[rowEntityKeyIndex].JoinKeys[joinKeyIndex] = joinKey
						entityKeys[rowEntityKeyIndex].EntityValues[joinKeyIndex] = value
					}
				}
			}
			
		}
			
		features, err := fs.onlineStore.OnlineRead(entityKeys, featureViewName, allFeatures)

		if err != nil {
			return nil, err
		}

		for rowIndex, featureList := range features {
			featureVector := response.Results[rowIndex]
			for _, feature := range featureList {
				status := serving.FieldStatus_PRESENT

				if _, ok := feature.value.Val.(*types.Value_NullVal); ok {
					status = serving.FieldStatus_NULL_VALUE
				} else if checkOutsideMaxAge(&feature.timestamp, timestamppb.Now(), featureViewSpec.GetTtl()) {
					status = serving.FieldStatus_OUTSIDE_MAX_AGE
				}
				value := feature.value
				timeStamp := feature.timestamp
				featureVector.Values = append(featureVector.Values, &value)
				featureVector.Statuses = append(featureVector.Statuses, status)
				featureVector.EventTimestamps = append(featureVector.EventTimestamps, &timeStamp)

			}
			
		}
	}
	return &response, nil
}

func checkOutsideMaxAge(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	return currentTimestamp.GetSeconds()-featureTimestamp.GetSeconds() > ttl.Seconds
}

func populateResultRowsFromColumnar(response *serving.GetOnlineFeaturesResponse, data map[string]*types.RepeatedValue) {
	timeStamp := timestamppb.Now()
	for entityName, values := range data {
		response.Metadata.FeatureNames.Val = append(response.Metadata.FeatureNames.Val, entityName)
		
		for rowIndex, value := range values.GetVal() {
			featureVector := response.Results[rowIndex]
			featureTimeStamp := *timeStamp
			featureValue := *value
			featureVector.Values = append(featureVector.Values, &featureValue)
			featureVector.Statuses = append(featureVector.Statuses, serving.FieldStatus_PRESENT)
			featureVector.EventTimestamps = append(featureVector.EventTimestamps, &featureTimeStamp)
		}
	}
}

func (fs *FeatureStore) DestructOnlineStore() {
	fs.onlineStore.Destruct()
}