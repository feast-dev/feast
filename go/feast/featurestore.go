package feast

import (
	"errors"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"sort"
	"fmt"
	"log"
)

type FeatureStore struct {
	config      *RepoConfig
	registry    *Registry // TODO (Ly): change this to a Registry Object so we can add cache
	onlineStore OnlineStore
}

type entityKeyRow struct {
	entityKey *types.EntityKey
	rowIndices []int
}

const (
	DUMMY_ENTITY_ID = "__dummy_id"
	DUMMY_ENTITY_NAME = "__dummy"
	DUMMY_ENTITY_VAL = ""
)

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config *RepoConfig) (*FeatureStore, error) {
	onlineStore, err := getOnlineStore(config)
	if err != nil {
		return nil, err
	}
	// registry, err := NewRegistry(config.Registry["path"].(string))
	
	registry, err := NewRegistry(config.getRegistryPath())
	if err != nil {
		return nil, err
	}
	return &FeatureStore{
		config:      config,
		registry:    registry,
		onlineStore: onlineStore,
	}, nil
}

/*
Kind is either FeatureService or a list of FeatureView:Feature
FeatureTable not supported since it's not in Python sdk
*/

// GetOnlineFeatures retrieves the latest online feature data
// Ignore nativeEntityValues
func (fs *FeatureStore) GetOnlineFeatures(request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {

	// TODO (Ly): Remove hackathon code with
	// similar function calls as in python's sdk
	kind := request.GetKind()
	fullFeatureNames := request.GetFullFeatureNames()
	parsedKind, err := fs.parseKind(kind)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	featureRefs, err := fs.getFeatures(parsedKind)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews, err := fs.getFeatureViewsToUse(parsedKind)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	entityNameToJoinKeyMap, err := fs.getEntityMaps(requestedFeatureViews)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	entityProtos := request.GetEntities()
	numRows, err := fs.validateEntityValues(entityProtos)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	err = fs.validateFeatureRefs(featureRefs, fullFeatureNames)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	groupedRefs, groupedOdfvRefs, groupedRequestFvRefs, err := groupFeatureRefs(featureRefs, requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	requestedResultRowNames := make(map[string]bool)
	if fullFeatureNames {
		for _, featureReference := range featureRefs {
			if !strings.Contains(featureReference, ":") {
				log.Fatalln(err)
				return nil, errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
			}
			requestedResultRowNames[strings.Replace(featureReference, ":", "__", 1)] = true
		}
	} else {
		for _, featureReference := range featureRefs {
			parsedFeatureName := strings.Split(featureReference, ":")
			var featureName string
			if len(parsedFeatureName) == 0 {
				log.Fatalln(err)
				return nil, errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
			} else if len(parsedFeatureName) == 1 {
				featureName = parsedFeatureName[0]
			} else {
				featureName = parsedFeatureName[1]
			}
			requestedResultRowNames[featureName] = true
		}
	}

	featureViews := make([]*FeatureView, len(groupedRefs))
	index := 0
	for featureView, _ := range groupedRefs {
		featureViews[index] = featureView
		index += 1
	}

	neededRequestData, neededRequestFvFeatures, err := fs.getNeededRequestData(groupedOdfvRefs, groupedRequestFvRefs)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	joinKeyValues := make(map[string]*types.RepeatedValue)
	requestDataFeatures := make(map[string]*types.RepeatedValue)
	for entityName, vals := range entityProtos {
		if _, ok := neededRequestFvFeatures[entityName]; ok {
			requestedResultRowNames[entityName] = true
			requestDataFeatures[entityName] = vals
		} else if _, ok = neededRequestData[entityName]; ok {
			requestDataFeatures[entityName] = vals
		} else {
			if joinKey, ok := entityNameToJoinKeyMap[entityName]; !ok {
				return nil, errors.New(fmt.Sprintf("EntityNotFoundException: %s", entityName))
			} else {
				joinKeyValues[joinKey] = vals
				requestedResultRowNames[joinKey] = true
			}
		}

	}

	// TODO (Ly): Skip this validation
	// since we're not supporting ODFV yet

	// err = fs.ensureRequestedDataExist(neededRequestData, neededRequestFvFeatures, requestDataFeatures)
	// if err != nil {
	// 	return nil, err
	// }

	onlineFeatureResponse := &serving.GetOnlineFeaturesResponse	{	Metadata: &serving.GetOnlineFeaturesResponseMetadata	{
																				FeatureNames: &serving.FeatureList{Val: make([]string, 0)},
																			},
																	Results: make([]*serving.GetOnlineFeaturesResponse_FeatureVector, numRows),	
																}
	// Allocate memory for each GetOnlineFeaturesResponse_FeatureVector
	for index = 0; index < numRows; index++ {
		onlineFeatureResponse.Results[index] = &serving.GetOnlineFeaturesResponse_FeatureVector	{	Values: make([]*types.Value, 0),
																									Statuses: make([]serving.FieldStatus, 0),
																									EventTimestamps: make([]*timestamppb.Timestamp, 0),
																								}
	}
	// Merge requestDataFeatures into joinKeyValues
	for entityName, vals := range joinKeyValues {
		requestDataFeatures[entityName] = vals
	}

	fs.populateResultRowsFromColumnar(onlineFeatureResponse, requestDataFeatures)

	// TODO (Ly): Review: Skip dummy / entityless case for now. Why?

	for table, requestedFeatures := range groupedRefs {
		tableEntityValues, idxs, err := fs.getUniqueEntities( table, joinKeyValues, entityNameToJoinKeyMap)
		if err != nil {
			log.Fatalln(err)
			return nil, err
		}
		featureData, err := fs.readFromOnlineStore(tableEntityValues, requestedFeatures, table)
		if err != nil {
			log.Fatalln(err)
			return nil, err
		}
		fs.populateResponseFromFeatureData(featureData, idxs, onlineFeatureResponse, fullFeatureNames, requestedFeatures, table)
	}

	// TODO (Ly): ODFV, skip augmentResponseWithOnDemandTransforms
	fs.dropUnneededColumns(onlineFeatureResponse, requestedResultRowNames)
	return onlineFeatureResponse, nil
}

func (fs *FeatureStore) DestructOnlineStore() {
	fs.onlineStore.Destruct()
}

func (fs *FeatureStore) parseKind(kind interface{}) (interface{}, error) {
	if featureList, ok := kind.(*serving.GetOnlineFeaturesRequest_Features); ok {
		return featureList.Features.GetVal(), nil
	}
	if featureService, ok := kind.(*serving.GetOnlineFeaturesRequest_FeatureService); ok {
		featureServiceProto, err := fs.registry.getFeatureService(fs.config.Project, featureService.FeatureService)
		if err != nil {
			return nil, err
		}
		return NewFeatureServiceFromProto(featureServiceProto), nil
	}
	return nil, errors.New("Cannot parse kind from GetOnlineFeaturesRequest")
}

func (fs *FeatureStore) getFeatures(parsedKind interface{}) ([]string, error) {
	
	if features, ok :=  parsedKind.([]string); !ok {
		var featureService *FeatureService
		if featureService, ok = parsedKind.(*FeatureService); !ok {
			return nil, errors.New("Cannot parse FeatureService from request")
		}

		var featureViewName string
		features = make([]string, 0)
		for _, featureProjection := range featureService.projections {
			// TODO (Ly): have a FeatureService class that
			// contains nameToUse() method as in Python's sdk
			featureViewName = featureProjection.nameToUse()
			for _, feature := range featureProjection.features {
				features = append(features, fmt.Sprintf("%s:%s", featureViewName, feature.Name))
			}
		}
		return features, nil
	} else {
		return features, nil
	}
}

// TODO (Ly): Implement allowCache option as in Python's sdk
// and dummy entity?
func (fs *FeatureStore) getFeatureViewsToUse(parsedKind interface{}) ([]*FeatureView, []*RequestFeatureView, []*OnDemandFeatureView, error) {
	
	fvs := make(map[string]*FeatureView)
	featureViewProtos, err := fs.listFeatureViews()
	if err != nil {
		return nil, nil, nil, err
	}
	for _, featureViewProto := range featureViewProtos {
		fmt.Println(featureViewProto.GetSpec().GetName())
		fvs[featureViewProto.Spec.Name] = NewFeatureViewFromProto(featureViewProto)
	}

	requestFvs := make(map[string]*RequestFeatureView)
	requestFeatureViewProtos, err := fs.registry.listRequestFeatureViews(fs.config.Project)
	if err != nil {
		requestFeatureViewProtos = make([]*core.RequestFeatureView, 0)
		// return nil, nil, nil, err
	}
	for _, requestFeatureViewProto := range requestFeatureViewProtos {
		requestFvs[requestFeatureViewProto.Spec.Name] = NewRequestFeatureViewFromProto(requestFeatureViewProto)
	}

	odFvs := make(map[string]*OnDemandFeatureView)
	onDemandFeatureViewProtos, err := fs.registry.listOnDemandFeatureViews(fs.config.Project)
	if err != nil {
		onDemandFeatureViewProtos = make([]*core.OnDemandFeatureView, 0)
		// return nil, nil, nil, err
	}
	for _, onDemandFeatureViewProto := range onDemandFeatureViewProtos {
		odFvs[onDemandFeatureViewProto.Spec.Name] = NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto)
	}

	if featureService, ok := parsedKind.(*FeatureService); ok {
		
		// TODO (Ly): Review: Skip checking featureService from registry since
		// we're only given featureServiceName

		fvsToUse := make([]*FeatureView, 0)
		requestFvsToUse := make([]*RequestFeatureView, 0)
		odFvsToUse := make([]*OnDemandFeatureView, 0)

		for _, featureProjection := range featureService.projections {
			
			// Create copies of FeatureView that may
			// contains the same *core.FeatureView but
			// each differentiated by a *FeatureViewProjection
			featureViewName := featureProjection.name
			if fv, ok := fvs[featureViewName]; ok {
				base, err := fv.base.withProjection(featureProjection)
				if err != nil {
					return nil, nil, nil, err
				}
				fvsToUse = append(fvsToUse, fv.NewFeatureViewFromBase(base))
			} else if requestFv, ok := requestFvs[featureViewName]; ok {
				base, err := requestFv.base.withProjection(featureProjection)
				if err != nil {
					return nil, nil, nil, err
				}
				requestFvsToUse = append(requestFvsToUse, requestFv.NewRequestFeatureViewFromBase(base))
			} else if odFv, ok := odFvs[featureViewName]; ok {
				base, err := odFv.base.withProjection(featureProjection)
				if err != nil {
					return nil, nil, nil, err
				}
				odFvsToUse = append(odFvsToUse, odFv.NewOnDemandFeatureViewFromBase(base))
			} else {
				return nil, nil, nil, errors.New(fmt.Sprintf("The provided feature service %s contains a reference to a feature view" +
				"%s which doesn't exist. Please make sure that you have created the feature view" +
				"%s and that you have registered it by running \"apply\".", featureService.name, featureViewName, featureViewName))
			}
		}
	}

	fvsToUse := make([]*FeatureView, len(fvs))
	requestFvsToUse := make([]*RequestFeatureView, len(requestFvs))
	odFvsToUse := make([]*OnDemandFeatureView, len(odFvs))
	index := 0
	for _, fv := range fvs {
		fvsToUse[index] = fv
		index += 1
	}
	index = 0
	for _, fv := range requestFvs {
		requestFvsToUse[index] = fv
		index += 1
	}
	index = 0
	for _, fv := range odFvs {
		odFvsToUse[index] = fv
		index += 1
	}
	return fvsToUse, requestFvsToUse, odFvsToUse, nil
}

/*
TODO (Ly): Review: that for every method that uses registry
check if that entity / featureView are in this project
*/

func (fs *FeatureStore) getEntityMaps(requestedFeatureViews []*FeatureView) (map[string]string, error) {

	entityNameToJoinKeyMap := make(map[string]string)
	var entityNames []string
	var entityName string
	var joinKeyMap map[string]string
	var featureView *FeatureView

	for _, featureView = range requestedFeatureViews {
		entityNames = featureView.entities
		joinKeyMap = featureView.base.projection.joinKeyMap
		for _, entityName = range entityNames {
			// TODO (Ly): Remove this with fs.registry.getEntity()
			entity, err := fs.registry.getEntity(fs.config.Project, entityName)
			if err != nil {
				return nil, err
			}
			entityName := entity.Spec.Name
			joinKey := entity.Spec.JoinKey

			// TODO (Ly): Review: weird that both uses the same map?
			// from python's sdk
			if entityNameMapped, ok := joinKeyMap[entityName]; ok {
				entityName = entityNameMapped
			}
			if joinKeyMapped, ok := joinKeyMap[joinKey]; ok {
				joinKey = joinKeyMapped
			}
			entityNameToJoinKeyMap[entityName] = joinKey
			// TODO (Ly): Review: Can we skip entity_type_map
			// in go's version?
		}
	}
	return entityNameToJoinKeyMap, nil
}

func (fs *FeatureStore) validateEntityValues(joinKeyValues map[string]*types.RepeatedValue) (int, error) {
	setOfRowLengths := make(map[int]bool)
	var numRows int
	for _, col := range joinKeyValues {
		setOfRowLengths[len(col.Val)] = true
		numRows = len(col.Val)
	}
	if len(setOfRowLengths) > 1 {
		return 0, errors.New("All entity rows must have the same columns.")
	}
	return numRows, nil
}

func (fs *FeatureStore) validateFeatureRefs(featureRefs []string, fullFeatureNames bool) error {
	collidedFeatureRefs := make(map[string]int)
	if fullFeatureNames {
		for _, featureRef := range featureRefs {
			collidedFeatureRefs[featureRef] += 1
			if collidedFeatureRefs[featureRef] > 1 {
				return errors.New(fmt.Sprintf("Collided FeatureRef detected: %s", featureRef))
			}
		}
	} else {
		var featureName string
		for _, featureRef := range featureRefs {
			
			parsedFeatureName := strings.Split(featureRef, ":")
			
			if len(parsedFeatureName) == 0 {
				return errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
			} else if len(parsedFeatureName) == 1 {
				featureName = parsedFeatureName[0]
			} else {
				featureName = parsedFeatureName[1]
			}
			
			collidedFeatureRefs[featureName] += 1

			if collidedFeatureRefs[featureName] > 1 {
				return errors.New(fmt.Sprintf("Collided FeatureName detected: %s", featureName))
			}
		}
	}
	return nil
}

func (fs *FeatureStore) getNeededRequestData(	groupedOdfvRefs map[*OnDemandFeatureView][]string,
												groupedRequestFvRefs map[*RequestFeatureView][]string) (map[string]bool, map[string]bool, error){
	neededRequestData := make(map[string]bool)
	neededRequestFvFeatures := make(map[string]bool)
	// TODO (Ly): Implement getRequestDataSchema in OnDemandFeatureView
	// and convert features from DataSource in RequestFeatureView
	// to complete this function
	return neededRequestData, neededRequestFvFeatures, nil
}

func (fs *FeatureStore) ensureRequestedDataExist(	neededRequestData map[string]bool,
													neededRequestFvFeatures map[string]bool,
													requestDataFeatures map[string]*types.RepeatedValue) error {
	// TODO (Ly): Review: Skip checking even if composite set of
	// neededRequestData neededRequestFvFeatures is different from
	// request_data_features but same length?
	if len(neededRequestData) + len(neededRequestFvFeatures) != len(requestDataFeatures) {
		missingFeatures := make([]string, 0)
		for feature, _ := range neededRequestData {
			if _, ok := requestDataFeatures[feature]; !ok {
				missingFeatures = append(missingFeatures, feature)
			}
		}
		for feature, _ := range neededRequestFvFeatures {
			if _, ok := requestDataFeatures[feature]; !ok {
				missingFeatures = append(missingFeatures, feature)
			}
		}
		return errors.New(fmt.Sprintf("RequestDataNotFoundInEntityRowsException: %s", strings.Join(missingFeatures, ", ")))
	}
	return nil
}

func (fs *FeatureStore) checkOutsideMaxAge(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	return currentTimestamp.GetSeconds()-featureTimestamp.GetSeconds() > ttl.Seconds
}

func (fs *FeatureStore) populateResultRowsFromColumnar(response *serving.GetOnlineFeaturesResponse, data map[string]*types.RepeatedValue) {
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

// TODO (Ly): Test this function
func (fs *FeatureStore) getUniqueEntities	(	table *FeatureView,
												joinKeyValues map[string]*types.RepeatedValue,
												entityNameToJoinKeyMap map[string]string,
											) 	([]*types.EntityKey, [][]int, error) {

	tableEntityValues := fs.getTableEntityValues(table, joinKeyValues, entityNameToJoinKeyMap)
	keys := make([]string, len(tableEntityValues))
	index := 0
	var numRows int
	for k, v := range tableEntityValues {
		keys[index] = k
		index += 1
		numRows = len(v.Val)
	}
	sort.Strings(keys)
	entityKeys := make([]*types.EntityKey, numRows)
	rowise := make(map[string]*entityKeyRow)
	numJoinKeys := len(keys)
	// Construct each EntityKey object
	for index = 0; index < numRows; index++ {
		entityKeys[index] = &types.EntityKey{JoinKeys: keys, EntityValues: make([]*types.Value, numJoinKeys)}
	}

	for colIndex, key := range keys {
		for index, value := range tableEntityValues[key].GetVal() {
			entityKeys[index].EntityValues[colIndex] = value
		}
	}

	for index, entityKey := range entityKeys {
		key, err := SerializeEntityKey(*entityKey)
		if err != nil {
			return nil, nil, err
		}
		if ekRow, ok := rowise[string(*key)]; ok {
			ekRow.rowIndices = append(ekRow.rowIndices, index)
		} else {
			ekRow = &entityKeyRow{entityKey: entityKeys[index], rowIndices: make([]int, 1)}
			rowise[string(*key)] = ekRow
			ekRow.rowIndices[0] = index
		}
	}
	numUniqueRows := len(rowise)
	uniqueEntityKeys := make([]*types.EntityKey, numUniqueRows)
	indices := make([][]int, numUniqueRows)
	index = 0
	for _, ekRow := range rowise {
		uniqueEntityKeys[index] = ekRow.entityKey
		indices[index] = ekRow.rowIndices
		index += 1
	}
	return uniqueEntityKeys, indices, nil
}

func (fs *FeatureStore) getTableEntityValues(	table *FeatureView,
												joinKeyValues map[string]*types.RepeatedValue,
												entityNameToJoinKeyMap map[string]string) map[string]*types.RepeatedValue {
	
	tableJoinKeys := make(map[string]bool)
	for _, enityName := range table.entities {
		tableJoinKeys[entityNameToJoinKeyMap[enityName]] = true
	}

	aliasToJoinKeyMap := make(map[string]string)
	for k,v := range table.base.projection.joinKeyMap {
		aliasToJoinKeyMap[v] = k
	}

	entityValues := make(map[string]*types.RepeatedValue)
	
	for k, v := range joinKeyValues {
		entityKey := k
		if _, ok := aliasToJoinKeyMap[k]; ok {
			entityKey = aliasToJoinKeyMap[k]
		}
		if _, ok := tableJoinKeys[entityKey]; ok {
			entityValues[entityKey] = v
		}
	}

	return entityValues
}

func (fs *FeatureStore) readFromOnlineStore(	entityRows []*types.EntityKey,
												requestedFeatures []string,
												table *FeatureView,
											) 	( [][]Feature, error ) {
	numRows := len(entityRows)
	entityRowsValue := make([]types.EntityKey, numRows)
	for index, entityKey := range entityRows {
		entityRowsValue[index] = *entityKey
	}
	return fs.onlineStore.OnlineRead(entityRowsValue, table.base.name, requestedFeatures)
}

func (fs *FeatureStore) populateResponseFromFeatureData(	featureData2D [][]Feature,
															indexes [][]int,
															onlineFeaturesResponse *serving.GetOnlineFeaturesResponse,
															fullFeatureNames bool,
															requestedFeatures []string,
															table *FeatureView,) {
	
	requestedFeatureRefs := make([]string, len(requestedFeatures))

	for index, featureName := range requestedFeatures {
		if fullFeatureNames {
			requestedFeatureRefs[index] = fmt.Sprintf("%s__%s", table.base.projection.nameToUse(), featureName)
		} else {
			requestedFeatureRefs[index] = featureName
		}
	}
	onlineFeaturesResponse.Metadata.FeatureNames.Val = append(onlineFeaturesResponse.Metadata.FeatureNames.Val, requestedFeatureRefs...)

	for entityIndex, featureList := range featureData2D {
		
		numFeatures := len(featureList)
		values := make([]*types.Value, numFeatures)
		statuses := make([]serving.FieldStatus, numFeatures)
		eventTimeStamps := make([]*timestamppb.Timestamp, numFeatures)

		for index, featureData := range featureList {
			value := featureData.value
			status := serving.FieldStatus_PRESENT
			eventTimeStamp := featureData.timestamp

			if _, ok := value.Val.(*types.Value_NullVal); ok {
				status = serving.FieldStatus_NULL_VALUE
			} else if fs.checkOutsideMaxAge(&eventTimeStamp, timestamppb.Now(), table.ttl ) {
				status = serving.FieldStatus_OUTSIDE_MAX_AGE
			}
			values[index] = &value
			statuses[index] = status
			eventTimeStamps[index] = &eventTimeStamp
		}

		for _, rowIndex := range indexes[entityIndex] {
			onlineFeaturesResponse.Results[rowIndex].Values = append(onlineFeaturesResponse.Results[rowIndex].Values, values...)
			onlineFeaturesResponse.Results[rowIndex].Statuses = append(onlineFeaturesResponse.Results[rowIndex].Statuses, statuses...)
			onlineFeaturesResponse.Results[rowIndex].EventTimestamps = append(onlineFeaturesResponse.Results[rowIndex].EventTimestamps, eventTimeStamps...)
		}
	}

}

// TODO (Ly): Complete this function + ODFV
func (fs *FeatureStore) augmentResponseWithOnDemandTransforms( 	onlineFeaturesResponse *serving.GetOnlineFeaturesResponse,
																featureRefs []string,
																requestedOnDemandFeatureViews *OnDemandFeatureView,
																fullFeatureNames bool,
															) {

}

// TODO (Ly): Review: Test this function
func (fs *FeatureStore) dropUnneededColumns(	onlineFeaturesResponse *serving.GetOnlineFeaturesResponse,
												requestedResultRowNames map[string]bool,) {
	metaDataLen := len(onlineFeaturesResponse.Metadata.FeatureNames.Val)
	neededMask := make([]bool, metaDataLen)
	for index, featureName := range onlineFeaturesResponse.Metadata.FeatureNames.Val {
		if _, ok := requestedResultRowNames[featureName]; !ok {
			neededMask[index] = false
		} else {
			neededMask[index] = true
		}
	}
	firstIndex := 0
	for index := 0; index < metaDataLen; index++ {
		if neededMask[index] {
			for rowIndex := 0; rowIndex < len(onlineFeaturesResponse.Results); rowIndex++ {
				onlineFeaturesResponse.Results[rowIndex].Values[firstIndex] = onlineFeaturesResponse.Results[rowIndex].Values[index]
				onlineFeaturesResponse.Results[rowIndex].Statuses[firstIndex] = onlineFeaturesResponse.Results[rowIndex].Statuses[index]
				onlineFeaturesResponse.Results[rowIndex].EventTimestamps[firstIndex] = onlineFeaturesResponse.Results[rowIndex].EventTimestamps[index]
				onlineFeaturesResponse.Metadata.FeatureNames.Val[firstIndex] = onlineFeaturesResponse.Metadata.FeatureNames.Val[index]
				
			}
			firstIndex += 1
		}
	}
	for rowIndex := 0; rowIndex < len(onlineFeaturesResponse.Results); rowIndex++ {
		onlineFeaturesResponse.Results[rowIndex].Values = onlineFeaturesResponse.Results[rowIndex].Values[:firstIndex]
		onlineFeaturesResponse.Results[rowIndex].Statuses = onlineFeaturesResponse.Results[rowIndex].Statuses[:firstIndex]
		onlineFeaturesResponse.Results[rowIndex].EventTimestamps = onlineFeaturesResponse.Results[rowIndex].EventTimestamps[:firstIndex]
		onlineFeaturesResponse.Metadata.FeatureNames.Val = onlineFeaturesResponse.Metadata.FeatureNames.Val[:firstIndex]
	}
}

// TODO (Ly): Implement allowCache option as in Python's sdk
// and dummy entity?
func (fs *FeatureStore) listFeatureViews() ([]*core.FeatureView, error) {
	return fs.registry.listFeatureViews(fs.config.Project)
}

// TODO (Ly): Implement allowCache option as in Python's sdk
// and dummy entity?
func (fs *FeatureStore) listEntitities() ([]*core.Entity, error) {
	return fs.registry.listEntities(fs.config.Project)
}

func groupFeatureRefs(	features []string,
						allFeatureViews []*FeatureView,
						allRequestFeatureViews []*RequestFeatureView,
						allOndemandFeatureViews []*OnDemandFeatureView,
					) 	(	map[*FeatureView][]string,
							map[*OnDemandFeatureView][]string,
							map[*RequestFeatureView][]string,
							error,
						)	{
	
	viewIndex := make(map[string]*FeatureView)
	requestViewIndex := make(map[string]*RequestFeatureView)
	onDemandViewIndex := make(map[string]*OnDemandFeatureView)

	for _, featureView := range allFeatureViews {
		viewIndex[featureView.base.name] = featureView
	}

	for _, requestView := range allRequestFeatureViews {
		requestViewIndex[requestView.base.name] = requestView
	}

	for _, onDemandView := range allOndemandFeatureViews {
		onDemandViewIndex[onDemandView.base.name] = onDemandView
	}

	fvFeatures := make(map[*FeatureView]map[string]bool)
	requestfvFeatures := make(map[*RequestFeatureView]map[string]bool)
	odfvFeatures := make(map[*OnDemandFeatureView]map[string]bool)

	for _, featureRef := range features {
		parsedFeatureName := strings.Split(featureRef, ":")
		if len(parsedFeatureName) < 2 {
			return nil, nil, nil, errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
		}
		featureViewName := parsedFeatureName[0]
		featureName := parsedFeatureName[1]
		if fv, ok := viewIndex[featureViewName]; ok {
			if _, ok = fvFeatures[fv]; !ok {
				fvFeatures[fv] = make(map[string]bool)
			}
			fvFeatures[fv][featureName] = true
		} else if requestfv, ok := requestViewIndex[featureViewName]; ok {
			if _, ok = requestfvFeatures[requestfv]; !ok {
				requestfvFeatures[requestfv] = make(map[string]bool)
			}
			requestfvFeatures[requestfv][featureName] = true
		} else if odfv, ok := onDemandViewIndex[featureViewName]; ok {
			if _, ok = odfvFeatures[odfv]; !ok {
				odfvFeatures[odfv] = make(map[string]bool)
			}
			odfvFeatures[odfv][featureName] = true
		} else {
			return nil, nil, nil, errors.New(fmt.Sprintf("FeatureView %s not found", featureViewName))
		}
	}

	fvResults := make(map[*FeatureView][]string, 0)
	requestfvResults := make(map[*RequestFeatureView][]string, 0)
	odfvResults := make(map[*OnDemandFeatureView][]string, 0)

	for fv, featureNamesMap := range fvFeatures {
		index := 0
		fvResults[fv] = make([]string, len(featureNamesMap))
		for featureName, _ := range featureNamesMap {
			fvResults[fv][index] = featureName
			index += 1
		}
		
	}

	for fv, featureNamesMap := range requestfvFeatures {
		index := 0
		requestfvResults[fv] = make([]string, len(featureNamesMap))
		for featureName, _ := range featureNamesMap {
			requestfvResults[fv][index] = featureName
			index += 1
		}
		
	}

	for fv, featureNamesMap := range odfvFeatures {
		index := 0
		odfvResults[fv] = make([]string, len(featureNamesMap))
		for featureName, _ := range featureNamesMap {
			odfvResults[fv][index] = featureName
			index += 1
		}
		
	}

	return fvResults, odfvResults, requestfvResults, nil
}