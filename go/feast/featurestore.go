package feast

import (
	"errors"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"sort"
	"fmt"
)

type FeatureStore struct {
	config      *RepoConfig
	registry    *Registry
	onlineStore OnlineStore
}

type entityKeyRow struct {
	entityKey *types.EntityKey
	rowIndices []int
}

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config *RepoConfig) (*FeatureStore, error) {
	onlineStore, err := getOnlineStore(config)
	if err != nil {
		return nil, err
	}
	
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

func (fs *FeatureStore) GetOnlineFeatures(request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {

	kind := request.GetKind()
	fullFeatureNames := request.GetFullFeatureNames()
	parsedKind, err := fs.parseKind(kind)
	if err != nil {
		return nil, err
	}

	featureRefs, err := fs.getFeatures(parsedKind, true)
	if err != nil {
		return nil, err
	}
	entityProtos := request.GetEntities()
	numRows, err := fs.validateEntityValues(entityProtos)
	if err != nil {
		return nil, err
	}
	err = fs.validateFeatureRefs(featureRefs, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews, err := fs.getFeatureViewsToUse(parsedKind, true, false)

	// TODO (Ly): Remove this BLOCK once odfv is supported
	if len(requestedRequestFeatureViews) + len(requestedOnDemandFeatureViews) > 0 {
		return nil, errors.New("Ondemand transform is not supported in this iteration, please wait!")
	}
	// END BLOCK

	if err != nil {
		return nil, err
	}
	entityNameToJoinKeyMap, err := fs.getEntityMaps(requestedFeatureViews)
	if err != nil {
		return nil, err
	}
	
	groupedRefs, groupedOdfvRefs, groupedRequestFvRefs, err := groupFeatureRefs(featureRefs, requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews)
	if err != nil {
		return nil, err
	}
	
	requestedResultRowNames := make(map[string]struct{})
	if fullFeatureNames {
		for _, featureReference := range featureRefs {
			if !strings.Contains(featureReference, ":") {
				return nil, errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
			}
			requestedResultRowNames[strings.Replace(featureReference, ":", "__", 1)] = struct{}{}
		}
	} else {
		for _, featureReference := range featureRefs {
			parsedFeatureName := strings.Split(featureReference, ":")
			var featureName string
			if len(parsedFeatureName) == 0 {
				return nil, errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
			} else if len(parsedFeatureName) == 1 {
				featureName = parsedFeatureName[0]
			} else {
				featureName = parsedFeatureName[1]
			}
			requestedResultRowNames[featureName] = struct{}{}
		}
	}

	featureViews := make([]*FeatureView, len(groupedRefs))
	index := 0
	for featureView, _ := range groupedRefs {
		featureViews[index] = featureView
		index += 1
	}

	// TODO (Ly): This should return empty now
	// Expect no ODFV or Request FV passed in GetOnlineFearuresRequest
	neededRequestData, neededRequestFvFeatures, err := fs.getNeededRequestData(groupedOdfvRefs, groupedRequestFvRefs)
	if err != nil {
		return nil, err
	}

	joinKeyValues := make(map[string]*types.RepeatedValue)
	requestDataFeatures := make(map[string]*types.RepeatedValue) // TODO (Ly): Should be empty now until ODFV and Request FV are supported
	for entityName, vals := range entityProtos {
		if _, ok := neededRequestFvFeatures[entityName]; ok {
			requestedResultRowNames[entityName] = struct{}{}
			requestDataFeatures[entityName] = vals
		} else if _, ok = neededRequestData[entityName]; ok {
			requestDataFeatures[entityName] = vals
		} else {
			if joinKey, ok := entityNameToJoinKeyMap[entityName]; !ok {
				return nil, errors.New(fmt.Sprintf("EntityNotFoundException: %s\n%v", entityName, entityNameToJoinKeyMap))
			} else {
				joinKeyValues[joinKey] = vals
				requestedResultRowNames[joinKey] = struct{}{}
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
	// Merge joinKeyValues into requestDataFeatures
	for entityName, vals := range joinKeyValues {
		requestDataFeatures[entityName] = vals
	}

	fs.populateResultRowsFromColumnar(onlineFeatureResponse, requestDataFeatures)

	entitylessCase := false
	
	for _, featureView := range featureViews {
		if _, ok := featureView.entities[DUMMY_ENTITY_NAME]; ok {
			entitylessCase = true
			break
		}
	}

	if entitylessCase {
		dummyEntityColumn := &types.RepeatedValue{Val: make([]*types.Value, numRows)}
		for index := 0; index < numRows; index++ {
			dummyEntityColumn.Val[index] = &DUMMY_ENTITY
		}
		joinKeyValues[DUMMY_ENTITY_ID] = dummyEntityColumn
	}

	for table, requestedFeatures := range groupedRefs {
		tableEntityValues, idxs, err := fs.getUniqueEntities( table, joinKeyValues, entityNameToJoinKeyMap)
		if err != nil {
			return nil, err
		}
		featureData, err := fs.readFromOnlineStore(tableEntityValues, requestedFeatures, table)
		fs.populateResponseFromFeatureData(		featureData,
												idxs,
												onlineFeatureResponse,
												fullFeatureNames,
												requestedFeatures,
												table,
												)
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
	if featureServiceRequest, ok := kind.(*serving.GetOnlineFeaturesRequest_FeatureService); ok {
		featureService, err := fs.registry.getFeatureService(fs.config.Project, featureServiceRequest.FeatureService)
		if err != nil {
			return nil, err
		}
		return featureService, nil
	}
	return nil, errors.New("Cannot parse kind from GetOnlineFeaturesRequest")
}

/*
	This function returns all feature references from GetOnlineFeaturesRequest.
	If a list of feature references is passed, return it.
	Otherwise, FeatureService was passed, parse this feature service
	to get a list of FeatureViewProjection and return feature references
	from this list
*/

func (fs *FeatureStore) getFeatures(parsedKind interface{}, allowCache bool) ([]string, error) {
	
	if features, ok :=  parsedKind.([]string); !ok {
		var featureService *FeatureService
		if featureService, ok = parsedKind.(*FeatureService); !ok {
			return nil, errors.New("Cannot parse FeatureService from request")
		}

		var featureViewName string
		features = make([]string, 0)
		for _, featureProjection := range featureService.projections {
			featureViewName = featureProjection.nameToUse()
			for _, feature := range featureProjection.features {
				features = append(features, fmt.Sprintf("%s:%s", featureViewName, feature.name))
			}
		}
		return features, nil
	} else {
		return features, nil
	}
}

/*
	If features passed into GetOnlineFeaturesRequest as a list of feature references,
		return all FeatureView, OnDemandFeatureView, RequestFeatureView from the registry
	Otherwise, a FeatureService was passed, return a list of copies of FeatureViewProjection
		copied from FeatureView, OnDemandFeatureView, RequestFeatureView existed in the registry
	
	TODO (Ly): Since the implementation of registry has changed, a better approach here is just
		retrieving featureViews asked in the passed in list of feature references instead of
		retrieving all feature views. Similar argument to FeatureService applies.

*/
func (fs *FeatureStore) getFeatureViewsToUse(parsedKind interface{}, allowCache, hideDummyEntity bool) ([]*FeatureView, []*RequestFeatureView, []*OnDemandFeatureView, error) {
	
	fvs := make(map[string]*FeatureView)
	requestFvs := make(map[string]*RequestFeatureView)
	odFvs := make(map[string]*OnDemandFeatureView)

	featureViews, err := fs.listFeatureViews(allowCache, hideDummyEntity)
	if err != nil {
		// TODO (Ly): Review: return an empty list instead of
		// error if no FeatureView is found in this project
		featureViews = make([]*FeatureView, 0)
	}
	for _, featureView := range featureViews {
		fvs[featureView.base.name] = featureView
	}
	
	requestFeatureViews, err := fs.registry.listRequestFeatureViews(fs.config.Project)
	if err != nil {
		// TODO (Ly): Review: return an empty list instead of
		// error if no RequestFeatureView is found in this project
		requestFeatureViews = make([]*RequestFeatureView, 0)
	}
	for _, requestFeatureView := range requestFeatureViews {
		requestFvs[requestFeatureView.base.name] = requestFeatureView
	}

	onDemandFeatureViews, err := fs.registry.listOnDemandFeatureViews(fs.config.Project)
	if err != nil {
		// TODO (Ly): Review: return an empty list instead of
		// error if no OnDemandFeatureView is found in this project
		onDemandFeatureViews = make([]*OnDemandFeatureView, 0)
	}
	for _, onDemandFeatureView := range onDemandFeatureViews {
		odFvs[onDemandFeatureView.base.name] = onDemandFeatureView
	}
	
	if featureService, ok := parsedKind.(*FeatureService); ok {

		fvsToUse := make([]*FeatureView, 0)
		requestFvsToUse := make([]*RequestFeatureView, 0)
		odFvsToUse := make([]*OnDemandFeatureView, 0)

		for _, featureProjection := range featureService.projections {
			// Create copies of FeatureView that may
			// contains the same *FeatureView but
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
		return fvsToUse, requestFvsToUse, odFvsToUse, nil
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

func (fs *FeatureStore) getEntityMaps(requestedFeatureViews []*FeatureView) (map[string]string, error) {

	entityNameToJoinKeyMap := make(map[string]string)
	var entityNames map[string]bool
	var entityName string
	var joinKeyMap map[string]string
	var featureView *FeatureView

	entities, err := fs.listEntities(true, false)

	if err != nil {
		entities = make([]*Entity, 0)
	}

	for _, entity := range entities {
		entityNameToJoinKeyMap[entity.name] = entity.joinKey
	}
	
	for _, featureView = range requestedFeatureViews {

		entityNames = featureView.entities
		joinKeyMap = featureView.base.projection.joinKeyMap
		for entityName, _ = range entityNames {

			entity, err := fs.registry.getEntity(fs.config.Project, entityName)
			if err != nil {
				return nil, err
			}
			entityName := entity.name
			joinKey := entity.joinKey

			// TODO (Ly): Review: weird that both uses the same map?
			// from python's sdk
			if entityNameMapped, ok := joinKeyMap[joinKey]; ok {
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
		return 0, errors.New("ValueError: All entity rows must have the same columns.")
	}
	return numRows, nil
}

func (fs *FeatureStore) validateFeatureRefs(featureRefs []string, fullFeatureNames bool) error {
	collidedFeatureRefs := make(map[string]int)
	if fullFeatureNames {
		for _, featureRef := range featureRefs {
			collidedFeatureRefs[featureRef] += 1
		}
		for featureName, occurrences := range collidedFeatureRefs {
			if occurrences == 1 {
				delete(collidedFeatureRefs, featureName)
			}
		}
		if len(collidedFeatureRefs) >= 1 {
			collidedFeatureRefList := make([]string, len(collidedFeatureRefs))
			index := 0
			for featureName, _ := range collidedFeatureRefs {
				collidedFeatureRefList[index] = featureName
				index += 1
			}
			return errors.New(fmt.Sprintf("FeatureNameCollisionError: %s; %t", strings.Join(collidedFeatureRefList, ", "), fullFeatureNames))
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
		}

		for featureName, occurrences := range collidedFeatureRefs {
			if occurrences == 1 {
				delete(collidedFeatureRefs, featureName)
			}
		}
		if len(collidedFeatureRefs) >= 1 {
			collidedFeatureRefList := make([]string, 0)
			for _, featureRef := range featureRefs {
				parsedFeatureName := strings.Split(featureRef, ":")
			
				if len(parsedFeatureName) == 0 {
					return errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
				} else if len(parsedFeatureName) == 1 {
					featureName = parsedFeatureName[0]
				} else {
					featureName = parsedFeatureName[1]
				}
				if _,ok := collidedFeatureRefs[featureName]; ok {
					collidedFeatureRefList = append(collidedFeatureRefList, featureRef)
				}
				
			}
			return errors.New(fmt.Sprintf("FeatureNameCollisionError: %s; %t", strings.Join(collidedFeatureRefList, ", "), fullFeatureNames))
		}
	}
	return nil
}

func (fs *FeatureStore) getNeededRequestData(	groupedOdfvRefs map[*OnDemandFeatureView][]string,
												groupedRequestFvRefs map[*RequestFeatureView][]string) (map[string]struct{}, map[string]struct{}, error){
	neededRequestData := make(map[string]struct{})
	neededRequestFvFeatures := make(map[string]struct{})

	for onDemandFeatureView, _ := range groupedOdfvRefs {
		requestSchema := onDemandFeatureView.getRequestDataSchema()
		for fieldName, _ := range requestSchema {
			neededRequestData[fieldName] = struct{}{}
		}
	}

	for requestFeatureView, _ := range groupedRequestFvRefs {
		for _, feature := range requestFeatureView.base.features {
			neededRequestFvFeatures[feature.name] = struct{}{}
		}
	}

	return neededRequestData, neededRequestFvFeatures, nil
}

func (fs *FeatureStore) ensureRequestedDataExist(	neededRequestData map[string]struct{},
													neededRequestFvFeatures map[string]struct{},
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
			featureTimeStamp := timestamppb.Timestamp{ Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos }
			featureValue := types.Value{Val: value.Val}
			featureVector.Values = append(featureVector.Values, &featureValue)
			featureVector.Statuses = append(featureVector.Statuses, serving.FieldStatus_PRESENT)
			featureVector.EventTimestamps = append(featureVector.EventTimestamps, &featureTimeStamp)
		}
	}
}

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
		key, err := serializeEntityKey(entityKey)
		if err != nil {
			return nil, nil, err
		}
		keyStr := string(*key)
		if ekRow, ok := rowise[keyStr]; ok {
			ekRow.rowIndices = append(ekRow.rowIndices, index)
		} else {
			ekRow = &entityKeyRow{entityKey: entityKeys[index], rowIndices: make([]int, 1)}
			rowise[keyStr] = ekRow
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
	for enityName, _ := range table.entities {
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
											) 	( [][]FeatureData, error ) {
	numRows := len(entityRows)
	entityRowsValue := make([]types.EntityKey, numRows)
	for index, entityKey := range entityRows {
		entityRowsValue[index] = types.EntityKey{ JoinKeys: entityKey.JoinKeys, EntityValues: entityKey.EntityValues }
	}
	return fs.onlineStore.OnlineRead(entityRowsValue, table.base.name, requestedFeatures)
}

func (fs *FeatureStore) populateResponseFromFeatureData(	featureData2D [][]FeatureData,
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
	numFeatures := len(requestedFeatureRefs)

	for entityIndex, featureList := range featureData2D {
		
		values := make([]*types.Value, numFeatures)
		statuses := make([]serving.FieldStatus, numFeatures)
		eventTimeStamps := make([]*timestamppb.Timestamp, numFeatures)
		if featureList == nil {
			for index := 0; index < numFeatures; index++ {
				status := serving.FieldStatus_NOT_FOUND
				eventTimeStamp := timestamppb.Timestamp{}

				values[index] = nil
				statuses[index] = status
				eventTimeStamps[index] = &eventTimeStamp
			}
		} else {
			for index, _ := range featureList {
				featureData := &featureList[index]
				value := types.Value{Val: featureData.value.Val}
				status := serving.FieldStatus_PRESENT
				eventTimeStamp := timestamppb.Timestamp{Seconds: featureData.timestamp.Seconds, Nanos: featureData.timestamp.Nanos }

				values[index] = &value
	
				if _, ok := value.Val.(*types.Value_NullVal); ok {
					values[index] = nil
					status = serving.FieldStatus_NOT_FOUND
				} else if fs.checkOutsideMaxAge(&eventTimeStamp, timestamppb.Now(), table.ttl ) {
					values[index] = &value
					status = serving.FieldStatus_OUTSIDE_MAX_AGE
				}
				
				statuses[index] = status
				eventTimeStamps[index] = &eventTimeStamp
			}
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
																requestedOnDemandFeatureViews []*OnDemandFeatureView,
																fullFeatureNames bool,
															) {
	requestedOdfvMap := make(map[string]*OnDemandFeatureView)
	requestedOdfvNames := make([]string, len(requestedOnDemandFeatureViews))
	for index, requestedOdfv := range requestedOnDemandFeatureViews {
		requestedOdfvMap[requestedOdfv.base.name] = requestedOdfv
		requestedOdfvNames[index] = requestedOdfv.base.name
	}

	odfvFeatureRefs := make(map[string][]string)
	for _, featureRef := range featureRefs {
		parsedFeatureName := strings.Split(featureRef, ":")
		if len(parsedFeatureName) < 2 {
			continue
		}
		viewName := parsedFeatureName[0]
		featureName := parsedFeatureName[1]
		if _, ok := requestedOdfvMap[viewName]; ok {
			
			viewNameToUse := requestedOdfvMap[viewName].base.projection.nameToUse()
			if fullFeatureNames {
				featureName = fmt.Sprintf("%s__%s", viewNameToUse, featureName)
			}
			odfvFeatureRefs[viewName] = append(odfvFeatureRefs[viewName], featureName)
		}
	}
}

func (fs *FeatureStore) dropUnneededColumns(	onlineFeaturesResponse *serving.GetOnlineFeaturesResponse,
												requestedResultRowNames map[string]struct{},) {
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

func (fs *FeatureStore) listFeatureViews(allowCache, hideDummyEntity bool) ([]*FeatureView, error) {
	featureViews, err := fs.registry.listFeatureViews(fs.config.Project)
	if err != nil {
		return nil, err
	}
	for _, featureView := range featureViews {
		if _, ok := featureView.entities[DUMMY_ENTITY_NAME]; ok && hideDummyEntity {
			featureView.entities = make(map[string]bool)
		}
	}
	return featureViews, nil
}

func (fs *FeatureStore) listEntities(allowCache, hideDummyEntity bool) ([]*Entity, error) {
	
	allEntities, err := fs.registry.listEntities(fs.config.Project)
	if err != nil {
		return nil, err
	}
	entities := make([]*Entity, 0)
	for _, entity := range allEntities {
		if entity.name != DUMMY_ENTITY_NAME || !hideDummyEntity {
			entities = append(entities, entity)
		}
	}
	return entities, nil
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
		viewIndex[featureView.base.projection.nameToUse()] = featureView
	}

	for _, requestView := range allRequestFeatureViews {
		requestViewIndex[requestView.base.projection.nameToUse()] = requestView
	}

	for _, onDemandView := range allOndemandFeatureViews {
		onDemandViewIndex[onDemandView.base.projection.nameToUse()] = onDemandView
	}

	fvFeatures := make(map[*FeatureView]map[string]struct{})
	requestfvFeatures := make(map[*RequestFeatureView]map[string]struct{})
	odfvFeatures := make(map[*OnDemandFeatureView]map[string]struct{})

	for _, featureRef := range features {
		parsedFeatureName := strings.Split(featureRef, ":")
		if len(parsedFeatureName) < 2 {
			return nil, nil, nil, errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
		}
		featureViewName := parsedFeatureName[0]
		featureName := parsedFeatureName[1]
		if fv, ok := viewIndex[featureViewName]; ok {
			if _, ok = fvFeatures[fv]; !ok {
				fvFeatures[fv] = make(map[string]struct{})
			}
			fvFeatures[fv][featureName] = struct{}{}
		} else if requestfv, ok := requestViewIndex[featureViewName]; ok {
			if _, ok = requestfvFeatures[requestfv]; !ok {
				requestfvFeatures[requestfv] = make(map[string]struct{})
			}
			requestfvFeatures[requestfv][featureName] = struct{}{}
		} else if odfv, ok := onDemandViewIndex[featureViewName]; ok {
			if _, ok = odfvFeatures[odfv]; !ok {
				odfvFeatures[odfv] = make(map[string]struct{})
			}
			odfvFeatures[odfv][featureName] = struct{}{}
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

func (fs *FeatureStore) getFeatureView(project, featureViewName string, allowCache, hideDummyEntity bool) (*FeatureView, error) {
	fv, err := fs.registry.getFeatureView(fs.config.Project, featureViewName)
	if err != nil {
		return nil, err
	}
	if _, ok := fv.entities[DUMMY_ENTITY_NAME]; ok && hideDummyEntity {
		fv.entities = make(map[string]bool)
	}
	return fv, nil
}