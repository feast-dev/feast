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
	registry, err := NewRegistry(config.Registry)
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
	parsedKind := fs.parseKind(kind)
	featureRefs, err := fs.getFeaturesparsedKind()
	if err != nil {
		return err
	}

	requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews, err := fs.getFeatureViewsToUse(parsedKind)
	if err != nil {
		return nil, err
	}
	entityNameToJoinKeyMap, err := fs.getEntityMaps(requestedFeatureViews)
	if err != nil {
		return nil, err
	}
	err := fs.validateEntityValues(request.GetEntities())
	if err != nil {
		return nil, err
	}
	err := fs.validateFeatureRefs(featureRefs, request.GetFullFeatureNames())
	if err != nil {
		return nil, err
	}
	groupedRef, groupedOdfvRefs, groupedRequestFvRefs, err := groupFeatureRefs(requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews)
	if err != nil {
		return nil, err
	}
	
	requestedResultRowNames := make([]string, len(featureRefs))
	if request.GetFullFeatureNames() {

	} else {
		
	}

}

func (fs *FeatureStore) DestructOnlineStore() {
	fs.onlineStore.Destruct()
}

func (fs *FeatureStore) parseKind(kind isGetOnlineFeaturesRequest_Kind) (interface{}, error) {
	if featureList, ok := kind.(*GetOnlineFeaturesRequest_Features); ok {
		return featureList.Features.GetVal(), nil
	}
	if featureService, ok := kind.(*GetOnlineFeaturesRequest_FeatureService); ok {
		featureServiceProto, err := fs.registry.getFeatureService(fs.config.Project, featureService.FeatureService)
		if err != nil {
			return nil, err
		}
		return NewFeatureServiceFromProto(featureServiceProto), nil
	}
	return nil, error.New("Cannot parse kind from GetOnlineFeaturesRequest")
}

func (fs *FeatureStore) getFeatures(parsedKind interface{}) ([]string, error) {
	
	if features, ok :=  parsedKind.([]string); !ok {
		var featureService *FeatureService
		if featureService, ok = parsedKind.(*FeatureService); !ok {
			return nil, errors.New("Cannot parse FeatureService from request")
		}

		var featureName string
		features := make([]string, len(featureService.projections))
		for index, featureProjection := range featureService.projections {
			// TODO (Ly): have a FeatureService class that
			// contains nameToUse() method as in Python's sdk
			featureViewName = featureProjection.nameToUse()
			for index, featureColumn := featureProjection.featureColumns {
				features[index] = fmt.Sprintf("%s:%s", featureViewName, featureColumn.GetName())
			}
		}
		return features, nil
	} else {
		return features, nil
	}
}

// TODO (Ly): Implement allowCache option as in Python's sdk
// and dummy entity?
func (fs *FeatureStore) getFeatureViewsToUse(parsedKind interface{}) ([]*FeatureView, []*RequestFeatureView, []*OndemandFeatureView, error) {
	
	fvs := make(map[string]*FeatureView)
	featureViews := fs.listFeatureViews()
	for _, featureView := range featureViews {
		fvs[featureView.GetSpec().GetName()] = NewFeatureViewFromProto(proto: featureView)
	}

	requestFvs := make(map[string]*RequestFeatureView)
	requestFeatureViews := fs.registry.listRequestFeatureViews()
	for _, requestFeatureView := range featureViews {
		fvs[requestFeatureView.GetSpec().GetName()] = NewRequestFeatureViewFromProto(proto: requestFeatureView)
	}

	odFvs := make(map[string]*OndemandFeatureView)
	ondemandFeatureViews := fs.registry.listOndemandFeatureViews()
	for _, ondemandFeatureView := range ondemandFeatureViews {
		odFvs[ondemandFeatureView.GetSpec().GetName()] = NewOndemandFeatureViewFromProto(proto: ondemandFeatureView)
	}

	if featureService, ok := parsedKind.(*FeatureService); ok {
		
		// TODO (Ly): Review: Skip checking featureService from registry since
		// we're only given featureServiceName

		fvsToUse := make([]*FeatureView, 0)
		requestFvsToUse := make([]*RequestFeatureView, 0)
		odFvsToUse := make([]*OndemandFeatureView, 0)

		for _, featureProjection := range featureService.projections {
			
			// Create copies of FeatureView that may
			// contains the same *core.FeatureView but
			// each differentiated by a *FeatureViewProjection
			featureViewName = featureProjection.GetFeatureViewName()
			if fv, ok := fvs[featureViewName]; ok {
				fvsToUse = append(fvsToUse, fv.withProjection(NewFeatureViewProjectionFromProto(featureProjection)))
			} else if fv, ok = requestFvs[featureViewName]; ok {
				requestFvsToUse = append(requestFvsToUse, fv.withProjection(NewFeatureViewProjectionFromProto(featureProjection)))
			} else if fv, ok = odFvs[featureViewName]; ok {
				odFvsToUse = append(odFvsToUse, fv.withProjection(NewFeatureViewProjectionFromProto(featureProjection)))
			} else {
				return nil, nil, nil, errors.New(fmt.Sprintf("The provided feature service %s contains a reference to a feature view"
				"%s which doesn't exist. Please make sure that you have created the feature view"
				"%s and that you have registered it by running \"apply\".", featureServiceName, featureViewName, featureViewName))
			}
		}
	}

	fvsToUse := make([]*FeatureView, len(fvs))
	requestFvsToUse := make([]*RequestFeatureView, len(requestFvs))
	odFvsToUse := make([]*OndemandFeatureView, len(odFvs))
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
	entities := fs.listEntitities()

	entityNameToJoinKeyMap := make(map[string]string)
	var entityNames []string
	var entityName string
	var featureView *types.FeatureView
	for _, featureView = range requestedFeatureViews {
		entityNames = featureView.proto.GetSpec().GetEntities()
		joinKeyMap = featureView.projection.joinKeyMap
		for _, entityName = range entityNames {
			// TODO (Ly): Remove this with fs.registry.getEntity()
			entity = fs.registry.getEntity(fs.config.Project, entityName)
			entityName := entity.GetName()
			joinKey := entity.GetJoinKey()

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
	return entityNameToJoinKeyMap
}

func (fs *FeatureStore) validateEntityValues(joinKeyValues map[string]*types.RepeatedValue) error {
	setOfRowLengths := map[int]bool
	for _, col := range joinKeyValues {
		setOfRowLengths[len(col.GetVal())] = true
	}
	if len(setOfRowLengths) > 1 {
		return errors.New("All entity rows must have the same columns.")
	}
	return nil
}

func (fs *FeatureStore) validateFeatureRefs(featureRefs []string, fullFeatureNames bool) error {
	collidedFeatureRefs := map[string]int
	if fullFeatureNames {
		for _, featureRef := range featureRefs {
			collidedFeatureRefs[featureRef] += 1
			if collidedFeatureRefs[featureRef] > 1 {
				return errors.New(fmt.Sprintf("Collided FeatureRef detected: %s", featureRef))
			}
		}
	} else {
		for _, featureRef := range featureRefs {
			
			parsedFeatureName := strings.Split(featureName, ":")
			var featureName string
			if len(parsedFeatureName) == 0 {
				return nil, nil, nil, errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
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
	return neededRequestData, neededRequestFvFeatures
}

func (fs *FeatureStore) ensureRequestedDataExist(	neededRequestData map[string]bool,
													neededRequestFvFeatures map[string]bool,
													requestDataFeatures map[string]*types.RepeatedValue) error {
	// TODO (Ly): Review: Skip checking even if composite set of
	// neededRequestData neededRequestFvFeatures is different from
	// request_data_features but same length?
	if len(neededRequestData) + len(neededRequestFvFeatures) != len(requestDataFeatures) {
		missingFeatures := make([]string, 0)
		for feature, _, := range neededRequestData {
			if _, ok := requestDataFeatures[feature]; !ok {
				missingFeatures = append(missingFeatures, feature)
			}
		}
		for feature, _, := range neededRequestFvFeatures {
			if _, ok := requestDataFeatures[feature]; !ok {
				missingFeatures = append(missingFeatures, feature)
			}
		}
		return errors.New("RequestDataNotFoundInEntityRowsException: %s", strings.Join(missingFeatures, ", "))
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
func (fs *FeatureStore) getUniqueEntities(	table *FeatureView,
											joinKeyValues map[string]*types.RepeatedValue,
											entityNameToJoinKeyMap map[string]string) 
											(uniqueEntityKeys []*types.EntityKey, indices [][]int, error) {
	tableEntityValues := getTableEntityValues(table, joinKeyValues, entityNameToJoinKeyMap)
	keys := make([]string, len(tableEntityValues))
	index := 0
	var numRows int
	for k, v := range tableEntityValues {
		keys[index] = k
		index += 1
		numRows = len(v)
	}
	sort.Strings(keys)
	entityKeys := make([]*types.EntityKey, numRows)
	rowise := make(map[[]byte]]*entityKeyRow)
	numJoinKeys := len(keys)
	// Construct each EntityKey object
	for index = 0; index < numRows; ++i {
		entityKeys[index] = &types.EntityKey{JoinKeys: keys, EntityValues: make([]*types.Value, numJoinKeys)}
	}

	for colIndex, key := range keys {
		for index, value := range tableEntityValues[key].GetVal() {
			entityKeys[index].EntityValues[colIndex] = value
		}
	}

	for index, entityKey := range entityKeys {
		key, err := SerializeEntityKey(entityKey)
		if err != nil {
			return nil, nil, err
		}
		if ekRow, ok := rowise[*key]; ok {
			ekRow.rowIndices = append(ekRow.rowIndices, index)
		} else {
			ekRow = &entityKeyRow{entityKey: entityKeys[index], rowIndices: make([]int, 1)}
			rowise[*key] = ekRow
			ekRow.rowIndices[0] = index
		}
	}
	numUniqueRows := len(rowise)
	uniqueEntityKeys = make([]*types.EntityKey, numUniqueRows)
	indices = make([][]int, numUniqueRows)
	index = 0
	for _, ekRow := range rowise {
		uniqueEntityKeys[index] = ekRow.entityKey
		indices[index] = ekRow.rowIndices
		index += 1
	}
	return uniqueEntityKeys, indices
}

func (fs *FeatureStore) getTableEntityValues(	table *FeatureView,
												joinKeyValues map[string]*types.RepeatedValue,
												entityNameToJoinKeyMap map[string]string) map[string]*types.RepeatedValue {
	
	tableJoinKeys := make(map[string]bool)
	for _, enityName := range table.GetSpec().GetEntities() {
		tableJoinKeys[entityNameToJoinKeyMap[enityName]] = true
	}

	aliasToJoinKeyMap := make(map[string]string)
	for k,v := range table.projection.joinKeyMap {
		aliasToJoinKeyMap[v] = k
	}

	entityValues := make(map[string]*types.RepeatedValue)
	
	for k, v := range joinKeyValues {
		entityKey := k
		if _, ok := aliasToJoinKeyMap[k]; ok {
			entityKey = aliasToJoinKeyMap[k]
		}
		if _, ok := tableJoinKeys[entityKey]; ok {
			entityValues[entityValues] = v
		}
	}

	return entityValues
}

func (fs *FeatureStore) readFromOnlineStore(	entityRows []*types.EntityKey,
												requestedFeatures []string,
												table *FeatureView) 
												( FeatureData, error ) {
	numRows := len(entityRows)
	entityRowsValue := make([]types.EntityKey, numRows)
	for index, entityKey := range entityRows {
		entityRowsValue[index] = *entityKey
	}
	return fs.onlineStore.OnlineRead(entityRowsValue, table.proto.GetSpec().GetName(), requestedFeatures)
}

func (fs *FeatureStore) populateResponseFromFeatureData(	featureData2D [][]FeatureData,
															indexes [][]int,
															onlineFeaturesResponse: *serving.GetOnlineFeaturesResponse,
															fullFeatureNames bool,
															requestedFeatures []string,
															table *FeatureView,) {
	
	requestedFeatureRefs := make([]string, len(requestedFeatures))

	for index, featureName := range requestedFeatures {
		if fullFeatureNames {
			requestedFeatureRefs[index] = fmt.Sprintf("%s__%s", table.projection.nameToUse(), featureName)
		} else {
			requestedFeatureRefs[index] = featureName
		}
	}
	onlineFeaturesResponse.MetaData.FeatureNames.Val = append(onlineFeaturesResponse.MetaData.FeatureNames.Val, requestedFeatureRefs...)
	featureViewSpec := table.proto.GetSpec()

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
			} else if checkOutsideMaxAge(&eventTimeStamp, timestamppb.Now(), featureViewSpec.GetTtl()) {
				status = serving.FieldStatus_OUTSIDE_MAX_AGE
			}
			values[index] = &value
			statuses[index] = status
			eventTimeStamps[index] = &eventTimeStamp
		}

		for _, rowIndex := range indexes[entityIndex] {
			onlineFeaturesResponse.Results[rowIndex].Values = append(onlineFeaturesResponse.Results[rowIndex].Values, values...)
			onlineFeaturesResponse.Results[rowIndex].Statuses = append(onlineFeaturesResponse.Results[rowIndex].Statuses, statuses...)
			onlineFeaturesResponse.Results[rowIndex].EventTimeStamps = append(onlineFeaturesResponse.Results[rowIndex].EventTimeStamps, eventTimeStamps...)
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

// TODO (Ly): Complete this function
func (fs *FeatureStore) dropUnneededColumns(	onlineFeaturesResponse *serving.GetOnlineFeaturesResponse,
												requestedResultRowNames map[string]bool,) {

}

// TODO (Ly): Implement allowCache option as in Python's sdk
// and dummy entity?
func (fs *FeatureStore) listFeatureViews() []*core.FeatureView {
	return fs.registry.listFeatureViews(fs.config.Project)
}

// TODO (Ly): Implement allowCache option as in Python's sdk
// and dummy entity?
func (fs *FeatureStore) listEntitities() ([]*types.Entity, error) {
	return fs.registry.listEntities(fs.config.Project)
}

func groupFeatureRefs(	features []string,
						allFeatureViews []*core.FeatureView,
						allRequestFeatureViews []*core.RequestFeatureView,
						allOndemandFeatureViews []*core.OndemandFeatureView) 
						(	map[*core.FeatureView][]string,
							map[*core.RequestFeatureView][]string,
							map[*core.OndemandFeatureView][]string,
							error
						)	{
	
	viewIndex := make(map[string]*core.FeatureView)
	requestViewIndex := make(map[string]*core.RequestFeatureView)
	onDemandViewIndex := make(map[string]*core.OnDemandFeatureView)

	for _, featureView := range allFeatureViews {
		viewIndex[featureView.GetSpec().GetName()] = featureView
	}

	for _, requestView := range allRequestFeatureViews {
		requestViewIndex[requestView.GetSpec().GetName()] = requestView
	}

	for _, onDemandView := range allOndemandFeatureViews {
		onDemandViewIndex[onDemandView.GetSpec().GetName()] = onDemandView
	}

	fvFeatures := make(map[*core.FeatureView]map[string]bool, 0)
	requestfvFeatures := make(map[*core.RequestFeatureView]map[string]bool, 0)
	odfvFeatures := make(map[*core.OndemandFeatureView]map[string]bool, 0)

	for index, featureRef := range features {
		parsedFeatureName := strings.Split(featureName, ":")
		if len(parsedFeatureName) < 2 {
			return nil, nil, nil, errors.New("FeatureReference should be in the format: 'FeatureViewName:FeatureName'")
		}
		featureViewName := parsedFeatureName[0]
		featureName := parsedFeatureName[1]
		if fv, ok := viewIndex[featureViewName]; ok {
			fvFeatures[fv][featureName] = true
		} else if fv, ok = requestViewIndex[featureViewName]; ok {
			requestfvFeatures[fv][featureName] = true
		} else if fv, ok = onDemandViewIndex[featureViewName]; ok {
			odfvFeatures[fv][featureName] = true
		} else {
			return nil, nil, nil, errors.New(fmt.Sprintf("FeatureView %s not found", featureViewName))
		}
	}

	fvResults := make(map[*core.FeatureView][]string, 0)
	requestfvResults := make(map[*core.RequestFeatureView][]string, 0)
	odfvResults := make(map[*core.OndemandFeatureView][]string, 0)
	var index int

	for fv, featureNamesMap := range fvFeatures {
		index = 0
		fvResults[fv] := make([]string, len(featureNamesMap))
		for featureName, _ := range featureNamesMap {
			fvResults[fv][index] = featureName
			index += 1
		}
		
	}

	for fv, featureNamesMap := range requestfvFeatures {
		index = 0
		requestfvResults[fv] := make([]string, len(featureNamesMap))
		for featureName, _ := range featureNamesMap {
			requestfvResults[fv][index] = featureName
			index += 1
		}
		
	}

	for fv, featureNamesMap := range odfvFeatures {
		index = 0
		odfvResults[fv] := make([]string, len(featureNamesMap))
		for featureName, _ := range featureNamesMap {
			odfvResults[fv][index] = featureName
			index += 1
		}
		
	}

	return fvResults, requestfvResults, odfvResults, nil
}