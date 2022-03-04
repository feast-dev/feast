package feast

import (
	"context"
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"sort"
	"strings"
)

type FeatureStore struct {
	config      *RepoConfig
	registry    *Registry
	onlineStore OnlineStore
}

type entityKeyRow struct {
	entityKey  *types.EntityKey
	rowIndices []int
}

type ParsedKind struct {
	features       []string
	featureService *FeatureService
}

type GroupedFeaturesPerEntitySet struct {
	// A list of requested feature references of the form featureViewName:featureName that share this entity set
	featureNames     []string
	featureViewNames []string
	// A list of requested featureName if fullFeatureNames = False or a list of featureViewNameAlias__featureName that share this
	// entity set
	featureResponseMeta []string
	// Entity set as a list of EntityKeys to pass to OnlineRead
	entityKeys []*types.EntityKey
	// Indices for each requested feature in a featureView to return to OnlineResponse that match with the corresponding row in entityKeys
	// Dim(indices[i]) = number of requested rows
	indices [][]int
	// Map from featureIndex to the set of indices it shares with other requested features in the same feature view / feature projection
	indicesMapper map[int]int
}

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config *RepoConfig) (*FeatureStore, error) {
	onlineStore, err := getOnlineStore(config)
	if err != nil {
		return nil, err
	}

	registry, err := NewRegistry(config.GetRegistryConfig(), config.RepoPath)
	if err != nil {
		return nil, err
	}
	registry.initializeRegistry()

	return &FeatureStore{
		config:      config,
		registry:    registry,
		onlineStore: onlineStore,
	}, nil
}

// TODO: Review all functions that use ODFV and Request FV since these have not been tested
func (fs *FeatureStore) GetOnlineFeatures(ctx context.Context, request *serving.GetOnlineFeaturesRequest) (*serving.GetOnlineFeaturesResponse, error) {
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

	fvs, requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews, err := fs.getFeatureViewsToUse(parsedKind, true, false)

	// TODO (Ly): Remove this BLOCK once odfv is supported
	if len(requestedRequestFeatureViews)+len(requestedOnDemandFeatureViews) > 0 {
		return nil, errors.New("ODFV is not supported in this iteration, please wait!")
	}
	// END BLOCK

	if err != nil {
		return nil, err
	}
	entityNameToJoinKeyMap, err := fs.getEntityMaps(requestedFeatureViews)
	if err != nil {
		return nil, err
	}

	// TODO (Ly): This should return empty now
	// Expect no ODFV or Request FV passed in GetOnlineFearuresRequest
	neededRequestData, neededRequestODFVFeatures, err := fs.getNeededRequestData(requestedRequestFeatureViews, requestedOnDemandFeatureViews)
	if err != nil {
		return nil, err
	}

	// TODO: Add a map that contains provided entities + ODFV schema entities + request schema
	// to use for ODFV
	// Remove comments for requestDataFeatures when ODFV is supported
	// requestDataFeatures := make(map[string]*types.RepeatedValue) // TODO (Ly): Should be empty now until ODFV and Request FV are supported
	responseEntities := make(map[string]*types.RepeatedValue)
	for entityName, vals := range entityProtos {
		if _, ok := neededRequestODFVFeatures[entityName]; ok {
			responseEntities[entityName] = vals
			// requestDataFeatures[entityName] = vals
		} else if _, ok = neededRequestData[entityName]; ok {
			// requestDataFeatures[entityName] = vals
		} else {
			if joinKey, ok := entityNameToJoinKeyMap[entityName]; !ok {
				return nil, errors.New(fmt.Sprintf("entityNotFoundException: %s\n%v", entityName, entityNameToJoinKeyMap))
			} else {
				responseEntities[joinKey] = vals
			}
		}
	}

	// TODO (Ly): Skip this validation since we're not supporting ODFV yet

	// err = fs.ensureRequestedDataExist(neededRequestData, neededRequestODFVFeatures, requestDataFeatures)
	// if err != nil {
	// 	return nil, err
	// }

	numOfReturnedFeatures := len(responseEntities) + len(featureRefs)
	onlineFeatureResponse := &serving.GetOnlineFeaturesResponse{Metadata: &serving.GetOnlineFeaturesResponseMetadata{
		FeatureNames: &serving.FeatureList{Val: make([]string, numOfReturnedFeatures)},
	},
		Results: make([]*serving.GetOnlineFeaturesResponse_FeatureVector, numRows),
	}

	// Allocate memory for each GetOnlineFeaturesResponse_FeatureVector
	for index := 0; index < numRows; index++ {
		onlineFeatureResponse.Results[index] = &serving.GetOnlineFeaturesResponse_FeatureVector{Values: make([]*types.Value, numOfReturnedFeatures),
			Statuses:        make([]serving.FieldStatus, numOfReturnedFeatures),
			EventTimestamps: make([]*timestamppb.Timestamp, numOfReturnedFeatures),
		}
	}

	// Add provided entities + ODFV schema entities to response
	fs.populateResponseEntities(onlineFeatureResponse, responseEntities)
	offset := len(responseEntities)

	featureViews := make([]*FeatureView, len(requestedFeatureViews))
	index := 0
	for featureView := range requestedFeatureViews {
		featureViews[index] = featureView
		index += 1
	}

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
		responseEntities[DUMMY_ENTITY_ID] = dummyEntityColumn
	}

	groupedRefs, err := fs.groupFeatureRefs(requestedFeatureViews, responseEntities, entityNameToJoinKeyMap, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	for _, groupRef := range groupedRefs {
		featureData, err := fs.readFromOnlineStore(ctx, groupRef.entityKeys, groupRef.featureViewNames, groupRef.featureNames)
		if err != nil {
			return nil, err
		}
		fs.populateResponseFromFeatureData(featureData,
			groupRef,
			onlineFeatureResponse,
			fvs,
			offset,
		)
		offset += len(groupRef.featureNames)
	}
	// TODO (Ly): ODFV, skip augmentResponseWithOnDemandTransforms
	return onlineFeatureResponse, nil
}

func (fs *FeatureStore) DestructOnlineStore() {
	fs.onlineStore.Destruct()
}

func (fs *FeatureStore) parseKind(kind interface{}) (*ParsedKind, error) {
	if featureList, ok := kind.(*serving.GetOnlineFeaturesRequest_Features); ok {
		return &ParsedKind{features: featureList.Features.GetVal(), featureService: nil}, nil
	}
	if featureServiceRequest, ok := kind.(*serving.GetOnlineFeaturesRequest_FeatureService); ok {
		featureService, err := fs.registry.getFeatureService(fs.config.Project, featureServiceRequest.FeatureService)
		if err != nil {
			return nil, err
		}
		return &ParsedKind{features: nil, featureService: featureService}, nil
	}
	return nil, errors.New("cannot parse kind from GetOnlineFeaturesRequest")
}

/*
	This function returns all feature references from GetOnlineFeaturesRequest.
	If a list of feature references is passed, return it.
	Otherwise, FeatureService was passed, parse this feature service to get a list of FeatureViewProjection and return feature
	references from this list
*/

func (fs *FeatureStore) getFeatures(parsedKind *ParsedKind, allowCache bool) ([]string, error) {

	if parsedKind.featureService != nil {

		var featureViewName string
		features := make([]string, 0)
		for _, featureProjection := range parsedKind.featureService.projections {
			featureViewName = featureProjection.nameToUse()
			for _, feature := range featureProjection.features {
				features = append(features, fmt.Sprintf("%s:%s", featureViewName, feature.name))
			}
		}
		return features, nil
	} else {
		return parsedKind.features, nil
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
func (fs *FeatureStore) getFeatureViewsToUse(parsedKind *ParsedKind, allowCache, hideDummyEntity bool) (map[string]*FeatureView, map[*FeatureView][]string, []*RequestFeatureView, []*OnDemandFeatureView, error) {

	fvs := make(map[string]*FeatureView)
	requestFvs := make(map[string]*RequestFeatureView)
	odFvs := make(map[string]*OnDemandFeatureView)

	featureViews := fs.listFeatureViews(allowCache, hideDummyEntity)
	for _, featureView := range featureViews {
		fvs[featureView.base.name] = featureView
	}

	requestFeatureViews := fs.registry.listRequestFeatureViews(fs.config.Project)
	for _, requestFeatureView := range requestFeatureViews {
		requestFvs[requestFeatureView.base.name] = requestFeatureView
	}

	onDemandFeatureViews := fs.registry.listOnDemandFeatureViews(fs.config.Project)
	for _, onDemandFeatureView := range onDemandFeatureViews {
		odFvs[onDemandFeatureView.base.name] = onDemandFeatureView
	}

	if parsedKind.featureService != nil {
		featureService := parsedKind.featureService

		fvsToUse := make(map[*FeatureView][]string)
		requestFvsToUse := make([]*RequestFeatureView, 0)
		odFvsToUse := make([]*OnDemandFeatureView, 0)

		for _, featureProjection := range featureService.projections {
			// Create copies of FeatureView that may contains the same *FeatureView but
			// each differentiated by a *FeatureViewProjection
			featureViewName := featureProjection.name
			if fv, ok := fvs[featureViewName]; ok {
				base, err := fv.base.withProjection(featureProjection)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				newFv := fv.NewFeatureViewFromBase(base)
				fvsToUse[newFv] = make([]string, len(newFv.base.features))
				for index, feature := range newFv.base.features {
					fvsToUse[newFv][index] = feature.name
				}
			} else if requestFv, ok := requestFvs[featureViewName]; ok {
				base, err := requestFv.base.withProjection(featureProjection)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				requestFvsToUse = append(requestFvsToUse, requestFv.NewRequestFeatureViewFromBase(base))
			} else if odFv, ok := odFvs[featureViewName]; ok {
				base, err := odFv.base.withProjection(featureProjection)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				odFvsToUse = append(odFvsToUse, odFv.NewOnDemandFeatureViewFromBase(base))
			} else {
				return nil, nil, nil, nil, errors.New(fmt.Sprintf("the provided feature service %s contains a reference to a feature view"+
					"%s which doesn't exist. Please make sure that you have created the feature view"+
					"%s and that you have registered it by running \"apply\".", featureService.name, featureViewName, featureViewName))
			}
		}
		return fvs, fvsToUse, requestFvsToUse, odFvsToUse, nil
	}

	fvsToUse := make(map[*FeatureView][]string)
	requestFvsToUse := make([]*RequestFeatureView, 0)
	odFvsToUse := make([]*OnDemandFeatureView, 0)

	for _, featureRef := range parsedKind.features {
		featureViewName, featureName, err := parseFeatureReference(featureRef)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if fv, ok := fvs[featureViewName]; ok {
			fvsToUse[fv] = append(fvsToUse[fv], featureName)
		} else if requestFv, ok := requestFvs[featureViewName]; ok {
			requestFvsToUse = append(requestFvsToUse, requestFv)
		} else if odFv, ok := odFvs[featureViewName]; ok {
			odFvsToUse = append(odFvsToUse, odFv)
		} else {
			return nil, nil, nil, nil, errors.New(fmt.Sprintf("feature view %s doesn't exist. Please make sure that you have created the"+
				" feature view %s and that you have registered it by running \"apply\".", featureViewName, featureViewName))
		}
	}

	return fvs, fvsToUse, requestFvsToUse, odFvsToUse, nil
}

func (fs *FeatureStore) getEntityMaps(requestedFeatureViews map[*FeatureView][]string) (map[string]string, error) {

	entityNameToJoinKeyMap := make(map[string]string)
	var entityNames map[string]struct{}
	var entityName string
	var joinKeyMap map[string]string
	var featureView *FeatureView

	entities := fs.listEntities(true, false)

	for _, entity := range entities {
		entityNameToJoinKeyMap[entity.name] = entity.joinKey
	}

	for featureView = range requestedFeatureViews {

		entityNames = featureView.entities
		joinKeyMap = featureView.base.projection.joinKeyMap
		for entityName = range entityNames {

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
		return 0, errors.New("valueError: All entity rows must have the same columns.")
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
			for featureName := range collidedFeatureRefs {
				collidedFeatureRefList[index] = featureName
				index += 1
			}
			return NewFeatureNameCollisionError(collidedFeatureRefList, fullFeatureNames)
		}
	} else {
		for _, featureRef := range featureRefs {
			_, featureName, err := parseFeatureReference(featureRef)
			if err != nil {
				return err
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
				_, featureName, err := parseFeatureReference(featureRef)
				if err != nil {
					return err
				}
				if _, ok := collidedFeatureRefs[featureName]; ok {
					collidedFeatureRefList = append(collidedFeatureRefList, featureRef)
				}

			}
			return errors.New(fmt.Sprintf("featureNameCollisionError: %s; %t", strings.Join(collidedFeatureRefList, ", "), fullFeatureNames))
		}
	}
	return nil
}

func (fs *FeatureStore) getNeededRequestData(requestedRequestFeatureViews []*RequestFeatureView,
	requestedOnDemandFeatureViews []*OnDemandFeatureView) (map[string]struct{}, map[string]struct{}, error) {
	neededRequestData := make(map[string]struct{})
	neededRequestFvFeatures := make(map[string]struct{})

	for _, onDemandFeatureView := range requestedOnDemandFeatureViews {
		requestSchema := onDemandFeatureView.getRequestDataSchema()
		for fieldName := range requestSchema {
			neededRequestData[fieldName] = struct{}{}
		}
	}

	for _, requestFeatureView := range requestedRequestFeatureViews {
		for _, feature := range requestFeatureView.base.features {
			neededRequestFvFeatures[feature.name] = struct{}{}
		}
	}

	return neededRequestData, neededRequestFvFeatures, nil
}

func (fs *FeatureStore) ensureRequestedDataExist(neededRequestData map[string]struct{},
	neededRequestFvFeatures map[string]struct{},
	requestDataFeatures map[string]*types.RepeatedValue) error {
	// TODO (Ly): Review: Skip checking even if composite set of
	// neededRequestData neededRequestFvFeatures is different from
	// request_data_features but same length?
	if len(neededRequestData)+len(neededRequestFvFeatures) != len(requestDataFeatures) {
		missingFeatures := make([]string, 0)
		for feature := range neededRequestData {
			if _, ok := requestDataFeatures[feature]; !ok {
				missingFeatures = append(missingFeatures, feature)
			}
		}
		for feature := range neededRequestFvFeatures {
			if _, ok := requestDataFeatures[feature]; !ok {
				missingFeatures = append(missingFeatures, feature)
			}
		}
		return errors.New(fmt.Sprintf("requestDataNotFoundInEntityRowsException: %s", strings.Join(missingFeatures, ", ")))
	}
	return nil
}

func (fs *FeatureStore) checkOutsideTtl(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	return currentTimestamp.GetSeconds()-featureTimestamp.GetSeconds() > ttl.Seconds
}

func (fs *FeatureStore) populateResponseEntities(response *serving.GetOnlineFeaturesResponse, responseEntities map[string]*types.RepeatedValue) {
	timeStamp := timestamppb.Now()
	featureIndex := 0
	for entityName, values := range responseEntities {
		response.Metadata.FeatureNames.Val[featureIndex] = entityName

		for rowIndex, value := range values.GetVal() {
			featureVector := response.Results[rowIndex]
			featureTimeStamp := timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos}
			featureValue := types.Value{Val: value.Val}
			featureVector.Values[featureIndex] = &featureValue
			featureVector.Statuses[featureIndex] = serving.FieldStatus_PRESENT
			featureVector.EventTimestamps[featureIndex] = &featureTimeStamp
		}
		featureIndex += 1
	}
}

func (fs *FeatureStore) readFromOnlineStore(ctx context.Context, entityRows []*types.EntityKey,
	requestedFeatureViewNames []string,
	requestedFeatureNames []string,
) ([][]FeatureData, error) {
	numRows := len(entityRows)
	entityRowsValue := make([]types.EntityKey, numRows)
	for index, entityKey := range entityRows {
		entityRowsValue[index] = types.EntityKey{JoinKeys: entityKey.JoinKeys, EntityValues: entityKey.EntityValues}
	}
	return fs.onlineStore.OnlineRead(ctx, entityRowsValue, requestedFeatureViewNames, requestedFeatureNames)
}

func (fs *FeatureStore) populateResponseFromFeatureData(featureData2D [][]FeatureData,
	groupRef *GroupedFeaturesPerEntitySet,
	onlineFeaturesResponse *serving.GetOnlineFeaturesResponse,
	fvs map[string]*FeatureView,
	offset int) {

	numFeatures := len(groupRef.featureResponseMeta)

	var value *types.Value
	var status serving.FieldStatus
	var eventTimeStamp *timestamppb.Timestamp
	var featureData *FeatureData
	var fv *FeatureView
	var featureViewName string
	var indicesToUse []int

	for featureIndex := 0; featureIndex < numFeatures; featureIndex++ {
		indicesToUse = groupRef.indices[groupRef.indicesMapper[featureIndex]]
		onlineFeaturesResponse.Metadata.FeatureNames.Val[offset+featureIndex] = groupRef.featureResponseMeta[featureIndex]
		for rowIndex, rowEntityIndex := range indicesToUse {
			if featureData2D[rowEntityIndex] == nil {
				value = nil
				status = serving.FieldStatus_NOT_FOUND
				eventTimeStamp = &timestamppb.Timestamp{}
			} else {
				featureData = &featureData2D[rowEntityIndex][featureIndex]
				eventTimeStamp = &timestamppb.Timestamp{Seconds: featureData.timestamp.Seconds, Nanos: featureData.timestamp.Nanos}
				featureViewName = featureData.reference.FeatureViewName
				fv = fvs[featureViewName]
				if _, ok := featureData.value.Val.(*types.Value_NullVal); ok {
					value = nil
					status = serving.FieldStatus_NOT_FOUND
				} else if fs.checkOutsideTtl(eventTimeStamp, timestamppb.Now(), fv.ttl) {
					value = &types.Value{Val: featureData.value.Val}
					status = serving.FieldStatus_OUTSIDE_MAX_AGE
				} else {
					value = &types.Value{Val: featureData.value.Val}
					status = serving.FieldStatus_PRESENT
				}
			}
			onlineFeaturesResponse.Results[rowIndex].Values[offset+featureIndex] = value
			onlineFeaturesResponse.Results[rowIndex].Statuses[offset+featureIndex] = status
			onlineFeaturesResponse.Results[rowIndex].EventTimestamps[offset+featureIndex] = eventTimeStamp
		}
	}

}

// TODO (Ly): Complete this function + ODFV
func (fs *FeatureStore) augmentResponseWithOnDemandTransforms(onlineFeaturesResponse *serving.GetOnlineFeaturesResponse,
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
		viewName, featureName, err := parseFeatureReference(featureRef)
		if err != nil {

		}

		if _, ok := requestedOdfvMap[viewName]; ok {

			viewNameToUse := requestedOdfvMap[viewName].base.projection.nameToUse()
			if fullFeatureNames {
				featureName = fmt.Sprintf("%s__%s", viewNameToUse, featureName)
			}
			odfvFeatureRefs[viewName] = append(odfvFeatureRefs[viewName], featureName)
		}
	}
}

func (fs *FeatureStore) dropUnneededColumns(onlineFeaturesResponse *serving.GetOnlineFeaturesResponse,
	requestedResultRowNames map[string]struct{}) {
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

func (fs *FeatureStore) listFeatureViews(allowCache, hideDummyEntity bool) []*FeatureView {
	featureViews := fs.registry.listFeatureViews(fs.config.Project)
	for _, featureView := range featureViews {
		if _, ok := featureView.entities[DUMMY_ENTITY_NAME]; ok && hideDummyEntity {
			featureView.entities = make(map[string]struct{})
		}
	}
	return featureViews
}

func (fs *FeatureStore) listEntities(allowCache, hideDummyEntity bool) []*Entity {

	allEntities := fs.registry.listEntities(fs.config.Project)
	entities := make([]*Entity, 0)
	for _, entity := range allEntities {
		if entity.name != DUMMY_ENTITY_NAME || !hideDummyEntity {
			entities = append(entities, entity)
		}
	}
	return entities
}

func (fs *FeatureStore) getFvEntityValues(fv *FeatureView,
	joinKeyValues map[string]*types.RepeatedValue,
	entityNameToJoinKeyMap map[string]string) map[string]*types.RepeatedValue {

	fvJoinKeys := make(map[string]struct{})
	for entityName := range fv.entities {
		fvJoinKeys[entityNameToJoinKeyMap[entityName]] = struct{}{}
	}

	aliasToJoinKeyMap := make(map[string]string)
	for k, v := range fv.base.projection.joinKeyMap {
		aliasToJoinKeyMap[v] = k
	}

	entityValues := make(map[string]*types.RepeatedValue)

	for k, v := range joinKeyValues {
		entityKey := k
		if _, ok := aliasToJoinKeyMap[k]; ok {
			entityKey = aliasToJoinKeyMap[k]
		}
		if _, ok := fvJoinKeys[entityKey]; ok {
			entityValues[entityKey] = v
		}
	}

	return entityValues
}

/* entityValues are rows of the same feature view */

func serializeEntityKeySet(entityValues []*types.EntityKey) string {
	if len(entityValues) == 0 {
		return ""
	}
	joinKeys := make([]string, len(entityValues[0].JoinKeys))
	for _, entityKey := range entityValues {
		for index, joinKey := range entityKey.JoinKeys {
			joinKeys[index] = joinKey
		}
		break
	}
	byteEntitySet := []byte{}
	sort.Strings(joinKeys)
	for _, key := range joinKeys {
		byteEntitySet = append(byteEntitySet, []byte(key)...)
		byteEntitySet = append(byteEntitySet, byte(0))
	}
	return string(byteEntitySet)
}

func (fs *FeatureStore) getEntityKeysFromFeatureView(fv *FeatureView,
	joinKeyValues map[string]*types.RepeatedValue,
	entityNameToJoinKeyMap map[string]string) []*types.EntityKey {
	fvEntityValues := fs.getFvEntityValues(fv, joinKeyValues, entityNameToJoinKeyMap)
	keys := make([]string, len(fvEntityValues))
	index := 0
	var numRows int
	for k, v := range fvEntityValues {
		keys[index] = k
		index += 1
		numRows = len(v.Val)
	}
	sort.Strings(keys)
	entityKeys := make([]*types.EntityKey, numRows)
	numJoinKeys := len(keys)
	// Construct each EntityKey object
	for index = 0; index < numRows; index++ {
		entityKeys[index] = &types.EntityKey{JoinKeys: keys, EntityValues: make([]*types.Value, numJoinKeys)}
	}

	for colIndex, key := range keys {
		for index, value := range fvEntityValues[key].GetVal() {
			entityKeys[index].EntityValues[colIndex] = value
		}
	}
	return entityKeys
}

func (fs *FeatureStore) getUniqueEntities(entityKeys []*types.EntityKey,
) ([]*types.EntityKey, [][]int, error) {

	rowise := make(map[string]*entityKeyRow)
	// start here
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
	index := 0
	for _, ekRow := range rowise {
		uniqueEntityKeys[index] = ekRow.entityKey
		indices[index] = ekRow.rowIndices
		index += 1
	}
	return uniqueEntityKeys, indices, nil
}

/*
Group feature views that share the same set of join keys. For each group, we store only unique rows and save indices to retrieve those
rows for each requested feature
*/

func (fs *FeatureStore) groupFeatureRefs(requestedFeatureViews map[*FeatureView][]string,
	joinKeyValues map[string]*types.RepeatedValue,
	entityNameToJoinKeyMap map[string]string,
	fullFeatureNames bool,
) (map[string]*GroupedFeaturesPerEntitySet,
	error,
) {
	fvFeatures := make(map[string]*GroupedFeaturesPerEntitySet)
	uniqueRowsPerEntitySet := make(map[string]map[string]int)
	var featureIndex int
	for fv, featureNames := range requestedFeatureViews {
		entityKeys := fs.getEntityKeysFromFeatureView(fv, joinKeyValues, entityNameToJoinKeyMap)
		indices := make([]int, len(entityKeys))
		entityKeySet := serializeEntityKeySet(entityKeys)
		if _, ok := uniqueRowsPerEntitySet[entityKeySet]; !ok {
			uniqueRowsPerEntitySet[entityKeySet] = make(map[string]int)
		}
		if _, ok := fvFeatures[entityKeySet]; !ok {
			// Feature names should be unique per feature view to pass validateFeatureRefs
			fvFeatures[entityKeySet] = &GroupedFeaturesPerEntitySet{indicesMapper: make(map[int]int)}
		}
		for index, entityKey := range entityKeys {
			serializedRow, err := serializeEntityKey(entityKey)
			if err != nil {
				return nil, err
			}
			rowKey := string(*serializedRow)
			if _, ok := uniqueRowsPerEntitySet[entityKeySet][rowKey]; !ok {
				uniqueRowsPerEntitySet[entityKeySet][rowKey] = len(uniqueRowsPerEntitySet[entityKeySet])
				fvFeatures[entityKeySet].entityKeys = append(fvFeatures[entityKeySet].entityKeys, entityKey)
			}
			indices[index] = uniqueRowsPerEntitySet[entityKeySet][rowKey]
		}

		for _, featureName := range featureNames {
			featureIndex = len(fvFeatures[entityKeySet].featureNames)
			fvFeatures[entityKeySet].featureNames = append(fvFeatures[entityKeySet].featureNames, featureName)
			fvFeatures[entityKeySet].featureViewNames = append(fvFeatures[entityKeySet].featureViewNames, fv.base.name)
			fvFeatures[entityKeySet].featureResponseMeta = append(fvFeatures[entityKeySet].featureResponseMeta,
				getFeatureResponseMeta(fv.base.projection.nameToUse(), featureName, fullFeatureNames))
			fvFeatures[entityKeySet].indicesMapper[featureIndex] = len(fvFeatures[entityKeySet].indices)
		}
		fvFeatures[entityKeySet].indices = append(fvFeatures[entityKeySet].indices, indices)
	}
	return fvFeatures, nil
}

func (fs *FeatureStore) getFeatureView(project, featureViewName string, allowCache, hideDummyEntity bool) (*FeatureView, error) {
	fv, err := fs.registry.getFeatureView(fs.config.Project, featureViewName)
	if err != nil {
		return nil, err
	}
	if _, ok := fv.entities[DUMMY_ENTITY_NAME]; ok && hideDummyEntity {
		fv.entities = make(map[string]struct{})
	}
	return fv, nil
}

func parseFeatureReference(featureRef string) (featureViewName, featureName string, e error) {
	parsedFeatureName := strings.Split(featureRef, ":")

	if len(parsedFeatureName) == 0 {
		e = errors.New("featureReference should be in the format: 'FeatureViewName:FeatureName'")
	} else if len(parsedFeatureName) == 1 {
		featureName = parsedFeatureName[0]
	} else {
		featureViewName = parsedFeatureName[0]
		featureName = parsedFeatureName[1]
	}
	return
}

func getFeatureResponseMeta(featureNameAlias string, featureName string, fullFeatureNames bool) string {
	if fullFeatureNames {
		return fmt.Sprintf("%s__%s", featureNameAlias, featureName)
	} else {
		return featureName
	}
}
