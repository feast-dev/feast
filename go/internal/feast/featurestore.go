package feast

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FeatureStore struct {
	config                *RepoConfig
	registry              *Registry
	onlineStore           OnlineStore
	tranformationCallback TransformationCallback
}

// A Features struct specifies a list of features to be retrieved from the online store. These features
// can be specified either as a list of string feature references or as a feature service. String
// feature references must have format "feature_view:feature", e.g. "customer_fv:daily_transactions".
type Features struct {
	FeaturesRefs   []string
	FeatureService *FeatureService
}

/*
	FeatureVector type represent result of retrieving single feature for multiple rows.
	It can be imagined as a column in output dataframe / table.
	It contains of feature name, list of values (across all rows),
	list of statuses and list of timestamp. All these lists have equal length.
	And this length is also equal to number of entity rows received in request.
*/
type FeatureVector struct {
	Name       string
	Values     arrow.Array
	Statuses   []serving.FieldStatus
	Timestamps []*timestamppb.Timestamp
}

type featureViewAndRefs struct {
	view        *FeatureView
	featureRefs []string
}

/*
	We group all features from a single request by entities they attached to.
	Thus, we will be able to call online retrieval per entity and not per each feature view.
	In this struct we collect all features and views that belongs to a group.
	We also store here projected entity keys (only ones that needed to retrieve these features)
	and indexes to map result of retrieval into output response.
*/
type GroupedFeaturesPerEntitySet struct {
	// A list of requested feature references of the form featureViewName:featureName that share this entity set
	featureNames     []string
	featureViewNames []string
	// full feature references as they supposed to appear in response
	aliasedFeatureNames []string
	// Entity set as a list of EntityKeys to pass to OnlineRead
	entityKeys []*prototypes.EntityKey
	// Reversed mapping to project result of retrieval from storage to response
	indices [][]int
}

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config *RepoConfig, callback TransformationCallback) (*FeatureStore, error) {
	onlineStore, err := NewOnlineStore(config)
	if err != nil {
		return nil, err
	}

	registry, err := NewRegistry(config.GetRegistryConfig(), config.RepoPath)
	if err != nil {
		return nil, err
	}
	registry.initializeRegistry()

	return &FeatureStore{
		config:                config,
		registry:              registry,
		onlineStore:           onlineStore,
		tranformationCallback: callback,
	}, nil
}

// TODO: Review all functions that use ODFV and Request FV since these have not been tested
// ToDo: Split GetOnlineFeatures interface into two: GetOnlinFeaturesByFeatureService and GetOnlineFeaturesByFeatureRefs
func (fs *FeatureStore) GetOnlineFeatures(
	ctx context.Context,
	featureRefs []string,
	featureService *FeatureService,
	joinKeyToEntityValues map[string]*prototypes.RepeatedValue,
	requestData map[string]*prototypes.RepeatedValue,
	fullFeatureNames bool) ([]*FeatureVector, error) {
	fvs, odFvs, err := fs.listAllViews()
	if err != nil {
		return nil, err
	}

	var requestedFeatureViews []*featureViewAndRefs
	var requestedOnDemandFeatureViews []*OnDemandFeatureView
	if featureService != nil {
		requestedFeatureViews, requestedOnDemandFeatureViews, err =
			getFeatureViewsToUseByService(featureService, fvs, odFvs)
	} else {
		requestedFeatureViews, requestedOnDemandFeatureViews, err =
			getFeatureViewsToUseByFeatureRefs(featureRefs, fvs, odFvs)
	}
	if err != nil {
		return nil, err
	}

	entityNameToJoinKeyMap, expectedJoinKeysSet, err := fs.getEntityMaps(requestedFeatureViews)
	if err != nil {
		return nil, err
	}

	err = validateFeatureRefs(requestedFeatureViews, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	numRows, err := validateEntityValues(joinKeyToEntityValues, requestData, expectedJoinKeysSet)
	if err != nil {
		return nil, err
	}

	err = ensureRequestedDataExist(requestedOnDemandFeatureViews, requestData)
	if err != nil {
		return nil, err
	}

	result := make([]*FeatureVector, 0)
	arrowMemory := memory.NewGoAllocator()
	featureViews := make([]*FeatureView, len(requestedFeatureViews))
	index := 0
	for _, featuresAndView := range requestedFeatureViews {
		featureViews[index] = featuresAndView.view
		index += 1
	}

	entitylessCase := false
	for _, featureView := range featureViews {
		if _, ok := featureView.Entities[DUMMY_ENTITY_NAME]; ok {
			entitylessCase = true
			break
		}
	}

	if entitylessCase {
		dummyEntityColumn := &prototypes.RepeatedValue{Val: make([]*prototypes.Value, numRows)}
		for index := 0; index < numRows; index++ {
			dummyEntityColumn.Val[index] = &DUMMY_ENTITY
		}
		joinKeyToEntityValues[DUMMY_ENTITY_ID] = dummyEntityColumn
	}

	groupedRefs, err := groupFeatureRefs(requestedFeatureViews, joinKeyToEntityValues, entityNameToJoinKeyMap, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	for _, groupRef := range groupedRefs {
		featureData, err := fs.readFromOnlineStore(ctx, groupRef.entityKeys, groupRef.featureViewNames, groupRef.featureNames)
		if err != nil {
			return nil, err
		}

		vectors, err := fs.transposeFeatureRowsIntoColumns(
			featureData,
			groupRef,
			requestedFeatureViews,
			arrowMemory,
			numRows,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, vectors...)
	}

	onDemandFeatures, err := augmentResponseWithOnDemandTransforms(
		requestedOnDemandFeatureViews,
		requestData,
		joinKeyToEntityValues,
		result,
		fs.tranformationCallback,
		arrowMemory,
		numRows,
		fullFeatureNames,
	)
	if err != nil {
		return nil, err
	}

	result = append(result, onDemandFeatures...)

	result, err = keepOnlyRequestedFeatures(result, featureRefs, featureService, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	entityColumns, err := entitiesToFeatureVectors(joinKeyToEntityValues, arrowMemory, numRows)
	result = append(entityColumns, result...)
	return result, nil
}

func (fs *FeatureStore) DestructOnlineStore() {
	fs.onlineStore.Destruct()
}

// ParseFeatures parses the kind field of a GetOnlineFeaturesRequest protobuf message
// and populates a Features struct with the result.
func (fs *FeatureStore) ParseFeatures(kind interface{}) (*Features, error) {
	if featureList, ok := kind.(*serving.GetOnlineFeaturesRequest_Features); ok {
		return &Features{FeaturesRefs: featureList.Features.GetVal(), FeatureService: nil}, nil
	}
	if featureServiceRequest, ok := kind.(*serving.GetOnlineFeaturesRequest_FeatureService); ok {
		featureService, err := fs.registry.getFeatureService(fs.config.Project, featureServiceRequest.FeatureService)
		if err != nil {
			return nil, err
		}
		return &Features{FeaturesRefs: nil, FeatureService: featureService}, nil
	}
	return nil, errors.New("cannot parse kind from GetOnlineFeaturesRequest")
}

func (fs *FeatureStore) GetFeatureService(name string) (*FeatureService, error) {
	return fs.registry.getFeatureService(fs.config.Project, name)
}

/*
	Return
		(1) requested feature views and features grouped per view
		(2) requested on demand feature views
	existed in the registry

*/
func getFeatureViewsToUseByService(
	featureService *FeatureService,
	featureViews map[string]*FeatureView,
	onDemandFeatureViews map[string]*OnDemandFeatureView) ([]*featureViewAndRefs, []*OnDemandFeatureView, error) {

	viewNameToViewAndRefs := make(map[string]*featureViewAndRefs)
	odFvsToUse := make([]*OnDemandFeatureView, 0)

	for _, featureProjection := range featureService.Projections {
		// Create copies of FeatureView that may contains the same *FeatureView but
		// each differentiated by a *FeatureViewProjection
		featureViewName := featureProjection.Name
		if fv, ok := featureViews[featureViewName]; ok {
			base, err := fv.Base.withProjection(featureProjection)
			if err != nil {
				return nil, nil, err
			}
			if _, ok := viewNameToViewAndRefs[featureProjection.NameToUse()]; !ok {
				viewNameToViewAndRefs[featureProjection.NameToUse()] = &featureViewAndRefs{
					view:        fv.NewFeatureViewFromBase(base),
					featureRefs: []string{},
				}
			}

			for _, feature := range featureProjection.Features {
				viewNameToViewAndRefs[featureProjection.NameToUse()].featureRefs =
					addStringIfNotContains(viewNameToViewAndRefs[featureProjection.NameToUse()].featureRefs,
						feature.name)
			}

		} else if odFv, ok := onDemandFeatureViews[featureViewName]; ok {
			projectedOdFv, err := odFv.NewWithProjection(featureProjection)
			if err != nil {
				return nil, nil, err
			}
			odFvsToUse = append(odFvsToUse, projectedOdFv)
			err = extractOdFvDependencies(
				projectedOdFv,
				featureViews,
				viewNameToViewAndRefs)
			if err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, fmt.Errorf("the provided feature service %s contains a reference to a feature view"+
				"%s which doesn't exist, please make sure that you have created the feature view"+
				"%s and that you have registered it by running \"apply\"", featureService.name, featureViewName, featureViewName)
		}
	}

	fvsToUse := make([]*featureViewAndRefs, 0)
	for _, viewAndRef := range viewNameToViewAndRefs {
		fvsToUse = append(fvsToUse, viewAndRef)
	}

	return fvsToUse, odFvsToUse, nil
}

/*
	Return
		(1) requested feature views and features grouped per view
		(2) requested on demand feature views
	existed in the registry
*/
func getFeatureViewsToUseByFeatureRefs(
	features []string,
	featureViews map[string]*FeatureView,
	onDemandFeatureViews map[string]*OnDemandFeatureView) ([]*featureViewAndRefs, []*OnDemandFeatureView, error) {
	viewNameToViewAndRefs := make(map[string]*featureViewAndRefs)
	odFvToFeatures := make(map[string][]string)

	for _, featureRef := range features {
		featureViewName, featureName, err := ParseFeatureReference(featureRef)
		if err != nil {
			return nil, nil, err
		}
		if fv, ok := featureViews[featureViewName]; ok {
			if viewAndRef, ok := viewNameToViewAndRefs[fv.Base.Name]; ok {
				viewAndRef.featureRefs = addStringIfNotContains(viewAndRef.featureRefs, featureName)
			} else {
				viewNameToViewAndRefs[fv.Base.Name] = &featureViewAndRefs{
					view:        fv,
					featureRefs: []string{featureName},
				}
			}
		} else if odfv, ok := onDemandFeatureViews[featureViewName]; ok {
			if _, ok := odFvToFeatures[odfv.base.Name]; !ok {
				odFvToFeatures[odfv.base.Name] = []string{featureName}
			} else {
				odFvToFeatures[odfv.base.Name] = append(
					odFvToFeatures[odfv.base.Name], featureName)
			}
		} else {
			return nil, nil, fmt.Errorf("feature view %s doesn't exist, please make sure that you have created the"+
				" feature view %s and that you have registered it by running \"apply\"", featureViewName, featureViewName)
		}
	}

	odFvsToUse := make([]*OnDemandFeatureView, 0)

	for odFvName, featureNames := range odFvToFeatures {
		projectedOdFv, err := onDemandFeatureViews[odFvName].projectWithFeatures(featureNames)
		if err != nil {
			return nil, nil, err
		}

		err = extractOdFvDependencies(
			projectedOdFv,
			featureViews,
			viewNameToViewAndRefs)
		if err != nil {
			return nil, nil, err
		}
		odFvsToUse = append(odFvsToUse, projectedOdFv)
	}

	fvsToUse := make([]*featureViewAndRefs, 0)
	for _, viewAndRefs := range viewNameToViewAndRefs {
		fvsToUse = append(fvsToUse, viewAndRefs)
	}

	return fvsToUse, odFvsToUse, nil
}

func extractOdFvDependencies(
	odFv *OnDemandFeatureView,
	sourceFvs map[string]*FeatureView,
	requestedFeatures map[string]*featureViewAndRefs,
) error {

	for _, sourceFvProjection := range odFv.sourceFeatureViewProjections {
		fv := sourceFvs[sourceFvProjection.Name]
		base, err := fv.Base.withProjection(sourceFvProjection)
		if err != nil {
			return err
		}
		newFv := fv.NewFeatureViewFromBase(base)

		if _, ok := requestedFeatures[sourceFvProjection.NameToUse()]; !ok {
			requestedFeatures[sourceFvProjection.NameToUse()] = &featureViewAndRefs{
				view:        newFv,
				featureRefs: []string{},
			}
		}

		for _, feature := range sourceFvProjection.Features {
			requestedFeatures[sourceFvProjection.NameToUse()].featureRefs = addStringIfNotContains(
				requestedFeatures[sourceFvProjection.NameToUse()].featureRefs, feature.name)
		}
	}

	return nil
}

func (fs *FeatureStore) listAllViews() (map[string]*FeatureView, map[string]*OnDemandFeatureView, error) {
	fvs := make(map[string]*FeatureView)
	odFvs := make(map[string]*OnDemandFeatureView)

	featureViews, err := fs.ListFeatureViews()
	if err != nil {
		return nil, nil, err
	}
	for _, featureView := range featureViews {
		fvs[featureView.Base.Name] = featureView
	}

	onDemandFeatureViews, err := fs.registry.listOnDemandFeatureViews(fs.config.Project)
	if err != nil {
		return nil, nil, err
	}
	for _, onDemandFeatureView := range onDemandFeatureViews {
		odFvs[onDemandFeatureView.base.Name] = onDemandFeatureView
	}
	return fvs, odFvs, nil
}

func addStringIfNotContains(slice []string, element string) []string {
	found := false
	for _, item := range slice {
		if element == item {
			found = true
		}
	}
	if !found {
		slice = append(slice, element)
	}
	return slice
}

func (fs *FeatureStore) getEntityMaps(requestedFeatureViews []*featureViewAndRefs) (map[string]string, map[string]interface{}, error) {
	entityNameToJoinKeyMap := make(map[string]string)
	expectedJoinKeysSet := make(map[string]interface{})

	entities, err := fs.ListEntities(false)
	if err != nil {
		return nil, nil, err
	}
	entitiesByName := make(map[string]*Entity)

	for _, entity := range entities {
		entitiesByName[entity.Name] = entity
	}

	for _, featuresAndView := range requestedFeatureViews {
		featureView := featuresAndView.view
		var joinKeyToAliasMap map[string]string
		if featureView.Base.Projection != nil && featureView.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = featureView.Base.Projection.JoinKeyMap
		} else {
			joinKeyToAliasMap = map[string]string{}
		}

		for entityName := range featureView.Entities {
			joinKey := entitiesByName[entityName].JoinKey
			entityNameToJoinKeyMap[entityName] = joinKey

			if alias, ok := joinKeyToAliasMap[joinKey]; ok {
				expectedJoinKeysSet[alias] = nil
			} else {
				expectedJoinKeysSet[joinKey] = nil
			}
		}
	}
	return entityNameToJoinKeyMap, expectedJoinKeysSet, nil
}

func validateEntityValues(joinKeyValues map[string]*prototypes.RepeatedValue,
	requestData map[string]*prototypes.RepeatedValue,
	expectedJoinKeysSet map[string]interface{}) (int, error) {
	numRows := -1

	for joinKey, values := range joinKeyValues {
		if _, ok := expectedJoinKeysSet[joinKey]; !ok {
			requestData[joinKey] = values
			delete(joinKeyValues, joinKey)
			// ToDo: when request data will be passed correctly (not as part of entity rows)
			// ToDo: throw this error instead
			// return 0, fmt.Errorf("JoinKey is not expected in this request: %s\n%v", JoinKey, expectedJoinKeysSet)
		} else {
			if numRows < 0 {
				numRows = len(values.Val)
			} else if len(values.Val) != numRows {
				return -1, errors.New("valueError: All entity rows must have the same columns")
			}

		}
	}

	return numRows, nil
}

func validateFeatureRefs(requestedFeatures []*featureViewAndRefs, fullFeatureNames bool) error {
	featureRefCounter := make(map[string]int)
	featureRefs := make([]string, 0)
	for _, viewAndFeatures := range requestedFeatures {
		for _, feature := range viewAndFeatures.featureRefs {
			projectedViewName := viewAndFeatures.view.Base.Name
			if viewAndFeatures.view.Base.Projection != nil {
				projectedViewName = viewAndFeatures.view.Base.Projection.NameToUse()
			}

			featureRefs = append(featureRefs,
				fmt.Sprintf("%s:%s", projectedViewName, feature))
		}
	}

	for _, featureRef := range featureRefs {
		if fullFeatureNames {
			featureRefCounter[featureRef]++
		} else {
			_, featureName, _ := ParseFeatureReference(featureRef)
			featureRefCounter[featureName]++
		}

	}
	for featureName, occurrences := range featureRefCounter {
		if occurrences == 1 {
			delete(featureRefCounter, featureName)
		}
	}
	if len(featureRefCounter) >= 1 {
		collidedFeatureRefs := make([]string, 0)
		for collidedFeatureRef := range featureRefCounter {
			if fullFeatureNames {
				collidedFeatureRefs = append(collidedFeatureRefs, collidedFeatureRef)
			} else {
				for _, featureRef := range featureRefs {
					_, featureName, _ := ParseFeatureReference(featureRef)
					if featureName == collidedFeatureRef {
						collidedFeatureRefs = append(collidedFeatureRefs, featureRef)
					}
				}
			}
		}
		return featureNameCollisionError{collidedFeatureRefs, fullFeatureNames}
	}

	return nil
}

func (fs *FeatureStore) checkOutsideTtl(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	return currentTimestamp.GetSeconds()-featureTimestamp.GetSeconds() > ttl.Seconds
}

func (fs *FeatureStore) readFromOnlineStore(ctx context.Context, entityRows []*prototypes.EntityKey,
	requestedFeatureViewNames []string,
	requestedFeatureNames []string,
) ([][]FeatureData, error) {
	numRows := len(entityRows)
	entityRowsValue := make([]*prototypes.EntityKey, numRows)
	for index, entityKey := range entityRows {
		entityRowsValue[index] = &prototypes.EntityKey{JoinKeys: entityKey.JoinKeys, EntityValues: entityKey.EntityValues}
	}
	return fs.onlineStore.OnlineRead(ctx, entityRowsValue, requestedFeatureViewNames, requestedFeatureNames)
}

func (fs *FeatureStore) transposeFeatureRowsIntoColumns(featureData2D [][]FeatureData,
	groupRef *GroupedFeaturesPerEntitySet,
	requestedFeatureViews []*featureViewAndRefs,
	arrowAllocator memory.Allocator,
	numRows int) ([]*FeatureVector, error) {

	numFeatures := len(groupRef.aliasedFeatureNames)
	fvs := make(map[string]*FeatureView)
	for _, viewAndRefs := range requestedFeatureViews {
		fvs[viewAndRefs.view.Base.Name] = viewAndRefs.view
	}

	var value *prototypes.Value
	var status serving.FieldStatus
	var eventTimeStamp *timestamppb.Timestamp
	var featureData *FeatureData
	var fv *FeatureView
	var featureViewName string

	vectors := make([]*FeatureVector, 0)

	for featureIndex := 0; featureIndex < numFeatures; featureIndex++ {
		currentVector := &FeatureVector{
			Name:       groupRef.aliasedFeatureNames[featureIndex],
			Statuses:   make([]serving.FieldStatus, numRows),
			Timestamps: make([]*timestamppb.Timestamp, numRows),
		}
		vectors = append(vectors, currentVector)
		protoValues := make([]*prototypes.Value, numRows)

		for rowEntityIndex, outputIndexes := range groupRef.indices {
			if featureData2D[rowEntityIndex] == nil {
				value = nil
				status = serving.FieldStatus_NOT_FOUND
				eventTimeStamp = &timestamppb.Timestamp{}
			} else {
				featureData = &featureData2D[rowEntityIndex][featureIndex]
				eventTimeStamp = &timestamppb.Timestamp{Seconds: featureData.timestamp.Seconds, Nanos: featureData.timestamp.Nanos}
				featureViewName = featureData.reference.FeatureViewName
				fv = fvs[featureViewName]
				if _, ok := featureData.value.Val.(*prototypes.Value_NullVal); ok {
					value = nil
					status = serving.FieldStatus_NOT_FOUND
				} else if fs.checkOutsideTtl(eventTimeStamp, timestamppb.Now(), fv.Ttl) {
					value = &prototypes.Value{Val: featureData.value.Val}
					status = serving.FieldStatus_OUTSIDE_MAX_AGE
				} else {
					value = &prototypes.Value{Val: featureData.value.Val}
					status = serving.FieldStatus_PRESENT
				}
			}
			for _, rowIndex := range outputIndexes {
				protoValues[rowIndex] = value
				currentVector.Statuses[rowIndex] = status
				currentVector.Timestamps[rowIndex] = eventTimeStamp
			}
		}
		arrowValues, err := types.ProtoValuesToArrowArray(protoValues, arrowAllocator, numRows)
		if err != nil {
			return nil, err
		}
		currentVector.Values = arrowValues
	}

	return vectors, nil

}

func keepOnlyRequestedFeatures(
	vectors []*FeatureVector,
	requestedFeatureRefs []string,
	featureService *FeatureService,
	fullFeatureNames bool) ([]*FeatureVector, error) {
	vectorsByName := make(map[string]*FeatureVector)
	expectedVectors := make([]*FeatureVector, 0)

	for _, vector := range vectors {
		vectorsByName[vector.Name] = vector
	}

	if featureService != nil {
		for _, projection := range featureService.Projections {
			for _, f := range projection.Features {
				requestedFeatureRefs = append(requestedFeatureRefs,
					fmt.Sprintf("%s:%s", projection.NameToUse(), f.name))
			}
		}
	}

	for _, featureRef := range requestedFeatureRefs {
		viewName, featureName, err := ParseFeatureReference(featureRef)
		if err != nil {
			return nil, err
		}
		qualifiedName := getQualifiedFeatureName(viewName, featureName, fullFeatureNames)
		if _, ok := vectorsByName[qualifiedName]; !ok {
			return nil, fmt.Errorf("requested feature %s can't be retrieved", featureRef)
		}
		expectedVectors = append(expectedVectors, vectorsByName[qualifiedName])
	}

	return expectedVectors, nil
}

func entitiesToFeatureVectors(entityColumns map[string]*prototypes.RepeatedValue, arrowAllocator memory.Allocator, numRows int) ([]*FeatureVector, error) {
	vectors := make([]*FeatureVector, 0)
	presentVector := make([]serving.FieldStatus, numRows)
	timestampVector := make([]*timestamppb.Timestamp, numRows)
	for idx := 0; idx < numRows; idx++ {
		presentVector[idx] = serving.FieldStatus_PRESENT
		timestampVector[idx] = timestamppb.Now()
	}
	for entityName, values := range entityColumns {
		arrowColumn, err := types.ProtoValuesToArrowArray(values.Val, arrowAllocator, numRows)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, &FeatureVector{
			Name:       entityName,
			Values:     arrowColumn,
			Statuses:   presentVector,
			Timestamps: timestampVector,
		})
	}
	return vectors, nil
}

func (fs *FeatureStore) ListFeatureViews() ([]*FeatureView, error) {
	featureViews, err := fs.registry.listFeatureViews(fs.config.Project)
	if err != nil {
		return featureViews, err
	}
	return featureViews, nil
}

func (fs *FeatureStore) ListEntities(hideDummyEntity bool) ([]*Entity, error) {

	allEntities, err := fs.registry.listEntities(fs.config.Project)
	if err != nil {
		return allEntities, err
	}
	entities := make([]*Entity, 0)
	for _, entity := range allEntities {
		if entity.Name != DUMMY_ENTITY_NAME || !hideDummyEntity {
			entities = append(entities, entity)
		}
	}
	return entities, nil
}

func entityKeysToProtos(joinKeyValues map[string]*prototypes.RepeatedValue) []*prototypes.EntityKey {
	keys := make([]string, len(joinKeyValues))
	index := 0
	var numRows int
	for k, v := range joinKeyValues {
		keys[index] = k
		index += 1
		numRows = len(v.Val)
	}
	sort.Strings(keys)
	entityKeys := make([]*prototypes.EntityKey, numRows)
	numJoinKeys := len(keys)
	// Construct each EntityKey object
	for index = 0; index < numRows; index++ {
		entityKeys[index] = &prototypes.EntityKey{JoinKeys: keys, EntityValues: make([]*prototypes.Value, numJoinKeys)}
	}

	for colIndex, key := range keys {
		for index, value := range joinKeyValues[key].GetVal() {
			entityKeys[index].EntityValues[colIndex] = value
		}
	}
	return entityKeys
}

/*
Group feature views that share the same set of join keys. For each group, we store only unique rows and save indices to retrieve those
rows for each requested feature
*/

func groupFeatureRefs(requestedFeatureViews []*featureViewAndRefs,
	joinKeyValues map[string]*prototypes.RepeatedValue,
	entityNameToJoinKeyMap map[string]string,
	fullFeatureNames bool,
) (map[string]*GroupedFeaturesPerEntitySet,
	error,
) {
	groups := make(map[string]*GroupedFeaturesPerEntitySet)

	for _, featuresAndView := range requestedFeatureViews {
		joinKeys := make([]string, 0)
		fv := featuresAndView.view
		featureNames := featuresAndView.featureRefs
		for entity := range fv.Entities {
			joinKeys = append(joinKeys, entityNameToJoinKeyMap[entity])
		}

		groupKeyBuilder := make([]string, 0)
		joinKeysValuesProjection := make(map[string]*prototypes.RepeatedValue)

		joinKeyToAliasMap := make(map[string]string)
		if fv.Base.Projection != nil && fv.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = fv.Base.Projection.JoinKeyMap
		}

		for _, joinKey := range joinKeys {
			var joinKeyOrAlias string

			if alias, ok := joinKeyToAliasMap[joinKey]; ok {
				groupKeyBuilder = append(groupKeyBuilder, fmt.Sprintf("%s[%s]", joinKey, alias))
				joinKeyOrAlias = alias
			} else {
				groupKeyBuilder = append(groupKeyBuilder, joinKey)
				joinKeyOrAlias = joinKey
			}

			if _, ok := joinKeyValues[joinKeyOrAlias]; !ok {
				return nil, fmt.Errorf("key %s is missing in provided entity rows", joinKey)
			}
			joinKeysValuesProjection[joinKey] = joinKeyValues[joinKeyOrAlias]
		}

		sort.Strings(groupKeyBuilder)
		groupKey := strings.Join(groupKeyBuilder, ",")

		aliasedFeatureNames := make([]string, 0)
		featureViewNames := make([]string, 0)
		var viewNameToUse string
		if fv.Base.Projection != nil {
			viewNameToUse = fv.Base.Projection.NameToUse()
		} else {
			viewNameToUse = fv.Base.Name
		}

		for _, featureName := range featureNames {
			aliasedFeatureNames = append(aliasedFeatureNames,
				getQualifiedFeatureName(viewNameToUse, featureName, fullFeatureNames))
			featureViewNames = append(featureViewNames, fv.Base.Name)
		}

		if _, ok := groups[groupKey]; !ok {
			joinKeysProto := entityKeysToProtos(joinKeysValuesProjection)
			uniqueEntityRows, mappingIndices, err := getUniqueEntityRows(joinKeysProto)
			if err != nil {
				return nil, err
			}

			groups[groupKey] = &GroupedFeaturesPerEntitySet{
				featureNames:        featureNames,
				featureViewNames:    featureViewNames,
				aliasedFeatureNames: aliasedFeatureNames,
				indices:             mappingIndices,
				entityKeys:          uniqueEntityRows,
			}

		} else {
			groups[groupKey].featureNames = append(groups[groupKey].featureNames, featureNames...)
			groups[groupKey].aliasedFeatureNames = append(groups[groupKey].aliasedFeatureNames, aliasedFeatureNames...)
			groups[groupKey].featureViewNames = append(groups[groupKey].featureViewNames, featureViewNames...)
		}
	}
	return groups, nil
}

func getUniqueEntityRows(joinKeysProto []*prototypes.EntityKey) ([]*prototypes.EntityKey, [][]int, error) {
	uniqueValues := make(map[[sha256.Size]byte]*prototypes.EntityKey, 0)
	positions := make(map[[sha256.Size]byte][]int, 0)

	for index, entityKey := range joinKeysProto {
		serializedRow, err := proto.Marshal(entityKey)
		if err != nil {
			return nil, nil, err
		}

		rowHash := sha256.Sum256(serializedRow)
		if _, ok := uniqueValues[rowHash]; !ok {
			uniqueValues[rowHash] = entityKey
			positions[rowHash] = []int{index}
		} else {
			positions[rowHash] = append(positions[rowHash], index)
		}
	}

	mappingIndices := make([][]int, len(uniqueValues))
	uniqueEntityRows := make([]*prototypes.EntityKey, 0)
	for rowHash, row := range uniqueValues {
		nextIdx := len(uniqueEntityRows)

		mappingIndices[nextIdx] = positions[rowHash]
		uniqueEntityRows = append(uniqueEntityRows, row)
	}
	return uniqueEntityRows, mappingIndices, nil
}

func (fs *FeatureStore) GetFeatureView(featureViewName string, hideDummyEntity bool) (*FeatureView, error) {
	fv, err := fs.registry.getFeatureView(fs.config.Project, featureViewName)
	if err != nil {
		return nil, err
	}
	if _, ok := fv.Entities[DUMMY_ENTITY_NAME]; ok && hideDummyEntity {
		fv.Entities = make(map[string]struct{})
	}
	return fv, nil
}

func ParseFeatureReference(featureRef string) (featureViewName, featureName string, e error) {
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

func getQualifiedFeatureName(viewName string, featureName string, fullFeatureNames bool) string {
	if fullFeatureNames {
		return fmt.Sprintf("%s__%s", viewName, featureName)
	} else {
		return featureName
	}
}

type featureNameCollisionError struct {
	featureRefCollisions []string
	fullFeatureNames     bool
}

func (e featureNameCollisionError) Error() string {
	return fmt.Sprintf("featureNameCollisionError: %s; %t", strings.Join(e.featureRefCollisions, ", "), e.fullFeatureNames)
}
