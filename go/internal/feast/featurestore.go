package feast

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/utils"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FeatureStore struct {
	config      *RepoConfig
	registry    *Registry
	onlineStore OnlineStore
}

// A Features struct specifies a list of Features to be retrieved from the online store. These Features
// can be specified either as a list of string feature references or as a feature service. String
// feature references must have format "feature_view:feature", e.g. "customer_fv:daily_transactions".
type Features struct {
	Features       []string
	FeatureService *FeatureService
}

type FeatureVector struct {
	Name       string
	Values     array.Interface
	Statuses   []serving.FieldStatus
	Timestamps []*timestamppb.Timestamp
}

type featuresAndView struct {
	view     *FeatureView
	features []string
}

type GroupedFeaturesPerEntitySet struct {
	// A list of requested feature references of the form featureViewName:featureName that share this entity set
	featureNames     []string
	featureViewNames []string
	// A list of requested featureName if fullFeatureNames = False or a list of featureViewNameAlias__featureName that share this
	// entity set
	aliasedFeatureNames []string
	// Entity set as a list of EntityKeys to pass to OnlineRead
	entityKeys []*types.EntityKey
	// Reversed mapping to project result of retrieval from storage to response
	indices [][]int
}

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config *RepoConfig) (*FeatureStore, error) {
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
		config:      config,
		registry:    registry,
		onlineStore: onlineStore,
	}, nil
}

// TODO: Review all functions that use ODFV and Request FV since these have not been tested
func (fs *FeatureStore) GetOnlineFeatures(
	ctx context.Context,
	featureRefs []string,
	featureService *FeatureService,
	entityProtos map[string]*types.RepeatedValue,
	fullFeatureNames bool) ([]*FeatureVector, error) {

	numRows, err := fs.validateEntityValues(entityProtos)
	if err != nil {
		return nil, err
	}

	var fvs map[string]*FeatureView
	var requestedFeatureViews []*featuresAndView
	var requestedRequestFeatureViews []*RequestFeatureView
	var requestedOnDemandFeatureViews []*OnDemandFeatureView
	if featureService != nil {
		fvs, requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews, err =
			fs.getFeatureViewsToUseByService(featureService, false)
	} else {
		fvs, requestedFeatureViews, requestedRequestFeatureViews, requestedOnDemandFeatureViews, err =
			fs.getFeatureViewsToUseByFeatureRefs(featureRefs, false)
	}

	err = validateFeatureRefs(requestedFeatureViews, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	if len(requestedRequestFeatureViews)+len(requestedOnDemandFeatureViews) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "on demand feature views are currently not supported")
	}

	entityNameToJoinKeyMap, expectedJoinKeysSet, err := fs.getEntityMaps(requestedFeatureViews)
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
	mappedEntityProtos := make(map[string]*types.RepeatedValue)
	for joinKeyOrFeature, vals := range entityProtos {
		if _, ok := neededRequestODFVFeatures[joinKeyOrFeature]; ok {
			mappedEntityProtos[joinKeyOrFeature] = vals
			// requestDataFeatures[joinKeyOrFeature] = vals
		} else if _, ok = neededRequestData[joinKeyOrFeature]; ok {
			// requestDataFeatures[joinKeyOrFeature] = vals
		} else {
			if _, ok := expectedJoinKeysSet[joinKeyOrFeature]; !ok {
				return nil, fmt.Errorf("JoinKey is not expected in this request: %s\n%v", joinKeyOrFeature, expectedJoinKeysSet)
			} else {
				mappedEntityProtos[joinKeyOrFeature] = vals
			}
		}
	}

	// TODO (Ly): Skip this validation since we're not supporting ODFV yet

	// err = fs.ensureRequestedDataExist(neededRequestData, neededRequestODFVFeatures, requestDataFeatures)
	// if err != nil {
	// 	return nil, err
	// }

	// Add provided entities + ODFV schema entities to response

	featureViews := make([]*FeatureView, len(requestedFeatureViews))
	index := 0
	for _, featuresAndView := range requestedFeatureViews {
		featureViews[index] = featuresAndView.view
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
		mappedEntityProtos[DUMMY_ENTITY_ID] = dummyEntityColumn
	}

	groupedRefs, err := groupFeatureRefs(requestedFeatureViews, mappedEntityProtos, entityNameToJoinKeyMap, fullFeatureNames)
	if err != nil {
		return nil, err
	}
	result := make([]*FeatureVector, 0)
	arrowMemory := memory.NewGoAllocator()
	for _, groupRef := range groupedRefs {
		featureData, err := fs.readFromOnlineStore(ctx, groupRef.entityKeys, groupRef.featureViewNames, groupRef.featureNames)
		if err != nil {
			return nil, err
		}

		vectors, err := fs.transposeResponseIntoColumns(featureData,
			groupRef,
			fvs,
			arrowMemory,
			numRows,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, vectors...)
	}
	// TODO (Ly): ODFV, skip augmentResponseWithOnDemandTransforms
	return result, nil
}

func (fs *FeatureStore) DestructOnlineStore() {
	fs.onlineStore.Destruct()
}

// ParseFeatures parses the kind field of a GetOnlineFeaturesRequest protobuf message
// and populates a Features struct with the result.
func (fs *FeatureStore) ParseFeatures(kind interface{}) (*Features, error) {
	if featureList, ok := kind.(*serving.GetOnlineFeaturesRequest_Features); ok {
		return &Features{Features: featureList.Features.GetVal(), FeatureService: nil}, nil
	}
	if featureServiceRequest, ok := kind.(*serving.GetOnlineFeaturesRequest_FeatureService); ok {
		featureService, err := fs.registry.getFeatureService(fs.config.Project, featureServiceRequest.FeatureService)
		if err != nil {
			return nil, err
		}
		return &Features{Features: nil, FeatureService: featureService}, nil
	}
	return nil, errors.New("cannot parse kind from GetOnlineFeaturesRequest")
}

// getFeatureRefs extracts a list of feature references from a Features struct.
func (fs *FeatureStore) getFeatureRefs(features *Features) ([]string, error) {
	if features.FeatureService != nil {
		var featureViewName string
		featureRefs := make([]string, 0)
		for _, featureProjection := range features.FeatureService.projections {
			featureViewName = featureProjection.nameToUse()
			for _, feature := range featureProjection.features {
				featureRefs = append(featureRefs, fmt.Sprintf("%s:%s", featureViewName, feature.name))
			}
		}
		return featureRefs, nil
	} else {
		return features.Features, nil
	}
}

func (fs *FeatureStore) ExtractFeatureRefs(kind interface{}, fullFeatureNames bool) ([]string, error) {
	features, err := fs.ParseFeatures(kind)
	if err != nil {
		return nil, err
	}

	featureRefs, err := fs.getFeatureRefs(features)

	return featureRefs, nil
}

func (fs *FeatureStore) GetFeatureService(name string, project string) (*FeatureService, error) {
	return fs.registry.getFeatureService(project, name)
}

/*
	Return a list of copies of FeatureViewProjection
		copied from FeatureView, OnDemandFeatureView, RequestFeatureView existed in the registry

	TODO (Ly): Since the implementation of registry has changed, a better approach here is just
		retrieving featureViews asked in the passed in list of feature references instead of
		retrieving all feature views. Similar argument to FeatureService applies.

*/
func (fs *FeatureStore) getFeatureViewsToUseByService(featureService *FeatureService, hideDummyEntity bool) (map[string]*FeatureView, []*featuresAndView, []*RequestFeatureView, []*OnDemandFeatureView, error) {
	fvs := make(map[string]*FeatureView)
	requestFvs := make(map[string]*RequestFeatureView)
	odFvs := make(map[string]*OnDemandFeatureView)

	featureViews, err := fs.listFeatureViews(hideDummyEntity)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, featureView := range featureViews {
		fvs[featureView.base.name] = featureView
	}

	requestFeatureViews, err := fs.registry.listRequestFeatureViews(fs.config.Project)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, requestFeatureView := range requestFeatureViews {
		requestFvs[requestFeatureView.base.name] = requestFeatureView
	}

	onDemandFeatureViews, err := fs.registry.listOnDemandFeatureViews(fs.config.Project)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, onDemandFeatureView := range onDemandFeatureViews {
		odFvs[onDemandFeatureView.base.name] = onDemandFeatureView
	}

	fvsToUse := make([]*featuresAndView, 0)
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
			features := make([]string, len(newFv.base.features))
			for index, feature := range newFv.base.features {
				features[index] = feature.name
			}
			fvsToUse = append(fvsToUse, &featuresAndView{
				view:     newFv,
				features: features,
			})
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
			return nil, nil, nil, nil, fmt.Errorf("the provided feature service %s contains a reference to a feature view"+
				"%s which doesn't exist, please make sure that you have created the feature view"+
				"%s and that you have registered it by running \"apply\"", featureService.name, featureViewName, featureViewName)
		}
	}
	return fvs, fvsToUse, requestFvsToUse, odFvsToUse, nil
}

/*
	Return all FeatureView, OnDemandFeatureView, RequestFeatureView from the registry
*/
func (fs *FeatureStore) getFeatureViewsToUseByFeatureRefs(features []string, hideDummyEntity bool) (map[string]*FeatureView, []*featuresAndView, []*RequestFeatureView, []*OnDemandFeatureView, error) {
	fvs := make(map[string]*FeatureView)
	requestFvs := make(map[string]*RequestFeatureView)
	odFvs := make(map[string]*OnDemandFeatureView)

	featureViews, err := fs.listFeatureViews(hideDummyEntity)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, featureView := range featureViews {
		fvs[featureView.base.name] = featureView
	}

	requestFeatureViews, err := fs.registry.listRequestFeatureViews(fs.config.Project)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, requestFeatureView := range requestFeatureViews {
		requestFvs[requestFeatureView.base.name] = requestFeatureView
	}

	onDemandFeatureViews, err := fs.registry.listOnDemandFeatureViews(fs.config.Project)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, onDemandFeatureView := range onDemandFeatureViews {
		odFvs[onDemandFeatureView.base.name] = onDemandFeatureView
	}

	fvsToUse := make([]*featuresAndView, 0)
	requestFvsToUse := make([]*RequestFeatureView, 0)
	odFvsToUse := make([]*OnDemandFeatureView, 0)

	for _, featureRef := range features {
		featureViewName, featureName, err := parseFeatureReference(featureRef)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if fv, ok := fvs[featureViewName]; ok {
			found := false
			for _, group := range fvsToUse {
				if group.view == fv {
					group.features = append(group.features, featureName)
					found = true
				}
			}
			if !found {
				fvsToUse = append(fvsToUse, &featuresAndView{
					view:     fv,
					features: []string{featureName},
				})
			}
		} else if requestFv, ok := requestFvs[featureViewName]; ok {
			requestFvsToUse = append(requestFvsToUse, requestFv)
		} else if odFv, ok := odFvs[featureViewName]; ok {
			odFvsToUse = append(odFvsToUse, odFv)
		} else {
			return nil, nil, nil, nil, fmt.Errorf("feature view %s doesn't exist, please make sure that you have created the"+
				" feature view %s and that you have registered it by running \"apply\"", featureViewName, featureViewName)
		}
	}

	return fvs, fvsToUse, requestFvsToUse, odFvsToUse, nil
}

func (fs *FeatureStore) getEntityMaps(requestedFeatureViews []*featuresAndView) (map[string]string, map[string]interface{}, error) {
	entityNameToJoinKeyMap := make(map[string]string)
	expectedJoinKeysSet := make(map[string]interface{})

	entities, err := fs.listEntities(false)
	if err != nil {
		return nil, nil, err
	}
	entitiesByName := make(map[string]*Entity)

	for _, entity := range entities {
		entitiesByName[entity.name] = entity
	}

	for _, featuresAndView := range requestedFeatureViews {
		featureView := featuresAndView.view
		var joinKeyToAliasMap map[string]string
		if featureView.base.projection != nil && featureView.base.projection.joinKeyMap != nil {
			joinKeyToAliasMap = featureView.base.projection.joinKeyMap
		} else {
			joinKeyToAliasMap = map[string]string{}
		}

		for entityName := range featureView.entities {
			joinKey := entitiesByName[entityName].joinKey
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

func (fs *FeatureStore) validateEntityValues(joinKeyValues map[string]*types.RepeatedValue) (int, error) {
	setOfRowLengths := make(map[int]bool)
	var numRows int
	for _, col := range joinKeyValues {
		setOfRowLengths[len(col.Val)] = true
		numRows = len(col.Val)
	}
	if len(setOfRowLengths) > 1 {
		return 0, errors.New("valueError: All entity rows must have the same columns")
	}
	return numRows, nil
}

func validateFeatureRefs(requestedFeatures []*featuresAndView, fullFeatureNames bool) error {
	featureRefCounter := make(map[string]int)
	featureRefs := make([]string, 0)
	for _, viewAndFeatures := range requestedFeatures {
		for _, feature := range viewAndFeatures.features {
			projectedViewName := viewAndFeatures.view.base.name
			if viewAndFeatures.view.base.projection != nil {
				projectedViewName = viewAndFeatures.view.base.projection.nameToUse()
			}

			featureRefs = append(featureRefs,
				fmt.Sprintf("%s:%s", projectedViewName, feature))
		}
	}

	for _, featureRef := range featureRefs {
		if fullFeatureNames {
			featureRefCounter[featureRef]++
		} else {
			_, featureName, _ := parseFeatureReference(featureRef)
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
					_, featureName, _ := parseFeatureReference(featureRef)
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
		return fmt.Errorf("requestDataNotFoundInEntityRowsException: %s", strings.Join(missingFeatures, ", "))
	}
	return nil
}

func (fs *FeatureStore) checkOutsideTtl(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	return currentTimestamp.GetSeconds()-featureTimestamp.GetSeconds() > ttl.Seconds
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

func (fs *FeatureStore) transposeResponseIntoColumns(featureData2D [][]FeatureData,
	groupRef *GroupedFeaturesPerEntitySet,
	fvs map[string]*FeatureView,
	arrowAllocator memory.Allocator,
	numRows int) ([]*FeatureVector, error) {

	numFeatures := len(groupRef.aliasedFeatureNames)

	var value *types.Value
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
		protoValues := make([]*types.Value, numRows)

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
			for _, rowIndex := range outputIndexes {
				protoValues[rowIndex] = value
				currentVector.Statuses[rowIndex] = status
				currentVector.Timestamps[rowIndex] = eventTimeStamp
			}
		}
		var fieldType arrow.DataType
		var err error

		for _, val := range protoValues {
			if val != nil {
				fieldType, err = utils.ProtoTypeToArrowType(val)
				if err != nil {
					return nil, err
				}
				break
			}
		}

		if fieldType != nil {
			builder := array.NewBuilder(arrowAllocator, fieldType)
			err = utils.ProtoValuesToArrowArray(builder, protoValues)
			if err != nil {
				return nil, err
			}

			currentVector.Values = builder.NewArray()
		} else {
			currentVector.Values = array.NewNull(numRows)
		}
	}

	return vectors, nil

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

func (fs *FeatureStore) listFeatureViews(hideDummyEntity bool) ([]*FeatureView, error) {
	featureViews, err := fs.registry.listFeatureViews(fs.config.Project)
	if err != nil {
		return featureViews, err
	}
	return featureViews, nil
}

func (fs *FeatureStore) listRequestFeatureViews() ([]*RequestFeatureView, error) {
	return fs.registry.listRequestFeatureViews(fs.config.Project)
}

func (fs *FeatureStore) listEntities(hideDummyEntity bool) ([]*Entity, error) {

	allEntities, err := fs.registry.listEntities(fs.config.Project)
	if err != nil {
		return allEntities, err
	}
	entities := make([]*Entity, 0)
	for _, entity := range allEntities {
		if entity.name != DUMMY_ENTITY_NAME || !hideDummyEntity {
			entities = append(entities, entity)
		}
	}
	return entities, nil
}

func entityKeysToProtos(joinKeyValues map[string]*types.RepeatedValue) []*types.EntityKey {
	keys := make([]string, len(joinKeyValues))
	index := 0
	var numRows int
	for k, v := range joinKeyValues {
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

func groupFeatureRefs(requestedFeatureViews []*featuresAndView,
	joinKeyValues map[string]*types.RepeatedValue,
	entityNameToJoinKeyMap map[string]string,
	fullFeatureNames bool,
) (map[string]*GroupedFeaturesPerEntitySet,
	error,
) {
	groups := make(map[string]*GroupedFeaturesPerEntitySet)

	for _, featuresAndView := range requestedFeatureViews {
		joinKeys := make([]string, 0)
		fv := featuresAndView.view
		featureNames := featuresAndView.features
		for entity, _ := range fv.entities {
			joinKeys = append(joinKeys, entityNameToJoinKeyMap[entity])
		}

		groupKeyBuilder := make([]string, 0)
		joinKeysValuesProjection := make(map[string]*types.RepeatedValue)

		joinKeyToAliasMap := make(map[string]string)
		if fv.base.projection != nil && fv.base.projection.joinKeyMap != nil {
			joinKeyToAliasMap = fv.base.projection.joinKeyMap
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
		if fv.base.projection != nil {
			viewNameToUse = fv.base.projection.nameToUse()
		} else {
			viewNameToUse = fv.base.name
		}

		for _, featureName := range featureNames {
			aliasedFeatureNames = append(aliasedFeatureNames,
				getFeatureResponseMeta(viewNameToUse, featureName, fullFeatureNames))
			featureViewNames = append(featureViewNames, fv.base.name)
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

func getUniqueEntityRows(joinKeysProto []*types.EntityKey) ([]*types.EntityKey, [][]int, error) {
	uniqueValues := make(map[[sha256.Size]byte]*types.EntityKey, 0)
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
	uniqueEntityRows := make([]*types.EntityKey, 0)
	for rowHash, row := range uniqueValues {
		nextIdx := len(uniqueEntityRows)

		mappingIndices[nextIdx] = positions[rowHash]
		uniqueEntityRows = append(uniqueEntityRows, row)
	}
	return uniqueEntityRows, mappingIndices, nil
}

func (fs *FeatureStore) getFeatureView(project, featureViewName string, hideDummyEntity bool) (*FeatureView, error) {
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

type featureNameCollisionError struct {
	featureRefCollisions []string
	fullFeatureNames     bool
}

func (e featureNameCollisionError) Error() string {
	return fmt.Sprintf("featureNameCollisionError: %s; %t", strings.Join(e.featureRefCollisions, ", "), e.fullFeatureNames)
}
