package feast

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/transformation"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
)

type FeatureStore struct {
	config                 *registry.RepoConfig
	registry               *registry.Registry
	onlineStore            onlinestore.OnlineStore
	transformationCallback transformation.TransformationCallback
	transformationService  *transformation.GrpcTransformationService
}

// A Features struct specifies a list of features to be retrieved from the online store. These features
// can be specified either as a list of string feature references or as a feature service. String
// feature references must have format "feature_view:feature", e.g. "customer_fv:daily_transactions".
type Features struct {
	FeaturesRefs   []string
	FeatureService *model.FeatureService
}

func (fs *FeatureStore) Registry() *registry.Registry {
	return fs.registry
}

func (fs *FeatureStore) GetRepoConfig() *registry.RepoConfig {
	return fs.config
}

// NewFeatureStore constructs a feature store fat client using the
// repo config (contents of feature_store.yaml converted to JSON map).
func NewFeatureStore(config *registry.RepoConfig, callback transformation.TransformationCallback) (*FeatureStore, error) {
	onlineStore, err := onlinestore.NewOnlineStore(config)
	if err != nil {
		return nil, err
	}
	registryConfig, err := config.GetRegistryConfig()
	if err != nil {
		return nil, err
	}
	registry, err := registry.NewRegistry(registryConfig, config.RepoPath, config.Project)
	if err != nil {
		return nil, err
	}
	err = registry.InitializeRegistry()
	if err != nil {
		return nil, err
	}
	sanitizedProjectName := strings.Replace(config.Project, "_", "-", -1)
	productName := os.Getenv("PRODUCT")
	endpoint := fmt.Sprintf("%s-transformations.%s.svc.cluster.local:80", sanitizedProjectName, productName)
	transformationService, _ := transformation.NewGrpcTransformationService(config, endpoint)

	return &FeatureStore{
		config:                 config,
		registry:               registry,
		onlineStore:            onlineStore,
		transformationCallback: callback,
		transformationService:  transformationService,
	}, nil
}

// TODO: Review all functions that use ODFV and Request FV since these have not been tested
// ToDo: Split GetOnlineFeatures interface into two: GetOnlinFeaturesByFeatureService and GetOnlineFeaturesByFeatureRefs
func (fs *FeatureStore) GetOnlineFeatures(
	ctx context.Context,
	featureRefs []string,
	featureService *model.FeatureService,
	joinKeyToEntityValues map[string]*prototypes.RepeatedValue,
	requestData map[string]*prototypes.RepeatedValue,
	fullFeatureNames bool) ([]*onlineserving.FeatureVector, error) {
	var err error
	var requestedFeatureViews []*onlineserving.FeatureViewAndRefs
	var requestedOnDemandFeatureViews []*model.OnDemandFeatureView

	// TODO: currently ignores SortedFeatureViews, need to either implement get for them or throw some kind of error/warning
	if featureService != nil {
		requestedFeatureViews, _, requestedOnDemandFeatureViews, err =
			onlineserving.GetFeatureViewsToUseByService(featureService, fs.registry, fs.config.Project)
	} else {
		requestedFeatureViews, _, requestedOnDemandFeatureViews, err =
			onlineserving.GetFeatureViewsToUseByFeatureRefs(featureRefs, fs.registry, fs.config.Project)
	}
	if err != nil {
		return nil, err
	}

	entityNameToJoinKeyMap, expectedJoinKeysSet, err := onlineserving.GetEntityMaps(requestedFeatureViews, fs.registry, fs.config.Project)
	if err != nil {
		return nil, err
	}

	err = onlineserving.ValidateFeatureRefs(requestedFeatureViews, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	numRows, err := onlineserving.ValidateEntityValues(joinKeyToEntityValues, requestData, expectedJoinKeysSet)
	if err != nil {
		return nil, err
	}

	err = transformation.EnsureRequestedDataExist(requestedOnDemandFeatureViews, requestData)
	if err != nil {
		return nil, err
	}

	result := make([]*onlineserving.FeatureVector, 0)
	arrowMemory := memory.NewGoAllocator()
	featureViews := make([]*model.FeatureView, len(requestedFeatureViews))
	index := 0
	for _, featuresAndView := range requestedFeatureViews {
		featureViews[index] = featuresAndView.View
		index += 1
	}

	entitylessCase := checkEntitylessCase(featureViews)
	addDummyEntityIfNeeded(entitylessCase, joinKeyToEntityValues, numRows)

	groupedRefs, err := onlineserving.GroupFeatureRefs(requestedFeatureViews, joinKeyToEntityValues, entityNameToJoinKeyMap, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	for _, groupRef := range groupedRefs {
		featureData, err := fs.readFromOnlineStore(ctx, groupRef.EntityKeys, groupRef.FeatureViewNames, groupRef.FeatureNames)
		if err != nil {
			return nil, err
		}

		vectors, err := onlineserving.TransposeFeatureRowsIntoColumns(
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

	if fs.transformationCallback != nil || fs.transformationService != nil {
		onDemandFeatures, err := transformation.AugmentResponseWithOnDemandTransforms(
			ctx,
			requestedOnDemandFeatureViews,
			requestData,
			joinKeyToEntityValues,
			result,
			fs.transformationCallback,
			fs.transformationService,
			arrowMemory,
			numRows,
			fullFeatureNames,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, onDemandFeatures...)
	}

	result, err = onlineserving.KeepOnlyRequestedFeatures(result, featureRefs, featureService, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	entityColumns, err := onlineserving.EntitiesToFeatureVectors(joinKeyToEntityValues, arrowMemory, numRows)
	result = append(entityColumns, result...)
	return result, nil
}

func (fs *FeatureStore) GetOnlineFeaturesRange(
	ctx context.Context,
	featureRefs []string,
	featureService *model.FeatureService,
	joinKeyToEntityValues map[string]*prototypes.RepeatedValue,
	sortKeyFilters []*serving.SortKeyFilter,
	reverseSortOrder bool,
	limit int32,
	requestData map[string]*prototypes.RepeatedValue,
	fullFeatureNames bool) ([]*onlineserving.RangeFeatureVector, error) {

	if requestData == nil {
		requestData = make(map[string]*prototypes.RepeatedValue)
	}

	var err error
	var requestedSortedFeatureViews []*onlineserving.SortedFeatureViewAndRefs

	if featureService != nil {
		_, requestedSortedFeatureViews, _, err =
			onlineserving.GetFeatureViewsToUseByService(featureService, fs.registry, fs.config.Project)
	} else {
		_, requestedSortedFeatureViews, _, err =
			onlineserving.GetFeatureViewsToUseByFeatureRefs(featureRefs, fs.registry, fs.config.Project)
	}
	if err != nil {
		return nil, err
	}

	if len(requestedSortedFeatureViews) == 0 {
		return nil, fmt.Errorf("no sorted feature views found for the requested features")
	}

	// Note: We're ignoring on-demand feature views for now.

	entityNameToJoinKeyMap, expectedJoinKeysSet, err := onlineserving.GetEntityMapsForSortedViews(
		requestedSortedFeatureViews, fs.registry, fs.config.Project)
	if err != nil {
		return nil, err
	}

	if len(expectedJoinKeysSet) == 0 {
		return nil, fmt.Errorf("no entity join keys found, check feature view entity definition is well defined")
	}

	err = onlineserving.ValidateSortedFeatureRefs(requestedSortedFeatureViews, fullFeatureNames)
	if err != nil {
		return nil, err
	}

	numRows, err := onlineserving.ValidateEntityValues(joinKeyToEntityValues, requestData, expectedJoinKeysSet)
	if err != nil {
		return nil, fmt.Errorf("entity validation failed: %w", err)
	}

	if numRows <= 0 {
		return nil, fmt.Errorf("invalid number of entity rows: %d", numRows)
	}

	err = onlineserving.ValidateSortKeyFilters(sortKeyFilters, requestedSortedFeatureViews)
	if err != nil {
		return nil, err
	}

	if limit < 0 {
		return nil, fmt.Errorf("limit must be non-negative, got %d", limit)
	}

	entitylessCase := checkEntitylessCase(requestedSortedFeatureViews)
	addDummyEntityIfNeeded(entitylessCase, joinKeyToEntityValues, numRows)

	arrowMemory := memory.NewGoAllocator()
	entityColumns, err := onlineserving.EntitiesToRangeFeatureVectors(
		joinKeyToEntityValues, arrowMemory, numRows)
	if err != nil {
		return nil, err
	}

	result := make([]*onlineserving.RangeFeatureVector, 0, len(entityColumns))
	result = append(result, entityColumns...)

	groupedRangeRefs, err := onlineserving.GroupSortedFeatureRefs(
		requestedSortedFeatureViews,
		joinKeyToEntityValues,
		entityNameToJoinKeyMap,
		sortKeyFilters,
		reverseSortOrder,
		limit,
		fullFeatureNames)
	if err != nil {
		return nil, err
	}

	for _, groupRef := range groupedRangeRefs {
		featureData, err := fs.readRangeFromOnlineStore(
			ctx,
			groupRef.EntityKeys,
			groupRef.FeatureViewNames,
			groupRef.FeatureNames,
			groupRef.SortKeyFilters,
			groupRef.Limit)
		if err != nil {
			return nil, err
		}

		vectors, err := onlineserving.TransposeRangeFeatureRowsIntoColumns(
			featureData,
			groupRef,
			requestedSortedFeatureViews,
			arrowMemory,
			numRows,
		)
		if err != nil {
			return nil, err
		}

		result = append(result, vectors...)
	}

	return result, nil
}

func (fs *FeatureStore) DestructOnlineStore() {
	fs.onlineStore.Destruct()
}

// ParseFeatures parses the kind field of a GetOnlineFeaturesRequest protobuf message
// and populates a Features struct with the result.
// todo: enable support for feature service for GetOnlineFeaturesRange requests
func (fs *FeatureStore) ParseFeatures(kind interface{}) (*Features, error) {
	switch kind.(type) {
	case *serving.GetOnlineFeaturesRequest_Features:
		featureList := kind.(*serving.GetOnlineFeaturesRequest_Features)
		return &Features{FeaturesRefs: featureList.Features.GetVal(), FeatureService: nil}, nil
	case *serving.GetOnlineFeaturesRangeRequest_Features:
		featureList := kind.(*serving.GetOnlineFeaturesRangeRequest_Features)
		return &Features{FeaturesRefs: featureList.Features.GetVal(), FeatureService: nil}, nil
	case *serving.GetOnlineFeaturesRequest_FeatureService:
		featureServiceRequest := kind.(*serving.GetOnlineFeaturesRequest_FeatureService)
		featureService, err := fs.registry.GetFeatureService(fs.config.Project, featureServiceRequest.FeatureService)
		if err != nil {
			return nil, err
		}
		return &Features{FeaturesRefs: nil, FeatureService: featureService}, nil
	case *serving.GetOnlineFeaturesRangeRequest_FeatureService:
		return nil, errors.New("range requests only support 'kind' of a list of Features")
	default:
		return nil, errors.New("cannot parse 'kind' of either a Feature Service or list of Features from request")
	}
}

func (fs *FeatureStore) GetFeatureService(name string) (*model.FeatureService, error) {
	return fs.registry.GetFeatureService(fs.config.Project, name)
}

func addDummyEntityIfNeeded(
	entitylessCase bool,
	joinKeyToEntityValues map[string]*prototypes.RepeatedValue,
	numRows int) {

	if entitylessCase {
		dummyEntityColumn := &prototypes.RepeatedValue{Val: make([]*prototypes.Value, numRows)}
		for index := 0; index < numRows; index++ {
			dummyEntityColumn.Val[index] = &model.DUMMY_ENTITY_VALUE
		}
		joinKeyToEntityValues[model.DUMMY_ENTITY_ID] = dummyEntityColumn
	}
}

func checkEntitylessCase(views interface{}) bool {
	switch v := views.(type) {
	case []*model.FeatureView:
		for _, featureView := range v {
			if featureView.HasEntity(model.DUMMY_ENTITY_NAME) {
				return true
			}
		}
	case []*onlineserving.SortedFeatureViewAndRefs:
		for _, sfv := range v {
			if onlineserving.HasEntityInSortedFeatureView(sfv.View, model.DUMMY_ENTITY_NAME) {
				return true
			}
		}
	}
	return false
}

/*
Group feature views that share the same set of join keys. For each group, we store only unique rows and save indices to retrieve those
rows for each requested feature
*/

func (fs *FeatureStore) GetFeatureView(featureViewName string, hideDummyEntity bool) (*model.FeatureView, error) {
	fv, err := fs.registry.GetFeatureView(fs.config.Project, featureViewName)
	if err != nil {
		return nil, err
	}
	if fv.HasEntity(model.DUMMY_ENTITY_NAME) && hideDummyEntity {
		fv.EntityNames = []string{}
	}
	return fv, nil
}

func (fs *FeatureStore) readFromOnlineStore(ctx context.Context, entityRows []*prototypes.EntityKey,
	requestedFeatureViewNames []string,
	requestedFeatureNames []string,
) ([][]onlinestore.FeatureData, error) {
	// Create a Datadog span from context
	span, _ := tracer.StartSpanFromContext(ctx, "fs.readFromOnlineStore")
	defer span.Finish()

	numRows := len(entityRows)
	entityRowsValue := make([]*prototypes.EntityKey, numRows)
	for index, entityKey := range entityRows {
		entityRowsValue[index] = &prototypes.EntityKey{JoinKeys: entityKey.JoinKeys, EntityValues: entityKey.EntityValues}
	}
	return fs.onlineStore.OnlineRead(ctx, entityRowsValue, requestedFeatureViewNames, requestedFeatureNames)
}

func (fs *FeatureStore) readRangeFromOnlineStore(
	ctx context.Context,
	entityRows []*prototypes.EntityKey,
	requestedFeatureViewNames []string,
	requestedFeatureNames []string,
	sortKeyFilters []*model.SortKeyFilter,
	limit int32) ([][]onlinestore.RangeFeatureData, error) {

	span, _ := tracer.StartSpanFromContext(ctx, "fs.readRangeFromOnlineStore")
	defer span.Finish()

	numRows := len(entityRows)
	entityRowsValue := make([]*prototypes.EntityKey, numRows)
	for index, entityKey := range entityRows {
		entityRowsValue[index] = &prototypes.EntityKey{
			JoinKeys:     entityKey.JoinKeys,
			EntityValues: entityKey.EntityValues,
		}
	}

	return fs.onlineStore.OnlineReadRange(
		ctx,
		entityRowsValue,
		requestedFeatureViewNames,
		requestedFeatureNames,
		sortKeyFilters,
		limit)
}

func (fs *FeatureStore) GetFcosMap(featureServiceName string) (*model.FeatureService, map[string]*model.Entity, map[string]*model.FeatureView, map[string]*model.SortedFeatureView, map[string]*model.OnDemandFeatureView, error) {
	featureService, err := fs.GetFeatureService(featureServiceName)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	fvMap := make(map[string]*model.FeatureView)
	sortedFvMap := make(map[string]*model.SortedFeatureView)
	odfvMap := make(map[string]*model.OnDemandFeatureView)
	entityNames := make(map[string]bool)
	entityMap := make(map[string]*model.Entity)

	for _, featureProjection := range featureService.Projections {
		// Create copies of FeatureView that may contains the same *FeatureView but
		// each differentiated by a *FeatureViewProjection
		featureViewName := featureProjection.Name
		if fv, ok := fs.registry.GetFeatureView(fs.config.Project, featureViewName); ok == nil {
			fvMap[fv.Base.Name] = fv

			for _, e := range fv.EntityNames {
				entityMap[e] = nil
			}
		} else if sortedFv, ok := fs.registry.GetSortedFeatureView(fs.config.Project, featureViewName); ok == nil {
			sortedFvMap[sortedFv.Base.Name] = sortedFv

			for _, e := range fv.EntityNames {
				entityMap[e] = nil
			}
		} else if odFv, err := fs.registry.GetOnDemandFeatureView(fs.config.Project, featureViewName); err == nil {
			odfvMap[odFv.Base.Name] = odFv
		}
	}

	for entityName := range entityNames {
		if entity, err := fs.registry.GetEntity(fs.config.Project, entityName); err != nil {
			entityMap[entityName] = entity
		} else {
			return nil, nil, nil, nil, nil, fmt.Errorf("entity %s not found in registry", entityName)
		}
	}

	return featureService, entityMap, fvMap, sortedFvMap, odfvMap, nil
}
