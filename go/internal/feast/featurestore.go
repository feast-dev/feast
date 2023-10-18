package feast

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/v8/arrow/memory"

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

	registry, err := registry.NewRegistry(config.GetRegistryConfig(), config.RepoPath)
	if err != nil {
		return nil, err
	}
	registry.InitializeRegistry()

	return &FeatureStore{
		config:                 config,
		registry:               registry,
		onlineStore:            onlineStore,
		transformationCallback: callback,
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
	fvs, odFvs, err := fs.listAllViews()
	if err != nil {
		return nil, err
	}

	entities, err := fs.ListEntities(false)
	if err != nil {
		return nil, err
	}

	var requestedFeatureViews []*onlineserving.FeatureViewAndRefs
	var requestedOnDemandFeatureViews []*model.OnDemandFeatureView
	if featureService != nil {
		requestedFeatureViews, requestedOnDemandFeatureViews, err =
			onlineserving.GetFeatureViewsToUseByService(featureService, fvs, odFvs)
	} else {
		requestedFeatureViews, requestedOnDemandFeatureViews, err =
			onlineserving.GetFeatureViewsToUseByFeatureRefs(featureRefs, fvs, odFvs)
	}
	if err != nil {
		return nil, err
	}

	entityNameToJoinKeyMap, expectedJoinKeysSet, err := onlineserving.GetEntityMaps(requestedFeatureViews, entities)
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
	arrowMemory := memory.NewCgoArrowAllocator()
	featureViews := make([]*model.FeatureView, len(requestedFeatureViews))
	index := 0
	for _, featuresAndView := range requestedFeatureViews {
		featureViews[index] = featuresAndView.View
		index += 1
	}

	entitylessCase := false
	for _, featureView := range featureViews {
		if featureView.HasEntity(model.DUMMY_ENTITY_NAME) {
			entitylessCase = true
			break
		}
	}

	if entitylessCase {
		dummyEntityColumn := &prototypes.RepeatedValue{Val: make([]*prototypes.Value, numRows)}
		for index := 0; index < numRows; index++ {
			dummyEntityColumn.Val[index] = &model.DUMMY_ENTITY_VALUE
		}
		joinKeyToEntityValues[model.DUMMY_ENTITY_ID] = dummyEntityColumn
	}

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

	if fs.transformationCallback != nil {
		onDemandFeatures, err := transformation.AugmentResponseWithOnDemandTransforms(
			requestedOnDemandFeatureViews,
			requestData,
			joinKeyToEntityValues,
			result,
			fs.transformationCallback,
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
		featureService, err := fs.registry.GetFeatureService(fs.config.Project, featureServiceRequest.FeatureService)
		if err != nil {
			return nil, err
		}
		return &Features{FeaturesRefs: nil, FeatureService: featureService}, nil
	}
	return nil, errors.New("cannot parse kind from GetOnlineFeaturesRequest")
}

func (fs *FeatureStore) GetFeatureService(name string) (*model.FeatureService, error) {
	return fs.registry.GetFeatureService(fs.config.Project, name)
}

func (fs *FeatureStore) listAllViews() (map[string]*model.FeatureView, map[string]*model.OnDemandFeatureView, error) {
	fvs := make(map[string]*model.FeatureView)
	odFvs := make(map[string]*model.OnDemandFeatureView)

	featureViews, err := fs.ListFeatureViews()
	if err != nil {
		return nil, nil, err
	}
	for _, featureView := range featureViews {
		fvs[featureView.Base.Name] = featureView
	}

	streamFeatureViews, err := fs.ListStreamFeatureViews()
	if err != nil {
		return nil, nil, err
	}
	for _, streamFeatureView := range streamFeatureViews {
		fvs[streamFeatureView.Base.Name] = streamFeatureView
	}

	onDemandFeatureViews, err := fs.registry.ListOnDemandFeatureViews(fs.config.Project)
	if err != nil {
		return nil, nil, err
	}
	for _, onDemandFeatureView := range onDemandFeatureViews {
		odFvs[onDemandFeatureView.Base.Name] = onDemandFeatureView
	}
	return fvs, odFvs, nil
}

func (fs *FeatureStore) ListFeatureViews() ([]*model.FeatureView, error) {
	featureViews, err := fs.registry.ListFeatureViews(fs.config.Project)
	if err != nil {
		return featureViews, err
	}
	return featureViews, nil
}

func (fs *FeatureStore) ListStreamFeatureViews() ([]*model.FeatureView, error) {
	streamFeatureViews, err := fs.registry.ListStreamFeatureViews(fs.config.Project)
	if err != nil {
		return streamFeatureViews, err
	}
	return streamFeatureViews, nil
}

func (fs *FeatureStore) ListEntities(hideDummyEntity bool) ([]*model.Entity, error) {

	allEntities, err := fs.registry.ListEntities(fs.config.Project)
	if err != nil {
		return allEntities, err
	}
	entities := make([]*model.Entity, 0)
	for _, entity := range allEntities {
		if entity.Name != model.DUMMY_ENTITY_NAME || !hideDummyEntity {
			entities = append(entities, entity)
		}
	}
	return entities, nil
}

func (fs *FeatureStore) ListOnDemandFeatureViews() ([]*model.OnDemandFeatureView, error) {
	return fs.registry.ListOnDemandFeatureViews(fs.config.Project)
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
	numRows := len(entityRows)
	entityRowsValue := make([]*prototypes.EntityKey, numRows)
	for index, entityKey := range entityRows {
		entityRowsValue[index] = &prototypes.EntityKey{JoinKeys: entityKey.JoinKeys, EntityValues: entityKey.EntityValues}
	}
	return fs.onlineStore.OnlineRead(ctx, entityRowsValue, requestedFeatureViewNames, requestedFeatureNames)
}

func (fs *FeatureStore) GetFcosMap() (map[string]*model.Entity, map[string]*model.FeatureView, map[string]*model.OnDemandFeatureView, error) {
	odfvs, err := fs.ListOnDemandFeatureViews()
	if err != nil {
		return nil, nil, nil, err
	}
	fvs, err := fs.ListFeatureViews()
	if err != nil {
		return nil, nil, nil, err
	}
	entities, err := fs.ListEntities(true)
	if err != nil {
		return nil, nil, nil, err
	}

	entityMap := make(map[string]*model.Entity)
	for _, entity := range entities {
		entityMap[entity.Name] = entity
	}
	fvMap := make(map[string]*model.FeatureView)
	for _, fv := range fvs {
		fvMap[fv.Base.Name] = fv
	}
	odfvMap := make(map[string]*model.OnDemandFeatureView)
	for _, odfv := range odfvs {
		odfvMap[odfv.Base.Name] = odfv
	}
	return entityMap, fvMap, odfvMap, nil
}
