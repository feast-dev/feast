package registry

import (
	"github.com/feast-dev/feast/go/internal/feast/errors"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/rs/zerolog/log"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

var REGISTRY_SCHEMA_VERSION string = "1"
var REGISTRY_STORE_CLASS_FOR_SCHEME map[string]string = map[string]string{
	"gs":    "GCSRegistryStore",
	"s3":    "S3RegistryStore",
	"file":  "FileRegistryStore",
	"http":  "HttpRegistryStore",
	"https": "HttpRegistryStore",
	"":      "FileRegistryStore",
}

/*
	Store protos of FeatureView, FeatureService, Entity, OnDemandFeatureView
	but return to user copies of non-proto versions of these objects
*/

type cacheMap[T any] struct {
	cache map[string]map[string]*model.ModelTTL[T]
	ttl   time.Duration
	mu    sync.RWMutex
}

func newCacheMap[T any](ttl time.Duration) *cacheMap[T] {
	return &cacheMap[T]{cache: make(map[string]map[string]*model.ModelTTL[T]), ttl: ttl}
}

func (m *cacheMap[T]) get(project string, key string) (T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if cache, ok := m.cache[project]; ok {
		if item, ok := cache[key]; ok {
			return item.Model, true
		}
	}
	var null T
	return null, false
}

func (m *cacheMap[T]) getOrLoad(project string, key string, load func(string, string) (T, error)) (T, error) {
	m.mu.RLock()
	if cache, ok := m.cache[project]; ok {
		if item, ok := cache[key]; ok {
			m.mu.RUnlock()
			return item.Model, nil
		}
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	// Double check if the item is already in the cache after acquiring the exclusive lock
	if cache, ok := m.cache[project]; ok {
		if item, ok := cache[key]; ok {
			return item.Model, nil
		}
	} else {
		m.cache[project] = make(map[string]*model.ModelTTL[T])
	}
	item, err := load(key, project)
	if err != nil {
		var null T
		return null, err
	}
	m.cache[project][key] = model.NewModelTTLWithExpiration(item, m.ttl)
	return item, nil
}

func (m *cacheMap[T]) set(project string, key string, item T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.cache[project]; !ok {
		m.cache[project] = make(map[string]*model.ModelTTL[T])
	}
	m.cache[project][key] = model.NewModelTTLWithExpiration(item, m.ttl)
}

func (m *cacheMap[T]) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache = make(map[string]map[string]*model.ModelTTL[T])
}

func deepCopy[T any](src map[string]map[string]*model.ModelTTL[T]) map[string]map[string]*model.ModelTTL[T] {
	dest := make(map[string]map[string]*model.ModelTTL[T])
	for k, vmap := range src {
		dest[k] = make(map[string]*model.ModelTTL[T])
		for l, m := range vmap {
			dest[k][l] = m.Copy()
		}
	}
	return dest
}

func (m *cacheMap[T]) expireCachedModels(getModel func(string, string) (T, error)) {
	m.mu.Lock()
	tempCacheMap := deepCopy(m.cache)
	m.mu.Unlock()

	for project, cache := range tempCacheMap {
		for modelName, cacheItem := range cache {
			if cacheItem.IsExpired() {
				newModel, err := getModel(modelName, project)
				if err != nil {
					// If we can't find the model in the registry, remove it from the cache
					if errors.IsGrpcNotFoundError(err) {
						delete(cache, modelName)
					} else {
						log.Error().Err(err).Msgf("error refreshing model %s in project %s", modelName, project)
					}
				} else {
					cache[modelName] = model.NewModelTTLWithExpiration(newModel, m.ttl)
				}
			}
		}
	}

	m.mu.Lock()
	m.cache = deepCopy(tempCacheMap)
	m.mu.Unlock()
}

type Registry struct {
	project                    string
	registryStore              RegistryStore
	cachedFeatureServices      *cacheMap[*model.FeatureService]
	cachedEntities             *cacheMap[*model.Entity]
	cachedFeatureViews         *cacheMap[*model.FeatureView]
	cachedSortedFeatureViews   *cacheMap[*model.SortedFeatureView]
	cachedOnDemandFeatureViews *cacheMap[*model.OnDemandFeatureView]
	cachedRegistry             *core.Registry
	cachedRegistryProtoTtl     time.Duration
}

func NewRegistry(registryConfig *RegistryConfig, repoPath string, project string) (*Registry, error) {
	registryStoreType := registryConfig.RegistryStoreType
	registryPath := registryConfig.Path
	ttl := time.Duration(registryConfig.CacheTtlSeconds) * time.Second

	r := &Registry{
		project:                    project,
		cachedFeatureServices:      newCacheMap[*model.FeatureService](ttl),
		cachedEntities:             newCacheMap[*model.Entity](ttl),
		cachedFeatureViews:         newCacheMap[*model.FeatureView](ttl),
		cachedSortedFeatureViews:   newCacheMap[*model.SortedFeatureView](ttl),
		cachedOnDemandFeatureViews: newCacheMap[*model.OnDemandFeatureView](ttl),
		cachedRegistryProtoTtl:     ttl,
	}

	if len(registryStoreType) == 0 {
		registryStore, err := getRegistryStoreFromScheme(registryPath, registryConfig, repoPath, project)
		if err != nil {
			return nil, err
		}
		r.registryStore = registryStore
	} else {
		registryStore, err := getRegistryStoreFromType(registryStoreType, registryConfig, repoPath, project)
		if err != nil {
			return nil, err
		}
		r.registryStore = registryStore
	}

	return r, nil
}

func (r *Registry) InitializeRegistry() error {
	registryProto, err := r.registryStore.GetRegistryProto()
	if err != nil {
		switch r.registryStore.(type) {
		case *FileRegistryStore, *HttpRegistryStore:
			log.Error().Err(err).Msg("Registry Initialization Failed")
			return err
		default:
			registryProto = &core.Registry{RegistrySchemaVersion: REGISTRY_SCHEMA_VERSION}
			r.registryStore.UpdateRegistryProto(registryProto)
		}
	}
	r.cachedRegistry = registryProto
	if !r.registryStore.HasFallback() {
		r.load(registryProto)
	}
	go r.RefreshRegistryOnInterval()
	return nil
}

func (r *Registry) RefreshRegistryOnInterval() {
	ticker := time.NewTicker(r.cachedRegistryProtoTtl)
	for ; true; <-ticker.C {
		err := r.refresh()
		if err != nil {
			log.Error().Stack().Err(err).Msg("Registry refresh Failed")
		}
	}
}

func (r *Registry) refresh() error {
	if r.registryStore.HasFallback() {
		r.cachedEntities.expireCachedModels(r.GetEntityFromRegistry)
		r.cachedFeatureServices.expireCachedModels(r.GetFeatureServiceFromRegistry)
		r.cachedFeatureViews.expireCachedModels(r.GetFeatureViewFromRegistry)
		r.cachedSortedFeatureViews.expireCachedModels(r.GetSortedFeatureViewFromRegistry)
		r.cachedOnDemandFeatureViews.expireCachedModels(r.GetOnDemandFeatureViewFromRegistry)
	} else {
		registryProto, err := r.registryStore.GetRegistryProto()
		if err != nil {
			return err
		}
		r.load(registryProto)
	}
	return nil
}

func (r *Registry) clearCache() {
	r.cachedFeatureServices.clear()
	r.cachedEntities.clear()
	r.cachedFeatureViews.clear()
	r.cachedSortedFeatureViews.clear()
	r.cachedOnDemandFeatureViews.clear()
}

func (r *Registry) SetModels(
	featureServices []*core.FeatureService,
	entities []*core.Entity,
	fvs []*core.FeatureView,
	sfvs []*core.SortedFeatureView,
	odfvs []*core.OnDemandFeatureView) {
	r.clearCache()
	loadModels(featureServices, r.cachedFeatureServices, model.NewFeatureServiceFromProto, r.project)
	loadModels(entities, r.cachedEntities, model.NewEntityFromProto, r.project)
	loadModels(fvs, r.cachedFeatureViews, model.NewFeatureViewFromProto, r.project)
	loadModels(sfvs, r.cachedSortedFeatureViews, model.NewSortedFeatureViewFromProto, r.project)
	loadModels(odfvs, r.cachedOnDemandFeatureViews, model.NewOnDemandFeatureViewFromProto, r.project)
}

func (r *Registry) load(registry *core.Registry) {
	r.cachedRegistry = registry
	r.SetModels(registry.FeatureServices, registry.Entities, registry.FeatureViews, registry.SortedFeatureViews, registry.OnDemandFeatureViews)
}

func loadModels[U any, T any](protoList []U, cachedModels *cacheMap[T], modelFactory func(proto U) T, project string) {
	for _, proto := range protoList {
		obj := modelFactory(proto)
		nameField := reflect.ValueOf(obj).Elem().FieldByName("Name")
		if !nameField.IsValid() {
			nameField = reflect.ValueOf(obj).Elem().FieldByName("Base").Elem().FieldByName("Name")
		}
		objName := nameField.Interface().(string)
		cachedModels.set(project, objName, obj)
	}
}

func (r *Registry) GetEntity(project string, entityName string) (*model.Entity, error) {
	if r.registryStore.HasFallback() {
		return r.cachedEntities.getOrLoad(project, entityName, r.GetEntityFromRegistry)
	}

	if entity, ok := r.cachedEntities.get(project, entityName); ok {
		return entity, nil
	}

	return nil, errors.GrpcNotFoundErrorf("no cached entity %s found for project %s", entityName, project)
}

func (r *Registry) GetEntityFromRegistry(entityName string, project string) (*model.Entity, error) {
	entityProto, err := r.registryStore.(*HttpRegistryStore).getEntity(entityName, true)
	if err != nil {
		if errors.IsHTTPNotFoundError(err) {
			log.Error().Err(err).Msgf("no entity %s found in project %s", entityName, project)
			return nil, errors.GrpcNotFoundErrorf("no entity %s found in project %s", entityName, project)
		}

		log.Error().Err(err).Msgf("no entity %s found in project %s", entityName, project)
		return nil, errors.GrpcInternalErrorf("error retrieving entity %s in project %s: %v", entityName, project, err)
	}

	return model.NewEntityFromProto(entityProto), nil
}

func (r *Registry) GetFeatureView(project string, featureViewName string) (*model.FeatureView, error) {
	if r.registryStore.HasFallback() {
		return r.cachedFeatureViews.getOrLoad(project, featureViewName, r.GetFeatureViewFromRegistry)
	}

	if cachedFeatureView, ok := r.cachedFeatureViews.get(project, featureViewName); ok {
		return cachedFeatureView, nil
	}

	return nil, errors.GrpcNotFoundErrorf("no cached feature view %s found for project %s", featureViewName, project)
}

func (r *Registry) GetFeatureViewFromRegistry(featureViewName string, project string) (*model.FeatureView, error) {
	featureViewProto, err := r.registryStore.(*HttpRegistryStore).getFeatureView(featureViewName, true)
	if err != nil {
		if errors.IsHTTPNotFoundError(err) {
			log.Error().Err(err).Msgf("no feature view %s found in project %s", featureViewName, project)
			return nil, errors.GrpcNotFoundErrorf("no feature view %s found in project %s", featureViewName, project)
		}

		log.Error().Err(err).Msgf("error retrieving feature view %s in project %s", featureViewName, project)
		return nil, errors.GrpcInternalErrorf("error retrieving feature view %s in project %s: %v", featureViewName, project, err)
	}

	return model.NewFeatureViewFromProto(featureViewProto), nil
}

func (r *Registry) GetSortedFeatureView(project string, sortedFeatureViewName string) (*model.SortedFeatureView, error) {
	if r.registryStore.HasFallback() {
		return r.cachedSortedFeatureViews.getOrLoad(project, sortedFeatureViewName, r.GetSortedFeatureViewFromRegistry)
	}

	if cachedSortedFeatureView, ok := r.cachedSortedFeatureViews.get(project, sortedFeatureViewName); ok {
		return cachedSortedFeatureView, nil
	}

	return nil, errors.GrpcNotFoundErrorf("no cached sorted feature view %s found for project %s", sortedFeatureViewName, project)
}

func (r *Registry) GetSortedFeatureViewFromRegistry(sortedFeatureViewName string, project string) (*model.SortedFeatureView, error) {
	sortedFeatureViewProto, err := r.registryStore.(*HttpRegistryStore).getSortedFeatureView(sortedFeatureViewName, true)
	if err != nil {
		if errors.IsHTTPNotFoundError(err) {
			log.Error().Err(err).Msgf("no sorted feature view %s found in project %s", sortedFeatureViewName, project)
			return nil, errors.GrpcNotFoundErrorf("no sorted feature view %s found in project %s", sortedFeatureViewName, project)
		}

		log.Error().Err(err).Msgf("error retrieving sorted feature view %s in project %s", sortedFeatureViewName, project)
		return nil, errors.GrpcInternalErrorf("error retrieving sorted feature view %s in project %s: %v", sortedFeatureViewName, project, err)
	}

	return model.NewSortedFeatureViewFromProto(sortedFeatureViewProto), nil
}

func (r *Registry) GetFeatureService(project string, featureServiceName string) (*model.FeatureService, error) {
	if r.registryStore.HasFallback() {
		return r.cachedFeatureServices.getOrLoad(project, featureServiceName, r.GetFeatureServiceFromRegistry)
	}

	if cachedFeatureService, ok := r.cachedFeatureServices.get(project, featureServiceName); ok {
		return cachedFeatureService, nil
	}

	return nil, errors.GrpcNotFoundErrorf("no cached feature service %s found for project %s", featureServiceName, project)
}

func (r *Registry) GetFeatureServiceFromRegistry(featureServiceName string, project string) (*model.FeatureService, error) {
	featureServiceProto, err := r.registryStore.(*HttpRegistryStore).getFeatureService(featureServiceName, true)
	if err != nil {
		if errors.IsHTTPNotFoundError(err) {
			log.Error().Err(err).Msgf("no feature service %s found in project %s", featureServiceName, project)
			return nil, errors.GrpcNotFoundErrorf("no feature service %s found in project %s", featureServiceName, project)
		}

		log.Error().Err(err).Msgf("error retrieving feature service %s in project %s", featureServiceName, project)
		return nil, errors.GrpcInternalErrorf("error retrieving feature service %s in project %s: %v", featureServiceName, project, err)
	}

	return model.NewFeatureServiceFromProto(featureServiceProto), nil
}

func (r *Registry) GetOnDemandFeatureView(project string, onDemandFeatureViewName string) (*model.OnDemandFeatureView, error) {
	if r.registryStore.HasFallback() {
		return r.cachedOnDemandFeatureViews.getOrLoad(project, onDemandFeatureViewName, r.GetOnDemandFeatureViewFromRegistry)
	}

	if cachedOnDemandFeatureView, ok := r.cachedOnDemandFeatureViews.get(project, onDemandFeatureViewName); ok {
		return cachedOnDemandFeatureView, nil
	}

	return nil, errors.GrpcNotFoundErrorf("no cached on demand feature view %s found for project %s", onDemandFeatureViewName, project)
}

func (r *Registry) GetOnDemandFeatureViewFromRegistry(onDemandFeatureViewName string, project string) (*model.OnDemandFeatureView, error) {
	onDemandFeatureViewProto, err := r.registryStore.(*HttpRegistryStore).getOnDemandFeatureView(onDemandFeatureViewName, true)
	if err != nil {
		if errors.IsHTTPNotFoundError(err) {
			log.Error().Err(err).Msgf("no on demand feature view %s found in project %s", onDemandFeatureViewName, project)
			return nil, errors.GrpcNotFoundErrorf("no on demand feature view %s found in project %s", onDemandFeatureViewName, project)
		}

		log.Error().Err(err).Msgf("error retrieving on demand feature view %s in project %s", onDemandFeatureViewName, project)
		return nil, errors.GrpcInternalErrorf("error retrieving on demand feature view %s in project %s: %v", onDemandFeatureViewName, project, err)
	}

	return model.NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto), nil
}

func getRegistryStoreFromScheme(registryPath string, registryConfig *RegistryConfig, repoPath string, project string) (RegistryStore, error) {
	uri, err := url.Parse(registryPath)
	if err != nil {
		return nil, err
	}
	if registryStoreType, ok := REGISTRY_STORE_CLASS_FOR_SCHEME[uri.Scheme]; ok {
		return getRegistryStoreFromType(registryStoreType, registryConfig, repoPath, project)
	}
	return nil, errors.GrpcNotFoundErrorf("registry path %s has unsupported scheme %s. Supported schemes are file, s3 and gs", registryPath, uri.Scheme)
}

func getRegistryStoreFromType(registryStoreType string, registryConfig *RegistryConfig, repoPath string, project string) (RegistryStore, error) {
	switch registryStoreType {
	case "FileRegistryStore":
		return NewFileRegistryStore(registryConfig, repoPath), nil
	case "HttpRegistryStore":
		return NewHttpRegistryStore(registryConfig, project)
	case "S3RegistryStore":
		return NewS3RegistryStore(registryConfig, repoPath), nil
	}
	return nil, errors.GrpcInternalErrorf("only FileRegistryStore or HttpRegistryStore as a RegistryStore is supported at this moment")
}
