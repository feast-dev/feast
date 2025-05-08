package registry

import (
	"errors"
	"fmt"
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

type Registry struct {
	project                    string
	registryStore              RegistryStore
	cachedFeatureServices      map[string]map[string]*model.ModelTTL[*model.FeatureService]
	cachedEntities             map[string]map[string]*model.ModelTTL[*model.Entity]
	cachedFeatureViews         map[string]map[string]*model.ModelTTL[*model.FeatureView]
	cachedSortedFeatureViews   map[string]map[string]*model.ModelTTL[*model.SortedFeatureView]
	cachedOnDemandFeatureViews map[string]map[string]*model.ModelTTL[*model.OnDemandFeatureView]
	cachedRegistry             *core.Registry
	cachedRegistryProtoTtl     time.Duration
	mu                         sync.RWMutex
}

func NewRegistry(registryConfig *RegistryConfig, repoPath string, project string) (*Registry, error) {
	registryStoreType := registryConfig.RegistryStoreType
	registryPath := registryConfig.Path
	r := &Registry{
		project:                project,
		cachedRegistryProtoTtl: time.Duration(registryConfig.CacheTtlSeconds) * time.Second,
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
		if _, ok := r.registryStore.(*HttpRegistryStore); ok {
			log.Error().Err(err).Msg("Registry Initialization Failed")
			return err
		}
		registryProto := &core.Registry{RegistrySchemaVersion: REGISTRY_SCHEMA_VERSION}
		r.registryStore.UpdateRegistryProto(registryProto)
	}
	r.cachedRegistry = registryProto
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
		expireCachedModels(r.cachedEntities, r.cachedRegistryProtoTtl, r.GetEntityFromRegistry, r)
		expireCachedModels(r.cachedFeatureServices, r.cachedRegistryProtoTtl, r.GetFeatureServiceFromRegistry, r)
		expireCachedModels(r.cachedFeatureViews, r.cachedRegistryProtoTtl, r.GetFeatureViewFromRegistry, r)
		expireCachedModels(r.cachedSortedFeatureViews, r.cachedRegistryProtoTtl, r.GetSortedFeatureViewFromRegistry, r)
		expireCachedModels(r.cachedOnDemandFeatureViews, r.cachedRegistryProtoTtl, r.GetOnDemandFeatureViewFromRegistry, r)
	} else {
		registryProto, err := r.registryStore.GetRegistryProto()
		if err != nil {
			return err
		}
		r.load(registryProto)
	}
	return nil
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

func expireCachedModels[T any](cachedModels map[string]map[string]*model.ModelTTL[T], ttl time.Duration, getModel func(string, string) (T, error), r *Registry) {
	r.mu.Lock()
	tempCacheMap := deepCopy(cachedModels)
	r.mu.Unlock()

	for project, cache := range tempCacheMap {
		for modelName, cacheItem := range cache {
			if cacheItem.IsExpired() {
				newModel, err := getModel(modelName, project)
				if err != nil {
					delete(cache, modelName)
				} else {
					cache[modelName] = model.NewModelTTLWithExpiration(newModel, ttl)
				}
			}
		}
	}

	r.mu.Lock()
	cachedModels = deepCopy(tempCacheMap)
	r.mu.Unlock()
}

func (r *Registry) clearCache() {
	r.cachedFeatureServices = make(map[string]map[string]*model.ModelTTL[*model.FeatureService])
	r.cachedEntities = make(map[string]map[string]*model.ModelTTL[*model.Entity])
	r.cachedFeatureViews = make(map[string]map[string]*model.ModelTTL[*model.FeatureView])
	r.cachedSortedFeatureViews = make(map[string]map[string]*model.ModelTTL[*model.SortedFeatureView])
	r.cachedOnDemandFeatureViews = make(map[string]map[string]*model.ModelTTL[*model.OnDemandFeatureView])
}

func (r *Registry) SetModels(
	featureServices []*core.FeatureService,
	entities []*core.Entity,
	fvs []*core.FeatureView,
	sfvs []*core.SortedFeatureView,
	odfvs []*core.OnDemandFeatureView) {
	r.clearCache()
	loadModels(featureServices, r.cachedFeatureServices, model.NewFeatureServiceFromProto, r.project, r.cachedRegistryProtoTtl)
	loadModels(entities, r.cachedEntities, model.NewEntityFromProto, r.project, r.cachedRegistryProtoTtl)
	loadModels(fvs, r.cachedFeatureViews, model.NewFeatureViewFromProto, r.project, r.cachedRegistryProtoTtl)
	loadModels(sfvs, r.cachedSortedFeatureViews, model.NewSortedFeatureViewFromProto, r.project, r.cachedRegistryProtoTtl)
	loadModels(odfvs, r.cachedOnDemandFeatureViews, model.NewOnDemandFeatureViewFromProto, r.project, r.cachedRegistryProtoTtl)
}

func (r *Registry) load(registry *core.Registry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cachedRegistry = registry
	r.SetModels(registry.FeatureServices, registry.Entities, registry.FeatureViews, registry.SortedFeatureViews, registry.OnDemandFeatureViews)
}

func loadModels[U any, T any](protoList []U, cachedModels map[string]map[string]*model.ModelTTL[T], modelFactory func(proto U) T, project string, ttl time.Duration) {
	for _, proto := range protoList {
		if _, ok := cachedModels[project]; !ok {
			cachedModels[project] = make(map[string]*model.ModelTTL[T])
		}
		obj := modelFactory(proto)
		nameField := reflect.ValueOf(obj).Elem().FieldByName("Name")
		if !nameField.IsValid() {
			nameField = reflect.ValueOf(obj).Elem().FieldByName("Base").Elem().FieldByName("Name")
		}
		objName := nameField.Interface().(string)
		cachedModels[project][objName] = model.NewModelTTLWithExpiration(obj, ttl)
	}

}

func (r *Registry) GetEntity(project string, entityName string) (*model.Entity, error) {
	r.mu.RLock()
	cachedEntities, ok := r.cachedEntities[project]
	r.mu.RUnlock()
	if !ok && r.registryStore.HasFallback() {
		return r.GetEntityFromRegistry(entityName, project)
	} else if !ok {
		return nil, fmt.Errorf("no cached entities found for project %s", project)
	}

	cachedEntity, modelOk := cachedEntities[entityName]
	if !modelOk {
		if r.registryStore.HasFallback() {
			return r.GetEntityFromRegistry(entityName, project)
		} else {
			return nil, fmt.Errorf("no cached entity %s found for project %s", entityName, project)
		}
	}
	return cachedEntity.Model, nil
}

func (r *Registry) GetEntityFromRegistry(entityName string, project string) (*model.Entity, error) {
	entityProto, err := r.registryStore.(*HttpRegistryStore).getEntity(entityName, true)
	if err != nil {
		log.Error().Err(err).Msgf("no entity %s found in project %s", entityName, project)
		return nil, fmt.Errorf("no entity %s found in project %s", entityName, project)
	}
	r.cachedEntities[project][entityName] = model.NewModelTTLWithExpiration(model.NewEntityFromProto(entityProto), r.cachedRegistryProtoTtl)
	return model.NewEntityFromProto(entityProto), nil
}

func (r *Registry) GetFeatureView(project string, featureViewName string) (*model.FeatureView, error) {
	r.mu.RLock()
	cachedFeatureViews, ok := r.cachedFeatureViews[project]
	r.mu.RUnlock()
	if !ok && r.registryStore.HasFallback() {
		return r.GetFeatureViewFromRegistry(featureViewName, project)
	} else if !ok {
		return nil, fmt.Errorf("no cached feature views found for project %s", project)
	}

	cachedFeatureView, modelOk := cachedFeatureViews[featureViewName]
	if !modelOk {
		if r.registryStore.HasFallback() {
			return r.GetFeatureViewFromRegistry(featureViewName, project)
		} else {
			return nil, fmt.Errorf("no cached feature view %s found for project %s", featureViewName, project)
		}
	}
	return cachedFeatureView.Model, nil
}

func (r *Registry) GetFeatureViewFromRegistry(featureViewName string, project string) (*model.FeatureView, error) {
	featureViewProto, err := r.registryStore.(*HttpRegistryStore).getFeatureView(featureViewName, true)
	if err != nil {
		log.Error().Err(err).Msgf("no feature view %s found in project %s", featureViewName, project)
		return nil, fmt.Errorf("no feature view %s found in project %s", featureViewName, project)
	}
	r.cachedFeatureViews[project][featureViewName] = model.NewModelTTLWithExpiration(model.NewFeatureViewFromProto(featureViewProto), r.cachedRegistryProtoTtl)
	return model.NewFeatureViewFromProto(featureViewProto), nil
}

func (r *Registry) GetSortedFeatureView(project string, sortedFeatureViewName string) (*model.SortedFeatureView, error) {
	r.mu.RLock()
	cachedSortedFeatureViews, ok := r.cachedSortedFeatureViews[project]
	r.mu.RUnlock()
	if !ok && r.registryStore.HasFallback() {
		return r.GetSortedFeatureViewFromRegistry(sortedFeatureViewName, project)
	} else if !ok {
		return nil, fmt.Errorf("no cached sorted feature views found for project %s", project)
	}

	cachedSortedFeatureView, modelOk := cachedSortedFeatureViews[sortedFeatureViewName]
	if !modelOk {
		if r.registryStore.HasFallback() {
			return r.GetSortedFeatureViewFromRegistry(sortedFeatureViewName, project)
		} else {
			return nil, fmt.Errorf("no cached sorted feature view %s found for project %s", sortedFeatureViewName, project)
		}
	}
	return cachedSortedFeatureView.Model, nil
}

func (r *Registry) GetSortedFeatureViewFromRegistry(sortedFeatureViewName string, project string) (*model.SortedFeatureView, error) {
	sortedFeatureViewProto, err := r.registryStore.(*HttpRegistryStore).getSortedFeatureView(sortedFeatureViewName, true)
	if err != nil {
		log.Error().Err(err).Msgf("no sorted feature view %s found in project %s", sortedFeatureViewName, project)
		return nil, fmt.Errorf("no sorted feature view %s found in project %s", sortedFeatureViewName, project)
	}
	r.cachedSortedFeatureViews[project][sortedFeatureViewName] = model.NewModelTTLWithExpiration(model.NewSortedFeatureViewFromProto(sortedFeatureViewProto), r.cachedRegistryProtoTtl)
	return model.NewSortedFeatureViewFromProto(sortedFeatureViewProto), nil
}

func (r *Registry) GetFeatureService(project string, featureServiceName string) (*model.FeatureService, error) {
	r.mu.RLock()
	cachedFeatureServices, ok := r.cachedFeatureServices[project]
	r.mu.RUnlock()
	if !ok && r.registryStore.HasFallback() {
		return r.GetFeatureServiceFromRegistry(featureServiceName, project)
	} else if !ok {
		return nil, fmt.Errorf("no cached feature services found for project %s", project)
	}

	cachedFeatureService, modelOk := cachedFeatureServices[featureServiceName]
	if !modelOk {
		if r.registryStore.HasFallback() {
			return r.GetFeatureServiceFromRegistry(featureServiceName, project)
		} else {
			return nil, fmt.Errorf("no cached feature service %s found for project %s", featureServiceName, project)
		}
	}
	return cachedFeatureService.Model, nil
}

func (r *Registry) GetFeatureServiceFromRegistry(featureServiceName string, project string) (*model.FeatureService, error) {
	featureServiceProto, err := r.registryStore.(*HttpRegistryStore).getFeatureService(featureServiceName, true)
	if err != nil {
		log.Error().Err(err).Msgf("no feature service %s found in project %s", featureServiceName, project)
		return nil, fmt.Errorf("no feature service %s found in project %s", featureServiceName, project)
	}
	r.cachedFeatureServices[project][featureServiceName] = model.NewModelTTLWithExpiration(model.NewFeatureServiceFromProto(featureServiceProto), r.cachedRegistryProtoTtl)
	return model.NewFeatureServiceFromProto(featureServiceProto), nil
}

func (r *Registry) GetOnDemandFeatureView(project string, onDemandFeatureViewName string) (*model.OnDemandFeatureView, error) {
	r.mu.RLock()
	cachedOnDemandFeatureViews, ok := r.cachedOnDemandFeatureViews[project]
	r.mu.RUnlock()
	if !ok && r.registryStore.HasFallback() {
		return r.GetOnDemandFeatureViewFromRegistry(onDemandFeatureViewName, project)
	} else if !ok {
		return nil, fmt.Errorf("no cached on demand feature views found for project %s", project)
	}

	cachedOnDemandFeatureView, modelOk := cachedOnDemandFeatureViews[onDemandFeatureViewName]
	if !modelOk {
		if r.registryStore.HasFallback() {
			return r.GetOnDemandFeatureViewFromRegistry(onDemandFeatureViewName, project)
		} else {
			return nil, fmt.Errorf("no cached on demand feature view %s found for project %s", onDemandFeatureViewName, project)
		}
	}
	return cachedOnDemandFeatureView.Model, nil
}

func (r *Registry) GetOnDemandFeatureViewFromRegistry(onDemandFeatureViewName string, project string) (*model.OnDemandFeatureView, error) {
	onDemandFeatureViewProto, err := r.registryStore.(*HttpRegistryStore).getOnDemandFeatureView(onDemandFeatureViewName, true)
	if err != nil {
		log.Error().Err(err).Msgf("no on demand feature view %s found in project %s", onDemandFeatureViewName, project)
		return nil, fmt.Errorf("no on demand feature view %s found in project %s", onDemandFeatureViewName, project)
	}
	r.cachedOnDemandFeatureViews[project][onDemandFeatureViewName] = model.NewModelTTLWithExpiration(model.NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto), r.cachedRegistryProtoTtl)
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
	return nil, fmt.Errorf("registry path %s has unsupported scheme %s. Supported schemes are file, s3 and gs", registryPath, uri.Scheme)
}

func getRegistryStoreFromType(registryStoreType string, registryConfig *RegistryConfig, repoPath string, project string) (RegistryStore, error) {
	switch registryStoreType {
	case "FileRegistryStore":
		return NewFileRegistryStore(registryConfig, repoPath), nil
	case "HttpRegistryStore":
		return NewHttpRegistryStore(registryConfig, project)
	}
	return nil, errors.New("only FileRegistryStore or HttpRegistryStore as a RegistryStore is supported at this moment")
}
