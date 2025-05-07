package registry

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/rs/zerolog/log"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

var REGISTRY_SCHEMA_VERSION string = "1"
var REGISTRY_STORE_CLASS_FOR_SCHEME map[string]string = map[string]string{
	"gs":   "GCSRegistryStore",
	"s3":   "S3RegistryStore",
	"file": "FileRegistryStore",
	"":     "FileRegistryStore",
}

/*
	Store protos of FeatureView, FeatureService, Entity, OnDemandFeatureView
	but return to user copies of non-proto versions of these objects
*/

type Registry struct {
	project                        string
	registryStore                  RegistryStore
	cachedFeatureServices          map[string]map[string]*core.FeatureService
	cachedEntities                 map[string]map[string]*core.Entity
	cachedFeatureViews             map[string]map[string]*core.FeatureView
	cachedStreamFeatureViews       map[string]map[string]*core.StreamFeatureView
	cachedOnDemandFeatureViews     map[string]map[string]*core.OnDemandFeatureView
	cachedRegistry                 *core.Registry
	cachedRegistryProtoLastUpdated time.Time
	cachedRegistryProtoTtl         time.Duration
	mu                             sync.RWMutex
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
	_, err := r.getRegistryProto()
	if err != nil {
		if _, ok := r.registryStore.(*FileRegistryStore); ok {
			log.Error().Err(err).Msg("Registry Initialization Failed")
			return err
		}
		registryProto := &core.Registry{RegistrySchemaVersion: REGISTRY_SCHEMA_VERSION}
		r.registryStore.UpdateRegistryProto(registryProto)
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
	_, err := r.getRegistryProto()
	return err
}

func (r *Registry) getRegistryProto() (*core.Registry, error) {
	expired := r.cachedRegistry == nil || (r.cachedRegistryProtoTtl > 0 && time.Now().After(r.cachedRegistryProtoLastUpdated.Add(r.cachedRegistryProtoTtl)))
	if !expired {
		return r.cachedRegistry, nil
	}
	registryProto, err := r.registryStore.GetRegistryProto()
	if err != nil {
		return nil, err
	}
	r.load(registryProto)
	return registryProto, nil
}

func (r *Registry) load(registry *core.Registry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cachedRegistry = registry
	r.cachedFeatureServices = make(map[string]map[string]*core.FeatureService)
	r.cachedEntities = make(map[string]map[string]*core.Entity)
	r.cachedFeatureViews = make(map[string]map[string]*core.FeatureView)
	r.cachedStreamFeatureViews = make(map[string]map[string]*core.StreamFeatureView)
	r.cachedOnDemandFeatureViews = make(map[string]map[string]*core.OnDemandFeatureView)
	r.loadEntities(registry)
	r.loadFeatureServices(registry)
	r.loadFeatureViews(registry)
	r.loadStreamFeatureViews(registry)
	r.loadOnDemandFeatureViews(registry)
	r.cachedRegistryProtoLastUpdated = time.Now()
}

func (r *Registry) loadEntities(registry *core.Registry) {
	entities := registry.Entities
	for _, entity := range entities {
		if _, ok := r.cachedEntities[r.project]; !ok {
			r.cachedEntities[r.project] = make(map[string]*core.Entity)
		}
		r.cachedEntities[r.project][entity.Spec.Name] = entity
	}
}

func (r *Registry) loadFeatureServices(registry *core.Registry) {
	featureServices := registry.FeatureServices
	for _, featureService := range featureServices {
		if _, ok := r.cachedFeatureServices[r.project]; !ok {
			r.cachedFeatureServices[r.project] = make(map[string]*core.FeatureService)
		}
		r.cachedFeatureServices[r.project][featureService.Spec.Name] = featureService
	}
}

func (r *Registry) loadFeatureViews(registry *core.Registry) {
	featureViews := registry.FeatureViews
	for _, featureView := range featureViews {
		if _, ok := r.cachedFeatureViews[r.project]; !ok {
			r.cachedFeatureViews[r.project] = make(map[string]*core.FeatureView)
		}
		r.cachedFeatureViews[r.project][featureView.Spec.Name] = featureView
	}
}

func (r *Registry) loadStreamFeatureViews(registry *core.Registry) {
	streamFeatureViews := registry.StreamFeatureViews
	for _, streamFeatureView := range streamFeatureViews {
		if _, ok := r.cachedStreamFeatureViews[r.project]; !ok {
			r.cachedStreamFeatureViews[r.project] = make(map[string]*core.StreamFeatureView)
		}
		r.cachedStreamFeatureViews[r.project][streamFeatureView.Spec.Name] = streamFeatureView
	}
}

func (r *Registry) loadOnDemandFeatureViews(registry *core.Registry) {
	onDemandFeatureViews := registry.OnDemandFeatureViews
	for _, onDemandFeatureView := range onDemandFeatureViews {
		if _, ok := r.cachedOnDemandFeatureViews[r.project]; !ok {
			r.cachedOnDemandFeatureViews[r.project] = make(map[string]*core.OnDemandFeatureView)
		}
		r.cachedOnDemandFeatureViews[r.project][onDemandFeatureView.Spec.Name] = onDemandFeatureView
	}
}

/*
	Look up Entities inside project
	Returns empty list if project not found
*/

func (r *Registry) ListEntities(project string) ([]*model.Entity, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedEntities, ok := r.cachedEntities[project]; !ok {
		return []*model.Entity{}, nil
	} else {
		entities := make([]*model.Entity, len(cachedEntities))
		index := 0
		for _, entityProto := range cachedEntities {
			entities[index] = model.NewEntityFromProto(entityProto)
			index += 1
		}
		return entities, nil
	}
}

/*
	Look up Feature Views inside project
	Returns empty list if project not found
*/

func (r *Registry) ListFeatureViews(project string) ([]*model.FeatureView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedFeatureViews, ok := r.cachedFeatureViews[project]; !ok {
		return []*model.FeatureView{}, nil
	} else {
		featureViews := make([]*model.FeatureView, len(cachedFeatureViews))
		index := 0
		for _, featureViewProto := range cachedFeatureViews {
			featureViews[index] = model.NewFeatureViewFromProto(featureViewProto)
			index += 1
		}
		return featureViews, nil
	}
}

/*
	Look up Stream Feature Views inside project
	Returns empty list if project not found
*/

func (r *Registry) ListStreamFeatureViews(project string) ([]*model.FeatureView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedStreamFeatureViews, ok := r.cachedStreamFeatureViews[project]; !ok {
		return []*model.FeatureView{}, nil
	} else {
		streamFeatureViews := make([]*model.FeatureView, len(cachedStreamFeatureViews))
		index := 0
		for _, streamFeatureViewProto := range cachedStreamFeatureViews {
			streamFeatureViews[index] = model.NewFeatureViewFromStreamFeatureViewProto(streamFeatureViewProto)
			index += 1
		}
		return streamFeatureViews, nil
	}
}

/*
	Look up Feature Services inside project
	Returns empty list if project not found
*/

func (r *Registry) ListFeatureServices(project string) ([]*model.FeatureService, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedFeatureServices, ok := r.cachedFeatureServices[project]; !ok {
		return []*model.FeatureService{}, nil
	} else {
		featureServices := make([]*model.FeatureService, len(cachedFeatureServices))
		index := 0
		for _, featureServiceProto := range cachedFeatureServices {
			featureServices[index] = model.NewFeatureServiceFromProto(featureServiceProto)
			index += 1
		}
		return featureServices, nil
	}
}

/*
	Look up On Demand Feature Views inside project
	Returns empty list if project not found
*/

func (r *Registry) ListOnDemandFeatureViews(project string) ([]*model.OnDemandFeatureView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedOnDemandFeatureViews, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return []*model.OnDemandFeatureView{}, nil
	} else {
		onDemandFeatureViews := make([]*model.OnDemandFeatureView, len(cachedOnDemandFeatureViews))
		index := 0
		for _, onDemandFeatureViewProto := range cachedOnDemandFeatureViews {
			onDemandFeatureViews[index] = model.NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto)
			index += 1
		}
		return onDemandFeatureViews, nil
	}
}

func (r *Registry) GetEntity(project, entityName string) (*model.Entity, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedEntities, ok := r.cachedEntities[project]; !ok {
		return nil, fmt.Errorf("no cached entities found for project %s", project)
	} else {
		if entity, ok := cachedEntities[entityName]; !ok {
			return nil, fmt.Errorf("no cached entity %s found for project %s", entityName, project)
		} else {
			return model.NewEntityFromProto(entity), nil
		}
	}
}

func (r *Registry) GetFeatureView(project, featureViewName string) (*model.FeatureView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedFeatureViews, ok := r.cachedFeatureViews[project]; !ok {
		return nil, fmt.Errorf("no cached feature views found for project %s", project)
	} else {
		if featureViewProto, ok := cachedFeatureViews[featureViewName]; !ok {
			return nil, fmt.Errorf("no cached feature view %s found for project %s", featureViewName, project)
		} else {
			return model.NewFeatureViewFromProto(featureViewProto), nil
		}
	}
}

func (r *Registry) GetStreamFeatureView(project, streamFeatureViewName string) (*model.FeatureView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedStreamFeatureViews, ok := r.cachedStreamFeatureViews[project]; !ok {
		return nil, fmt.Errorf("no cached stream feature views found for project %s", project)
	} else {
		if streamFeatureViewProto, ok := cachedStreamFeatureViews[streamFeatureViewName]; !ok {
			return nil, fmt.Errorf("no cached stream feature view %s found for project %s", streamFeatureViewName, project)
		} else {
			return model.NewFeatureViewFromStreamFeatureViewProto(streamFeatureViewProto), nil
		}
	}
}

func (r *Registry) GetFeatureService(project, featureServiceName string) (*model.FeatureService, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedFeatureServices, ok := r.cachedFeatureServices[project]; !ok {
		return nil, fmt.Errorf("no cached feature services found for project %s", project)
	} else {
		if featureServiceProto, ok := cachedFeatureServices[featureServiceName]; !ok {
			return nil, fmt.Errorf("no cached feature service %s found for project %s", featureServiceName, project)
		} else {
			return model.NewFeatureServiceFromProto(featureServiceProto), nil
		}
	}
}

func (r *Registry) GetOnDemandFeatureView(project, onDemandFeatureViewName string) (*model.OnDemandFeatureView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if cachedOnDemandFeatureViews, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return nil, fmt.Errorf("no cached on demand feature views found for project %s", project)
	} else {
		if onDemandFeatureViewProto, ok := cachedOnDemandFeatureViews[onDemandFeatureViewName]; !ok {
			return nil, fmt.Errorf("no cached on demand feature view %s found for project %s", onDemandFeatureViewName, project)
		} else {
			return model.NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto), nil
		}
	}
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
	case "S3RegistryStore":
		return NewS3RegistryStore(registryConfig, repoPath), nil
	}
	return nil, errors.New("only FileRegistryStore as a RegistryStore is supported at this moment")
}
