package feast

import (
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"net/url"
	"time"
)

var REGISTRY_SCHEMA_VERSION string = "1"
var REGISTRY_STORE_CLASS_FOR_SCHEME map[string]string = map[string]string{
	"gs":   "GCSRegistryStore",
	"s3":   "S3RegistryStore",
	"file": "LocalRegistryStore",
	"":     "LocalRegistryStore",
}

/*
	Store protos of FeatureView, FeatureService, Entity, OnDemandFeatureView, RequestFeatureView
	but return to user copies of non-proto versions of these objects
*/

type Registry struct {
	registryStore              RegistryStore
	cachedFeatureServices      map[string]map[string]*core.FeatureService
	cachedEntities             map[string]map[string]*core.Entity
	cachedFeatureViews         map[string]map[string]*core.FeatureView
	cachedOnDemandFeatureViews map[string]map[string]*core.OnDemandFeatureView
	cachedRequestFeatureViews  map[string]map[string]*core.RequestFeatureView

	cachedRegistryProtoCreated time.Time
	cachedRegistryProtoTtl     time.Duration
}

func NewRegistry(registryConfig *RegistryConfig, repoPath string) (*Registry, error) {
	registryStoreType := registryConfig.RegistryStoreType
	registryPath := registryConfig.Path
	r := &Registry{
		cachedRegistryProtoTtl: time.Duration(registryConfig.CacheTtlSeconds),
	}

	if len(registryStoreType) == 0 {
		registryStore, err := getRegistryStoreFromSheme(registryPath, registryConfig, repoPath)
		if err != nil {
			return nil, err
		}
		r.registryStore = registryStore
	} else {
		registryStore, err := getRegistryStoreFromType(registryStoreType, registryConfig, repoPath)
		if err != nil {
			return nil, err
		}
		r.registryStore = registryStore
	}

	return r, nil
}

func (r *Registry) initializeRegistry() {
	err := r.getRegistryProto(false)
	if err != nil {
		registryProto := &core.Registry{RegistrySchemaVersion: REGISTRY_SCHEMA_VERSION}
		r.registryStore.UpdateRegistryProto(registryProto)
		r.load(registryProto)
	}
}

// TODO: Add a goroutine and automatically refresh every cachedRegistryProtoTtl
func (r *Registry) refresh() error {
	return r.getRegistryProto(false)
}

func (r *Registry) getRegistryProto(allowCache bool) error {
	expired := r.cachedFeatureServices == nil || (r.cachedRegistryProtoTtl > 0 && time.Now().After(r.cachedRegistryProtoCreated.Add(r.cachedRegistryProtoTtl)))
	if allowCache && !expired {
		return nil
	}
	registryProto, err := r.registryStore.GetRegistryProto()
	if err != nil {
		return err
	}
	r.load(registryProto)
	return nil
}

func (r *Registry) load(registry *core.Registry) {
	r.cachedFeatureServices = make(map[string]map[string]*core.FeatureService)
	r.cachedEntities = make(map[string]map[string]*core.Entity)
	r.cachedFeatureViews = make(map[string]map[string]*core.FeatureView)
	r.cachedOnDemandFeatureViews = make(map[string]map[string]*core.OnDemandFeatureView)
	r.cachedRequestFeatureViews = make(map[string]map[string]*core.RequestFeatureView)
	r.loadEntities(registry)
	r.loadFeatureServices(registry)
	r.loadFeatureViews(registry)
	r.loadOnDemandFeatureViews(registry)
	r.loadRequestFeatureViews(registry)
}

func (r *Registry) loadEntities(registry *core.Registry) {
	entities := registry.Entities
	for _, entity := range entities {
		if _, ok := r.cachedEntities[entity.Spec.Project]; !ok {
			r.cachedEntities[entity.Spec.Project] = make(map[string]*core.Entity)
		}
		r.cachedEntities[entity.Spec.Project][entity.Spec.Name] = entity
	}
}

func (r *Registry) loadFeatureServices(registry *core.Registry) {
	featureServices := registry.FeatureServices
	for _, featureService := range featureServices {
		if _, ok := r.cachedFeatureServices[featureService.Spec.Project]; !ok {
			r.cachedFeatureServices[featureService.Spec.Project] = make(map[string]*core.FeatureService)
		}
		r.cachedFeatureServices[featureService.Spec.Project][featureService.Spec.Name] = featureService
	}
}

func (r *Registry) loadFeatureViews(registry *core.Registry) {
	featureViews := registry.FeatureViews
	for _, featureView := range featureViews {
		if _, ok := r.cachedFeatureViews[featureView.Spec.Project]; !ok {
			r.cachedFeatureViews[featureView.Spec.Project] = make(map[string]*core.FeatureView)
		}
		r.cachedFeatureViews[featureView.Spec.Project][featureView.Spec.Name] = featureView
	}
}

func (r *Registry) loadOnDemandFeatureViews(registry *core.Registry) {
	onDemandFeatureViews := registry.OnDemandFeatureViews
	for _, onDemandFeatureView := range onDemandFeatureViews {
		if _, ok := r.cachedOnDemandFeatureViews[onDemandFeatureView.Spec.Project]; !ok {
			r.cachedOnDemandFeatureViews[onDemandFeatureView.Spec.Project] = make(map[string]*core.OnDemandFeatureView)
		}
		r.cachedOnDemandFeatureViews[onDemandFeatureView.Spec.Project][onDemandFeatureView.Spec.Name] = onDemandFeatureView
	}
}

func (r *Registry) loadRequestFeatureViews(registry *core.Registry) {
	requestFeatureViews := registry.RequestFeatureViews
	for _, requestFeatureView := range requestFeatureViews {
		if _, ok := r.cachedRequestFeatureViews[requestFeatureView.Spec.Project]; !ok {
			r.cachedRequestFeatureViews[requestFeatureView.Spec.Project] = make(map[string]*core.RequestFeatureView)
		}
		r.cachedRequestFeatureViews[requestFeatureView.Spec.Project][requestFeatureView.Spec.Name] = requestFeatureView
	}
}

/*
	Look up Entities inside project
	Returns empty list if project not found
*/

func (r *Registry) listEntities(project string) []*Entity {
	if entities, ok := r.cachedEntities[project]; !ok {
		return []*Entity{}
	} else {
		entityList := make([]*Entity, len(entities))
		index := 0
		for _, entity := range entities {
			entityList[index] = NewEntityFromProto(entity)
			index += 1
		}
		return entityList
	}
}

/*
	Look up Feature Views inside project
	Returns empty list if project not found
*/

func (r *Registry) listFeatureViews(project string) []*FeatureView {
	if featureViewProtos, ok := r.cachedFeatureViews[project]; !ok {
		return []*FeatureView{}
	} else {
		featureViews := make([]*FeatureView, len(featureViewProtos))
		index := 0
		for _, featureViewProto := range featureViewProtos {
			featureViews[index] = NewFeatureViewFromProto(featureViewProto)
			index += 1
		}
		return featureViews
	}
}

/*
	Look up Feature Views inside project
	Returns empty list if project not found
*/

func (r *Registry) listFeatureServices(project string) []*FeatureService {
	if featureServiceProtos, ok := r.cachedFeatureServices[project]; !ok {
		return []*FeatureService{}
	} else {
		featureServices := make([]*FeatureService, len(featureServiceProtos))
		index := 0
		for _, featureServiceProto := range featureServiceProtos {
			featureServices[index] = NewFeatureServiceFromProto(featureServiceProto)
			index += 1
		}
		return featureServices
	}
}

/*
	Look up On Demand Feature Views inside project
	Returns empty list if project not found
*/

func (r *Registry) listOnDemandFeatureViews(project string) []*OnDemandFeatureView {
	if onDemandFeatureViewProtos, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return []*OnDemandFeatureView{}
	} else {
		onDemandFeatureViews := make([]*OnDemandFeatureView, len(onDemandFeatureViewProtos))
		index := 0
		for _, onDemandFeatureViewProto := range onDemandFeatureViewProtos {
			onDemandFeatureViews[index] = NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto)
			index += 1
		}
		return onDemandFeatureViews
	}
}

/*
	Look up Request Feature Views inside project
	Returns empty list if project not found
*/

func (r *Registry) listRequestFeatureViews(project string) []*RequestFeatureView {
	if requestFeatureViewProtos, ok := r.cachedRequestFeatureViews[project]; !ok {
		return []*RequestFeatureView{}
	} else {
		requestFeatureViews := make([]*RequestFeatureView, len(requestFeatureViewProtos))
		index := 0
		for _, requestFeatureViewProto := range requestFeatureViewProtos {
			requestFeatureViews[index] = NewRequestFeatureViewFromProto(requestFeatureViewProto)
			index += 1
		}
		return requestFeatureViews
	}
}

func (r *Registry) getEntity(project, entityName string) (*Entity, error) {
	if entities, ok := r.cachedEntities[project]; !ok {
		return nil, errors.New(fmt.Sprintf("project %s not found in getEntity", project))
	} else {
		if entity, ok := entities[entityName]; !ok {
			return nil, errors.New(fmt.Sprintf("entity %s not found inside project %s", entityName, project))
		} else {
			return NewEntityFromProto(entity), nil
		}
	}
}

func (r *Registry) getFeatureView(project, featureViewName string) (*FeatureView, error) {
	if featureViews, ok := r.cachedFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("project %s not found in getFeatureView", project))
	} else {
		if featureViewProto, ok := featureViews[featureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("featureView %s not found inside project %s", featureViewName, project))
		} else {
			return NewFeatureViewFromProto(featureViewProto), nil
		}
	}
}

func (r *Registry) getFeatureService(project, featureServiceName string) (*FeatureService, error) {
	if featureServices, ok := r.cachedFeatureServices[project]; !ok {
		return nil, errors.New(fmt.Sprintf("project %s not found in getFeatureService", project))
	} else {
		if featureServiceProto, ok := featureServices[featureServiceName]; !ok {
			return nil, errors.New(fmt.Sprintf("featureService %s not found inside project %s", featureServiceName, project))
		} else {
			return NewFeatureServiceFromProto(featureServiceProto), nil
		}
	}
}

func (r *Registry) getOnDemandFeatureView(project, onDemandFeatureViewName string) (*OnDemandFeatureView, error) {
	if onDemandFeatureViews, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("project %s not found in getOnDemandFeatureView", project))
	} else {
		if onDemandFeatureViewProto, ok := onDemandFeatureViews[onDemandFeatureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("onDemandFeatureView %s not found inside project %s", onDemandFeatureViewName, project))
		} else {
			return NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto), nil
		}
	}
}

func (r *Registry) getRequestFeatureView(project, requestFeatureViewName string) (*RequestFeatureView, error) {
	if requestFeatureViews, ok := r.cachedRequestFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("project %s not found in getRequestFeatureView", project))
	} else {
		if requestFeatureViewProto, ok := requestFeatureViews[requestFeatureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("requestFeatureView %s not found inside project %s", requestFeatureViewName, project))
		} else {
			return NewRequestFeatureViewFromProto(requestFeatureViewProto), nil
		}
	}
}

func getRegistryStoreFromSheme(registryPath string, registryConfig *RegistryConfig, repoPath string) (RegistryStore, error) {
	uri, err := url.Parse(registryPath)
	if err != nil {
		return nil, err
	}
	if registryStoreType, ok := REGISTRY_STORE_CLASS_FOR_SCHEME[uri.Scheme]; ok {
		return getRegistryStoreFromType(registryStoreType, registryConfig, repoPath)
	}
	return nil, errors.New(fmt.Sprintf("registry path %s has unsupported scheme %s. Supported schemes are file, s3 and gs.", registryPath, uri.Scheme))
}

func getRegistryStoreFromType(registryStoreType string, registryConfig *RegistryConfig, repoPath string) (RegistryStore, error) {
	switch registryStoreType {
	case "LocalRegistryStore":
		return NewLocalRegistryStore(registryConfig, repoPath), nil
	}
	return nil, errors.New("only LocalRegistryStore as a RegistryStore is supported at this moment")
}
