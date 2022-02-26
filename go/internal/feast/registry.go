package feast

import (
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
)

/*
	Store protos of FeatureView, FeatureService, Entity,
		OnDemandFeatureView, RequestFeatureView
	but return to user copies of non-proto versions of these objects
*/

type Registry struct {
	path                       string
	cachedFeatureServices      map[string]map[string]*core.FeatureService
	cachedEntities             map[string]map[string]*core.Entity
	cachedFeatureViews         map[string]map[string]*core.FeatureView
	cachedOnDemandFeatureViews map[string]map[string]*core.OnDemandFeatureView
	cachedRequestFeatureViews  map[string]map[string]*core.RequestFeatureView
}

func NewRegistry(path string) (*Registry, error) {
	// Read the local registry
	in, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	registry := &core.Registry{}
	if err := proto.Unmarshal(in, registry); err != nil {
		return nil, err
	}
	r := &Registry{path: path,
		cachedFeatureServices:      make(map[string]map[string]*core.FeatureService),
		cachedEntities:             make(map[string]map[string]*core.Entity),
		cachedFeatureViews:         make(map[string]map[string]*core.FeatureView),
		cachedOnDemandFeatureViews: make(map[string]map[string]*core.OnDemandFeatureView),
		cachedRequestFeatureViews:  make(map[string]map[string]*core.RequestFeatureView),
	}
	r.loadEntities(registry)
	r.loadFeatureServices(registry)
	r.loadFeatureViews(registry)
	r.loadOnDemandFeatureViews(registry)
	r.loadRequestFeatureViews(registry)

	return r, nil
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

		// newRequestFeatureView := *requestFeatureView
		r.cachedRequestFeatureViews[requestFeatureView.Spec.Project][requestFeatureView.Spec.Name] = requestFeatureView
	}
}

// TODO (Ly): Create Entity Object and return a list of Entities instead
func (r *Registry) listEntities(project string) ([]*Entity, error) {
	if entities, ok := r.cachedEntities[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listEntities", project))
	} else {
		entityList := make([]*Entity, len(entities))
		index := 0
		for _, entity := range entities {
			entityList[index] = NewEntityFromProto(entity)
			index += 1
		}
		return entityList, nil
	}
}

func (r *Registry) listFeatureViews(project string) ([]*FeatureView, error) {
	if featureViewProtos, ok := r.cachedFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listFeatureViews", project))
	} else {
		featureViews := make([]*FeatureView, len(featureViewProtos))
		index := 0
		for _, featureViewProto := range featureViewProtos {
			featureViews[index] = NewFeatureViewFromProto(featureViewProto)
			index += 1
		}
		return featureViews, nil
	}
}

func (r *Registry) listFeatureServices(project string) ([]*FeatureService, error) {
	if featureServiceProtos, ok := r.cachedFeatureServices[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listFeatureServices", project))
	} else {
		featureServices := make([]*FeatureService, len(featureServiceProtos))
		index := 0
		for _, featureServiceProto := range featureServiceProtos {
			featureServices[index] = NewFeatureServiceFromProto(featureServiceProto)
			index += 1
		}
		return featureServices, nil
	}
}

func (r *Registry) listOnDemandFeatureViews(project string) ([]*OnDemandFeatureView, error) {
	if onDemandFeatureViewProtos, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listOnDemandFeatureViews", project))
	} else {
		onDemandFeatureViews := make([]*OnDemandFeatureView, len(onDemandFeatureViewProtos))
		index := 0
		for _, onDemandFeatureViewProto := range onDemandFeatureViewProtos {
			onDemandFeatureViews[index] = NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto)
			index += 1
		}
		return onDemandFeatureViews, nil
	}
}

func (r *Registry) listRequestFeatureViews(project string) ([]*RequestFeatureView, error) {
	if requestFeatureViewProtos, ok := r.cachedRequestFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listRequestFeatureViews", project))
	} else {
		requestFeatureViews := make([]*RequestFeatureView, len(requestFeatureViewProtos))
		index := 0
		for _, requestFeatureViewProto := range requestFeatureViewProtos {
			requestFeatureViews[index] = NewRequestFeatureViewFromProto(requestFeatureViewProto)
			index += 1
		}
		return requestFeatureViews, nil
	}
}

func (r *Registry) getEntity(project, entityName string) (*Entity, error) {
	if entities, ok := r.cachedEntities[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getEntity", project))
	} else {
		if entity, ok := entities[entityName]; !ok {
			return nil, errors.New(fmt.Sprintf("Entity %s not found inside project %s", entityName, project))
		} else {
			return NewEntityFromProto(entity), nil
		}
	}
}

func (r *Registry) getFeatureView(project, featureViewName string) (*FeatureView, error) {
	if featureViews, ok := r.cachedFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getFeatureView", project))
	} else {
		if featureViewProto, ok := featureViews[featureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("FeatureView %s not found inside project %s", featureViewName, project))
		} else {
			return NewFeatureViewFromProto(featureViewProto), nil
		}
	}
}

func (r *Registry) getFeatureService(project, featureServiceName string) (*FeatureService, error) {
	if featureServices, ok := r.cachedFeatureServices[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getFeatureService", project))
	} else {
		if featureServiceProto, ok := featureServices[featureServiceName]; !ok {
			return nil, errors.New(fmt.Sprintf("FeatureService %s not found inside project %s", featureServiceName, project))
		} else {
			return NewFeatureServiceFromProto(featureServiceProto), nil
		}
	}
}

func (r *Registry) getOnDemandFeatureView(project, onDemandFeatureViewName string) (*OnDemandFeatureView, error) {
	if onDemandFeatureViews, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getOnDemandFeatureView", project))
	} else {
		if onDemandFeatureViewProto, ok := onDemandFeatureViews[onDemandFeatureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("OnDemandFeatureView %s not found inside project %s", onDemandFeatureViewName, project))
		} else {
			return NewOnDemandFeatureViewFromProto(onDemandFeatureViewProto), nil
		}
	}
}

func (r *Registry) getRequestFeatureView(project, requestFeatureViewName string) (*RequestFeatureView, error) {
	if requestFeatureViews, ok := r.cachedRequestFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getRequestFeatureView", project))
	} else {
		if requestFeatureViewProto, ok := requestFeatureViews[requestFeatureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("RequestFeatureView %s not found inside project %s", requestFeatureViewName, project))
		} else {
			return NewRequestFeatureViewFromProto(requestFeatureViewProto), nil
		}
	}
}
