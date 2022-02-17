package feast

import (
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
)

type Registry struct {
	path 				string
	// TODO: add more cache and fields
	// as needed
	// cachedRegistry		*core.Registry

	// Each of the following map
	// maps from project to list of fields
	// Further optimization could be to create a Project Object
	// so that we reduces the repeated project names in each map
	cachedFeatureServices		map[string]map[string]*core.FeatureService
	cachedEntities				map[string]map[string]*core.Entity
	cachedFeatureViews			map[string]map[string]*core.FeatureView
	cachedOnDemandFeatureViews	map[string]map[string]*core.OnDemandFeatureView
	cachedRequestFeatureViews	map[string]map[string]*core.RequestFeatureView

	// TODO (Ly): Support cache reload
    // cachedRegistryProtoCreated 	time.Time
    // cachedRegistryProtoTtl		time.Time
}

func NewRegistry(path string) (*Registry, error) {
	fmt.Println(path)
	// Read the local registry
	in, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	registry := &core.Registry{}
	if err := proto.Unmarshal(in, registry); err != nil {
		return nil, err
	}
	r := &Registry{	path: path,
					// cachedRegistry: registry,
					cachedFeatureServices: make(map[string]map[string]*core.FeatureService),
					cachedEntities: make(map[string]map[string]*core.Entity),
					cachedFeatureViews: make(map[string]map[string]*core.FeatureView),
					cachedOnDemandFeatureViews: make(map[string]map[string]*core.OnDemandFeatureView),
					cachedRequestFeatureViews: make(map[string]map[string]*core.RequestFeatureView),
				}
	r.LoadEntities(registry)
	r.LoadFeatureServices(registry)
	r.LoadFeatureViews(registry)
	r.LoadOnDemandFeatureViews(registry)
	r.LoadRequestFeatureViews(registry)
	return r, nil
}

func (r *Registry) LoadEntities(registry *core.Registry) {
	entities := registry.Entities
	for _, entity := range entities {
		if _, ok := r.cachedEntities[entity.Spec.Project]; !ok {
			r.cachedEntities[entity.Spec.Project] = make(map[string]*core.Entity)
		}
		// newEntity := *entity
		r.cachedEntities[entity.Spec.Project][entity.Spec.Name] = entity
	}
}

func (r *Registry) LoadFeatureServices(registry *core.Registry) {
	featureServices := registry.FeatureServices
	for _, featureService := range featureServices {
		if _, ok := r.cachedFeatureServices[featureService.Spec.Project]; !ok {
			r.cachedFeatureServices[featureService.Spec.Project] = make(map[string]*core.FeatureService)
		}
		// newFeatureService := *featureService
		r.cachedFeatureServices[featureService.Spec.Project][featureService.Spec.Name] = featureService
	}
}

func (r *Registry) LoadFeatureViews(registry *core.Registry) {
	featureViews := registry.FeatureViews
	for _, featureView := range featureViews {
		if _, ok := r.cachedFeatureViews[featureView.Spec.Project]; !ok {
			r.cachedFeatureViews[featureView.Spec.Project] = make(map[string]*core.FeatureView)
		}
		// fmt.Println(featureView.Spec.Name, "hiiii")
		// newFeatureView := *featureView
		r.cachedFeatureViews[featureView.Spec.Project][featureView.Spec.Name] = featureView
	}
}

func (r *Registry) LoadOnDemandFeatureViews(registry *core.Registry) {
	onDemandFeatureViews := registry.OnDemandFeatureViews
	for _, onDemandFeatureView := range onDemandFeatureViews {
		if _, ok := r.cachedOnDemandFeatureViews[onDemandFeatureView.Spec.Project]; !ok {
			r.cachedOnDemandFeatureViews[onDemandFeatureView.Spec.Project] = make(map[string]*core.OnDemandFeatureView)
		}
		// newOnDemandFeatureView := *onDemandFeatureView
		r.cachedOnDemandFeatureViews[onDemandFeatureView.Spec.Project][onDemandFeatureView.Spec.Name] = onDemandFeatureView
	}
}

func (r *Registry) LoadRequestFeatureViews(registry *core.Registry) {
	requestFeatureViews := registry.RequestFeatureViews
	for _, requestFeatureView := range requestFeatureViews {
		if _, ok := r.cachedRequestFeatureViews[requestFeatureView.Spec.Project]; !ok {
			r.cachedRequestFeatureViews[requestFeatureView.Spec.Project] = make(map[string]*core.RequestFeatureView)
		}

		// newRequestFeatureView := *requestFeatureView
		r.cachedRequestFeatureViews[requestFeatureView.Spec.Project][requestFeatureView.Spec.Name] = requestFeatureView
	}
}

// TODO (Ly): Support and fix these functions if needed
func (r *Registry) listEntities(project string) ([]*core.Entity, error) {
	if entities, ok := r.cachedEntities[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listEntities", project))
	} else {
		entityList := make([]*core.Entity, len(entities))
		index := 0
		for _, entity := range entities {
			entityList[index] = entity
			index += 1
		}
		return entityList, nil
	}
}

func (r *Registry) listFeatureViews(project string) ([]*core.FeatureView, error) {
	if featureViews, ok := r.cachedFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listFeatureViews", project))
	} else {
		featureViewList := make([]*core.FeatureView, len(featureViews))
		index := 0
		for _, featureView := range featureViews {
			featureViewList[index] = featureView
			index += 1
		}
		return featureViewList, nil
	}
}

func (r *Registry) listFeatureServices(project string) ([]*core.FeatureService, error) {
	if featureServices, ok := r.cachedFeatureServices[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listFeatureServices", project))
	} else {
		featureServiceList := make([]*core.FeatureService, len(featureServices))
		index := 0
		for _, featureService := range featureServices {
			featureServiceList[index] = featureService
			index += 1
		}
		return featureServiceList, nil
	}
}

func (r *Registry) listOnDemandFeatureViews(project string) ([]*core.OnDemandFeatureView, error) {
	if onDemandFeatureViews, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listOnDemandFeatureViews", project))
	} else {
		onDemandFeatureViewList := make([]*core.OnDemandFeatureView, len(onDemandFeatureViews))
		index := 0
		for _, onDemandFeatureView := range onDemandFeatureViews {
			onDemandFeatureViewList[index] = onDemandFeatureView
			index += 1
		}
		return onDemandFeatureViewList, nil
	}
}

func (r *Registry) listRequestFeatureViews(project string) ([]*core.RequestFeatureView, error) {
	if requestFeatureViews, ok := r.cachedRequestFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in listRequestFeatureViews", project))
	} else {
		requestFeatureViewList := make([]*core.RequestFeatureView, len(requestFeatureViews))
		index := 0
		for _, requestFeatureView := range requestFeatureViews {
			requestFeatureViewList[index] = requestFeatureView
			index += 1
		}
		return requestFeatureViewList, nil
	}
}

func (r *Registry) getEntity(project, entityName string) (*core.Entity, error) {
	if entities, ok := r.cachedEntities[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getEntity", project))
	} else {
		if entity, ok := entities[entityName]; !ok {
			return nil, errors.New(fmt.Sprintf("Entity %s not found inside project %s", entityName, project))
		} else {
			return entity, nil
		}
	}
}

func (r *Registry) getFeatureView(project, featureViewName string) (*core.FeatureView, error) {
	if featureViews, ok := r.cachedFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getFeatureView", project))
	} else {
		if featureView, ok := featureViews[featureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("FeatureView %s not found inside project %s", featureView, project))
		} else {
			return featureView, nil
		}
	}
}

func (r *Registry) getFeatureService(project, featureServiceName string) (*core.FeatureService, error) {
	if featureServices, ok := r.cachedFeatureServices[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getFeatureService", project))
	} else {
		if featureService, ok := featureServices[featureServiceName]; !ok {
			return nil, errors.New(fmt.Sprintf("FeatureService %s not found inside project %s", featureServiceName, project))
		} else {
			return featureService, nil
		}
	}
}

func (r *Registry) getOnDemandFeatureView(project, onDemandFeatureViewName string) (*core.OnDemandFeatureView, error) {
	if onDemandFeatureViews, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getOnDemandFeatureView", project))
	} else {
		if onDemandFeatureView, ok := onDemandFeatureViews[onDemandFeatureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("OnDemandFeatureView %s not found inside project %s", onDemandFeatureViewName, project))
		} else {
			return onDemandFeatureView, nil
		}
	}
}

func (r *Registry) getRequestFeatureView(project, requestFeatureViewName string) (*core.RequestFeatureView, error) {
	if requestFeatureViews, ok := r.cachedRequestFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found in getRequestFeatureView", project))
	} else {
		if requestFeatureView, ok := requestFeatureViews[requestFeatureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("RequestFeatureView %s not found inside project %s", requestFeatureViewName, project))
		} else {
			return requestFeatureView, nil
		}
	}
}
