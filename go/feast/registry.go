package feast

import (
	"time"
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Registry struct {
	path 				string
	cachedRegistryProto		*core.Registry

	// TODO: add more cache and fields
	// so that we can remove
	// cachedRegistryProto entirely

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
    cachedRegistryProtoCreated 	time.Time
    cachedRegistryProtoTtl		time.Time
}

func NewRegistry(path string) (r *Registry, error) {

	// Read the local registry
	in, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	registry := &core.Registry{}
	if err := proto.Unmarshal(in, registry); err != nil {
		return nil, err
	}
	r = &Registry{	path: path,
					cachedRegistryProto: registry,
					cachedFeatureServices: make(map[string][]*core.FeatureService),
					cachedEntities: make(map[string][]*core.Entity),
					cachedFeatureViews: make(map[string][]*core.FeatureView),
					cachedOnDemandFeatureViews: make(map[string]map[string]*core.OnDemandFeatureViews),
					cachedRequestFeatureViews: make(map[string]map[string]*core.RequestFeatureViews),
				}
	r.LoadEntities()
	r.LoadFeatureServices()
	r.LoadFeatureViews()
	r.LoadOndemandFeatureViews()
	r.LoadRequestFeatureViews()
	return r, nil
}

func (r *Registry) LoadEntities() {
	entities := r.GetEntities()
	for _, entity := range entities {
		r.cachedEntities[entity.GetSpec().GetProject()][entity.GetSpec().GetName()] = entity
	}
}

func (r *Registry) LoadFeatureServices() {
	featureServices := r.GetFeatureServices()
	for _, featureService := range featureServices {
		r.cachedFeatureServices[featureService.GetSpec().GetProject()][featureService.GetSpec().GetName()] = featureService
	}
}

func (r *Registry) LoadFeatureViews() {
	featureViews := r.GetFeatureViews()
	for _, featureView := range featureViews {
		r.cachedFeatureViews[featureView.GetSpec().GetProject()][featureView.GetSpec().GetName()] = featureView
	}
}

func (r *Registry) LoadOndemandFeatureViews() {
	ondemandFeatureViews := r.GetOnDemandFeatureViews()
	for _, ondemandFeatureView := range ondemandFeatureViews {
		r.cachedOndemandFeatureViews[ondemandFeatureView.GetSpec().GetProject()][ondemandFeatureView.GetSpec().GetName()] = ondemandFeatureView
	}
}

func (r *Registry) LoadRequestFeatureViews() {
	requestFeatureViews := r.GetRequestFeatureViews()
	for _, requestFeatureView := range requestFeatureViews {
		r.cachedRequestFeatureViews[requestFeatureView.GetSpec().GetProject()][requestFeatureView.GetSpec().GetName()] = requestFeatureView
	}
}

// TODO (Ly): Support and fix these functions if needed
func (r *Registry) listEntities(project string) ([]*core.Entity, error) {
	if entities, ok := r.cachedEntities[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		entityList := make([]*core.Entity, len(entities))
		index := 0
		for _, entity := range entityList {
			entityList[index] = entity
			index += 1
		}
		return entityList, nil
	}
}

func (r *Registry) listFeatureViews(project string) ([]*core.FeatureViews, error) {
	if featureViews, ok := r.cachedFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		featureViewList := make([]*core.FeatureView, len(featureViews))
		index := 0
		for _, featureView := range featureViewList {
			featureViewList[index] = featureView
			index += 1
		}
		return featureViewList, nil
	}
}

func (r *Registry) listFeatureServices(project string) ([]*core.FeatureService, error) {
	if featureServices, ok := r.cachedFeatureServices[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		featureServiceList := make([]*core.FeatureService, len(featureServices))
		index := 0
		for _, featureService := range featureServiceList {
			featureServiceList[index] = featureService
			index += 1
		}
		return featureServiceList, nil
	}
}

func (r *Registry) listOnDemandFeatureViews(project string) ([]*core.OnDemandFeatureView, error) {
	if ondemandFeatureViews, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		ondemandFeatureViewList := make([]*core.FeatureService, len(ondemandFeatureViews))
		index := 0
		for _, ondemandFeatureView := range ondemandFeatureViewList {
			ondemandFeatureViewList[index] = ondemandFeatureView
			index += 1
		}
		return ondemandFeatureViewList, nil
	}
}

func (r *Registry) listRequestFeatureViews(project string) ([]*core.RequestFeatureView, error) {
	if requestFeatureViews, ok := r.cachedRequestFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		requestFeatureViewList := make([]*core.FeatureService, len(requestFeatureViews))
		index := 0
		for _, requestFeatureView := range requestFeatureViewList {
			requestFeatureViewList[index] = requestFeatureView
			index += 1
		}
		return requestFeatureViewList, nil
	}
}

func (r *Registry) getEntity(project, entityName string) (*core.Entity, error) {
	if entities, ok := r.cachedFeatureServices[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		if enity, ok := entities[entityName]; !ok {
			return nil, errors.New(fmt.Sprintf("Entity %s not found inside project %s", entityName, project))
		}
		return enity, nil
	}
}

func (r *Registry) getFeatureView(project, featureViewName string) (*core.FeatureView, error) {
	if featureViews, ok := r.cachedFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		if featureView, ok := featureViews[featureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("FeatureView %s not found inside project %s", featureView, project))
		}
		return featureView, nil
	}
}

func (r *Registry) getFeatureService(project, featureServiceName string) (*core.FeatureService, error) {
	if featureServices, ok := r.cachedFeatureServices[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		if featureService ok := featureServices[featureServiceName]; !ok {
			return nil, errors.New(fmt.Sprintf("FeatureService %s not found inside project %s", featureServiceName, project))
		}
		return featureService, nil
	}
}

func (r *Registry) getOnDemandFeatureView(project, ondemandFeatureViewName string) (*core.OnDemandFeatureView, error) {
	if ondemandFeatureView, ok := r.cachedOnDemandFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		if ondemandFeatureView ok := ondemandFeatureViews[ondemandFeatureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("OnDemandFeatureView %s not found inside project %s", ondemandFeatureViewName, project))
		}
		return ondemandFeatureView, nil
	}
}

func (r *Registry) getRequestFeatureView(project, requestFeatureViewName string) (*core.RequestFeatureView, error) {
	if requestFeatureViews, ok := r.cachedRequestFeatureViews[project]; !ok {
		return nil, errors.New(fmt.Sprintf("Project %s not found", project))
	} else {
		if requestFeatureView ok := requestFeatureViews[requestFeatureViewName]; !ok {
			return nil, errors.New(fmt.Sprintf("RequestFeatureView %s not found inside project %s", requestFeatureViewName, project))
		}
		return requestFeatureView, nil
	}
}
