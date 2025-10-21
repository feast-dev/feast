//go:build !integration

package registry

import (
	"context"
	goErrors "errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/feast-dev/feast/go/internal/feast/errors"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

var PROJECT = "test_project"

func mockRegistryWithResponse(responseProto proto.Message, ttlSeconds int) (*Registry, *httptest.Server) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write some data to the response if valid
		bytes, err := proto.Marshal(responseProto)
		if responseProto == nil || err != nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(bytes)
		}
	}))

	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  PROJECT,
	}

	registryTtl := time.Duration(ttlSeconds) * time.Second

	registry := &Registry{
		project:                    PROJECT,
		registryStore:              store,
		cachedFeatureServices:      newCacheMap[*model.FeatureService](registryTtl),
		cachedEntities:             newCacheMap[*model.Entity](registryTtl),
		cachedFeatureViews:         newCacheMap[*model.FeatureView](registryTtl),
		cachedSortedFeatureViews:   newCacheMap[*model.SortedFeatureView](registryTtl),
		cachedOnDemandFeatureViews: newCacheMap[*model.OnDemandFeatureView](registryTtl),
		cachedRegistryProtoTtl:     registryTtl,
	}

	return registry, server
}

func mockRegistryWithInternalServerError() (*Registry, *httptest.Server) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}))

	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  PROJECT,
	}

	registryTtl := 5 * time.Second

	registry := &Registry{
		project:                    PROJECT,
		registryStore:              store,
		cachedFeatureServices:      newCacheMap[*model.FeatureService](registryTtl),
		cachedEntities:             newCacheMap[*model.Entity](registryTtl),
		cachedFeatureViews:         newCacheMap[*model.FeatureView](registryTtl),
		cachedSortedFeatureViews:   newCacheMap[*model.SortedFeatureView](registryTtl),
		cachedOnDemandFeatureViews: newCacheMap[*model.OnDemandFeatureView](registryTtl),
		cachedRegistryProtoTtl:     registryTtl,
	}

	return registry, server
}

func TestRegistry_GetFeatureService_FromCache(t *testing.T) {
	// Set up a mock feature service
	featureService := &model.FeatureService{
		Name: "test_feature_service",
	}

	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Add the feature service to the cache
	registry.cachedFeatureServices.set(PROJECT, featureService.Name, featureService)

	// Call GetFeatureService
	result, err := registry.GetFeatureService(PROJECT, featureService.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil feature service")
	assert.Equal(t, featureService, result, "Expected the same feature service from cache")
}

func TestRegistry_GetFeatureService_FromStore(t *testing.T) {
	// Set up a mock feature service
	featureService := &core.FeatureService{
		Spec: &core.FeatureServiceSpec{
			Name: "test_feature_service",
		},
		Meta: &core.FeatureServiceMeta{},
	}

	registry, server := mockRegistryWithResponse(featureService, 5)
	defer server.Close()

	// Call GetFeatureService
	result, err := registry.GetFeatureService(PROJECT, featureService.Spec.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil feature service")
	assert.Equal(t, model.NewFeatureServiceFromProto(featureService), result, "Expected the same feature service from store")
}

func TestRegistry_GetFeatureService_Error(t *testing.T) {
	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Call GetFeatureService with an invalid feature service name
	result, err := registry.GetFeatureService(PROJECT, "invalid_feature_service")
	assert.Error(t, err, "Expected an error")
	assert.Equal(t, "rpc error: code = NotFound desc = no feature service invalid_feature_service found in project test_project", err.Error(), "Expected a specific error message")
	assert.Nil(t, result, "Expected a nil feature service")
}

func TestRegistry_GetFeatureService_InternalServerError(t *testing.T) {
	registry, server := mockRegistryWithInternalServerError()
	defer server.Close()

	result, err := registry.GetFeatureService(PROJECT, "any_feature_service")

	assert.Error(t, err, "Expected an internal server error")
	assert.Contains(t, err.Error(), "500 Internal Server Error")
	assert.Nil(t, result, "Expected a nil feature service")
}

func TestRegistry_GetEntity_FromCache(t *testing.T) {
	// Set up a mock entity
	entity := &model.Entity{
		Name: "test_entity",
	}

	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Add the entity to the cache
	registry.cachedEntities.set(PROJECT, entity.Name, entity)

	// Call GetEntity
	result, err := registry.GetEntity(PROJECT, entity.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil entity")
	assert.Equal(t, entity, result, "Expected the same entity from cache")
}

func TestRegistry_GetEntity_FromStore(t *testing.T) {
	// Set up a mock entity
	entity := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name: "test_entity",
		},
	}

	registry, server := mockRegistryWithResponse(entity, 5)
	defer server.Close()

	// Call GetEntity
	result, err := registry.GetEntity(PROJECT, entity.Spec.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil entity")
	assert.Equal(t, model.NewEntityFromProto(entity), result, "Expected the same entity from store")
}

func TestRegistry_GetEntity_Error(t *testing.T) {
	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Call GetEntity with an invalid entity name
	result, err := registry.GetEntity(PROJECT, "invalid_entity")
	assert.Error(t, err, "Expected an error")
	assert.Equal(t, "rpc error: code = NotFound desc = no entity invalid_entity found in project test_project", err.Error(), "Expected a specific error message")
	assert.Nil(t, result, "Expected a nil entity")
}

func TestRegistry_GetEntity_InternalServerError(t *testing.T) {
	registry, server := mockRegistryWithInternalServerError()
	defer server.Close()

	result, err := registry.GetEntity(PROJECT, "any_entity")

	assert.Error(t, err, "Expected an internal server error")
	assert.Contains(t, err.Error(), "500 Internal Server Error")
	assert.Nil(t, result, "Expected a nil entity")
}

func TestRegistry_GetFeatureView_FromCache(t *testing.T) {
	// Set up a mock feature view
	featureView := &model.FeatureView{
		Base: model.NewBaseFeatureView("test_feature_view", []*core.FeatureSpecV2{}),
	}

	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Add the feature view to the cache
	registry.cachedFeatureViews.set(PROJECT, featureView.Base.Name, featureView)

	// Call GetFeatureView
	result, err := registry.GetFeatureView(PROJECT, featureView.Base.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil feature view")
	assert.Equal(t, featureView, result, "Expected the same feature view from cache")
}

func TestRegistry_GetFeatureView_FromStore(t *testing.T) {
	// Set up a mock feature view
	featureView := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "test_feature_view",
		},
	}

	registry, server := mockRegistryWithResponse(featureView, 5)
	defer server.Close()

	// Call GetFeatureView
	result, err := registry.GetFeatureView(PROJECT, featureView.Spec.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil feature view")
	assert.Equal(t, model.NewFeatureViewFromProto(featureView), result, "Expected the same feature view from store")
}

func TestRegistry_GetFeatureView_Error(t *testing.T) {
	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Call GetFeatureView with an invalid feature view name
	result, err := registry.GetFeatureView(PROJECT, "invalid_feature_view")
	assert.Error(t, err, "Expected an error")
	assert.Equal(t, "rpc error: code = NotFound desc = no feature view invalid_feature_view found in project test_project", err.Error(), "Expected a specific error message")
	assert.Nil(t, result, "Expected a nil feature view")
}

func TestRegistry_GetFeatureView_InternalServerError(t *testing.T) {
	registry, server := mockRegistryWithInternalServerError()
	defer server.Close()

	result, err := registry.GetFeatureView(PROJECT, "any_feature_view")

	assert.Error(t, err, "Expected an internal server error")
	assert.Contains(t, err.Error(), "500 Internal Server Error")
	assert.Nil(t, result, "Expected a nil feature view")
}

func TestRegistry_GetSortedFeatureView_FromCache(t *testing.T) {
	// Set up a mock sorted feature view
	sortedFeatureView := &model.SortedFeatureView{
		FeatureView: &model.FeatureView{
			Base: model.NewBaseFeatureView("test_sorted_feature_view", []*core.FeatureSpecV2{}),
		},
	}

	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Add the sorted feature view to the cache
	registry.cachedSortedFeatureViews.set(PROJECT, sortedFeatureView.FeatureView.Base.Name, sortedFeatureView)

	// Call GetSortedFeatureView
	result, err := registry.GetSortedFeatureView(PROJECT, sortedFeatureView.FeatureView.Base.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil sorted feature view")
	assert.Equal(t, sortedFeatureView, result, "Expected the same sorted feature view from cache")
}

func TestRegistry_GetSortedFeatureView_FromStore(t *testing.T) {
	// Set up a mock sorted feature view
	sortedFeatureView := &core.SortedFeatureView{
		Spec: &core.SortedFeatureViewSpec{
			Name: "test_sorted_feature_view",
		},
	}

	registry, server := mockRegistryWithResponse(sortedFeatureView, 5)
	defer server.Close()

	// Call GetSortedFeatureView
	result, err := registry.GetSortedFeatureView(PROJECT, sortedFeatureView.Spec.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil sorted feature view")
	assert.Equal(t, model.NewSortedFeatureViewFromProto(sortedFeatureView), result, "Expected the same sorted feature view from store")
}

func TestRegistry_GetSortedFeatureView_Error(t *testing.T) {
	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Call GetSortedFeatureView with an invalid sorted feature view name
	result, err := registry.GetSortedFeatureView(PROJECT, "invalid_sorted_feature_view")
	assert.Error(t, err, "Expected an error")
	assert.Equal(t, "rpc error: code = NotFound desc = no sorted feature view invalid_sorted_feature_view found in project test_project", err.Error(), "Expected a specific error message")
	assert.Nil(t, result, "Expected a nil sorted feature view")
}

func TestRegistry_GetSortedFeatureView_InternalServerError(t *testing.T) {
	registry, server := mockRegistryWithInternalServerError()
	defer server.Close()

	result, err := registry.GetSortedFeatureView(PROJECT, "any_sorted_feature_view")

	assert.Error(t, err, "Expected an internal server error")
	assert.Contains(t, err.Error(), "500 Internal Server Error")
	assert.Nil(t, result, "Expected a nil feature view")
}

func TestRegistry_GetOnDemandFeatureView_FromCache(t *testing.T) {
	// Set up a mock on-demand feature view
	onDemandFeatureView := &model.OnDemandFeatureView{
		Base: model.NewBaseFeatureView("test_on_demand_feature_view", []*core.FeatureSpecV2{}),
	}

	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Add the on-demand feature view to the cache
	registry.cachedOnDemandFeatureViews.set(PROJECT, onDemandFeatureView.Base.Name, onDemandFeatureView)

	// Call GetOnDemandFeatureView
	result, err := registry.GetOnDemandFeatureView(PROJECT, onDemandFeatureView.Base.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil on-demand feature view")
	assert.Equal(t, onDemandFeatureView, result, "Expected the same on-demand feature view from cache")
}

func TestRegistry_GetOnDemandFeatureView_FromStore(t *testing.T) {
	// Set up a mock on-demand feature view
	onDemandFeatureView := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name: "test_on_demand_feature_view",
		},
	}

	registry, server := mockRegistryWithResponse(onDemandFeatureView, 5)
	defer server.Close()

	// Call GetOnDemandFeatureView
	result, err := registry.GetOnDemandFeatureView(PROJECT, onDemandFeatureView.Spec.Name)
	assert.NoError(t, err, "Expected no error")
	assert.NotNil(t, result, "Expected a non-nil on-demand feature view")
	assert.Equal(t, model.NewOnDemandFeatureViewFromProto(onDemandFeatureView), result, "Expected the same on-demand feature view from store")
}

func TestRegistry_GetOnDemandFeatureView_Error(t *testing.T) {
	registry, server := mockRegistryWithResponse(nil, 5)
	defer server.Close()

	// Call GetOnDemandFeatureView with an invalid on-demand feature view name
	result, err := registry.GetOnDemandFeatureView(PROJECT, "invalid_on_demand_feature_view")
	assert.Error(t, err, "Expected an error")
	assert.Equal(t, "rpc error: code = NotFound desc = no on demand feature view invalid_on_demand_feature_view found in project test_project", err.Error(), "Expected a specific error message")
	assert.Nil(t, result, "Expected a nil on-demand feature view")
}

func TestRegistry_GetOnDemandFeatureView_InternalServerError(t *testing.T) {
	registry, server := mockRegistryWithInternalServerError()
	defer server.Close()

	result, err := registry.GetOnDemandFeatureView(PROJECT, "any_on_demand_feature_view")

	assert.Error(t, err, "Expected an internal server error")
	assert.Contains(t, err.Error(), "500 Internal Server Error")
	assert.Nil(t, result, "Expected a nil feature view")
}

func compareFeaturesToFields(t *testing.T, expectedFeatures []*core.FeatureSpecV2, actualFields []*model.Field) {
	assert.Equal(t, len(expectedFeatures), len(actualFields), "Expected the same number of fields from cache")
	for i, feature := range expectedFeatures {
		field := model.NewFieldFromProto(feature)
		assert.Equal(t, field.Name, actualFields[i].Name, "Expected the same field name from cache")
		assert.Equal(t, field.Dtype, actualFields[i].Dtype, "Expected the same field value type from cache")
	}
}

func TestRefresh(t *testing.T) {
	// Set up a mock objects
	updateTime := time.Now().Unix()
	startTimestamp := &timestamppb.Timestamp{Seconds: updateTime, Nanos: 0}
	initialFeatures := []*core.FeatureSpecV2{{Name: "initial_feature", ValueType: types.ValueType_STRING}}

	featureService := &core.FeatureService{
		Spec: &core.FeatureServiceSpec{
			Name: "test_feature_service",
		},
		Meta: &core.FeatureServiceMeta{
			LastUpdatedTimestamp: startTimestamp,
		},
	}

	entity := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name: "test_entity",
		},
		Meta: &core.EntityMeta{
			LastUpdatedTimestamp: startTimestamp,
		},
	}

	featureView := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name:     "test_feature_view",
			Features: initialFeatures,
		},
		Meta: &core.FeatureViewMeta{
			LastUpdatedTimestamp: startTimestamp,
		},
	}

	sortedFeatureView := &core.SortedFeatureView{
		Spec: &core.SortedFeatureViewSpec{
			Name:     "test_sorted_feature_view",
			Features: initialFeatures,
		},
		Meta: &core.FeatureViewMeta{
			LastUpdatedTimestamp: startTimestamp,
		},
	}

	onDemandFeatureView := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name:     "test_on_demand_feature_view",
			Features: initialFeatures,
		},
		Meta: &core.OnDemandFeatureViewMeta{
			LastUpdatedTimestamp: startTimestamp,
		},
	}

	registry, server := mockRegistryWithResponse(featureService, 1)
	server.Close()

	registry.cachedFeatureServices.set(PROJECT, featureService.Spec.Name, model.NewFeatureServiceFromProto(featureService))
	registry.cachedEntities.set(PROJECT, entity.Spec.Name, model.NewEntityFromProto(entity))
	registry.cachedFeatureViews.set(PROJECT, featureView.Spec.Name, model.NewFeatureViewFromProto(featureView))
	registry.cachedSortedFeatureViews.set(PROJECT, sortedFeatureView.Spec.Name, model.NewSortedFeatureViewFromProto(sortedFeatureView))
	registry.cachedOnDemandFeatureViews.set(PROJECT, onDemandFeatureView.Spec.Name, model.NewOnDemandFeatureViewFromProto(onDemandFeatureView))

	// Update the objects to be refreshed
	updateTimestamp := &timestamppb.Timestamp{Seconds: updateTime + 4, Nanos: 0}
	updatedFeatures := append(initialFeatures, &core.FeatureSpecV2{Name: "updated_feature", ValueType: types.ValueType_STRING})

	featureService.Meta.LastUpdatedTimestamp = updateTimestamp
	featureView.Spec.Features = updatedFeatures
	sortedFeatureView.Spec.Features = updatedFeatures
	onDemandFeatureView.Spec.Features = updatedFeatures

	// Create a mock HTTP server
	fsUrl := fmt.Sprintf("/projects/%s/feature_services/%s", PROJECT, featureService.Spec.Name)
	entityUrl := fmt.Sprintf("/projects/%s/entities/%s", PROJECT, entity.Spec.Name)
	fvUrl := fmt.Sprintf("/projects/%s/feature_views/%s", PROJECT, featureView.Spec.Name)
	sortedFvUrl := fmt.Sprintf("/projects/%s/sorted_feature_views/%s", PROJECT, sortedFeatureView.Spec.Name)
	onDemandFvUrl := fmt.Sprintf("/projects/%s/on_demand_feature_views/%s", PROJECT, onDemandFeatureView.Spec.Name)
	callMap := map[string]int{fsUrl: 0, entityUrl: 0, fvUrl: 0, sortedFvUrl: 0, onDemandFvUrl: 0}

	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var bytes []byte
		var err error
		callMap[r.URL.Path]++
		switch r.URL.Path {
		case fsUrl:
			bytes, err = proto.Marshal(featureService)
		case entityUrl:
			err = fmt.Errorf("entity not found")
		case fvUrl:
			bytes, err = proto.Marshal(featureView)
		case sortedFvUrl:
			bytes, err = proto.Marshal(sortedFeatureView)
		case onDemandFvUrl:
			bytes, err = proto.Marshal(onDemandFeatureView)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
		// Write some data to the response if valid
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(bytes)
		}
	}))
	defer server.Close()

	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  PROJECT,
	}
	registry.registryStore = store

	fs, ok := registry.cachedFeatureServices.get(PROJECT, featureService.Spec.Name)
	assert.Truef(t, ok, "Expected to find feature service in cache")
	assert.Equal(t, startTimestamp.Seconds, fs.LastUpdatedTimestamp.Seconds, "Expected the same feature service from cache")
	assert.Equal(t, 0, callMap[fsUrl], "Expected no calls to feature service URL before refresh")

	e, ok := registry.cachedEntities.get(PROJECT, entity.Spec.Name)
	assert.Truef(t, ok, "Expected to find entity in cache")
	assert.Equal(t, model.NewEntityFromProto(entity), e, "Expected the same entity from cache")
	assert.Equal(t, 0, callMap[entityUrl], "Expected no calls to entity URL before refresh")

	fv, ok := registry.cachedFeatureViews.get(PROJECT, featureView.Spec.Name)
	assert.Truef(t, ok, "Expected to find feature view in cache")
	compareFeaturesToFields(t, initialFeatures, fv.Base.Features)
	assert.Equal(t, 0, callMap[fvUrl], "Expected no calls to feature view URL before refresh")

	sf, ok := registry.cachedSortedFeatureViews.get(PROJECT, sortedFeatureView.Spec.Name)
	assert.Truef(t, ok, "Expected to find sorted feature view in cache")
	compareFeaturesToFields(t, initialFeatures, sf.Base.Features)
	assert.Equal(t, 0, callMap[sortedFvUrl], "Expected no calls to sorted feature view URL before refresh")

	odfv, ok := registry.cachedOnDemandFeatureViews.get(PROJECT, onDemandFeatureView.Spec.Name)
	assert.Truef(t, ok, "Expected to find on-demand feature view in cache")
	compareFeaturesToFields(t, initialFeatures, odfv.Base.Features)
	assert.Equal(t, 0, callMap[onDemandFvUrl], "Expected no calls to on-demand feature view URL before refresh")

	// Call Refresh
	err := registry.refresh()
	assert.NoError(t, err, "Expected no error")
	assert.Equal(t, 0, callMap[fsUrl], "Expected no calls to feature service URL before ttl expires")
	assert.Equal(t, 0, callMap[entityUrl], "Expected no calls to entity URL before ttl expires")
	assert.Equal(t, 0, callMap[fvUrl], "Expected no calls to feature view URL before ttl expires")
	assert.Equal(t, 0, callMap[sortedFvUrl], "Expected no calls to sorted feature view URL before ttl expires")
	assert.Equal(t, 0, callMap[onDemandFvUrl], "Expected no calls to on-demand feature view URL before ttl expires")

	// Wait for the cache to expire
	time.Sleep(registry.cachedRegistryProtoTtl + time.Second)

	// Call Refresh again
	err = registry.refresh()
	assert.NoError(t, err, "Expected no error on second refresh")

	// Check if the cache is updated
	fs, ok = registry.cachedFeatureServices.get(PROJECT, featureService.Spec.Name)
	assert.Truef(t, ok, "Expected to find feature service in cache")
	assert.Equal(t, updateTimestamp.Seconds, fs.LastUpdatedTimestamp.Seconds, "Expected the updated feature service from cache")

	e, ok = registry.cachedEntities.get(PROJECT, entity.Spec.Name)
	assert.Falsef(t, ok, "Expected to not find entity in cache")
	assert.Nil(t, e, "Expected the entity to be nil after refresh")

	fv, ok = registry.cachedFeatureViews.get(PROJECT, featureView.Spec.Name)
	assert.Truef(t, ok, "Expected to find feature view in cache")
	compareFeaturesToFields(t, updatedFeatures, fv.Base.Features)

	sf, ok = registry.cachedSortedFeatureViews.get(PROJECT, sortedFeatureView.Spec.Name)
	assert.Truef(t, ok, "Expected to find sorted feature view in cache")
	compareFeaturesToFields(t, updatedFeatures, sf.Base.Features)

	odfv, ok = registry.cachedOnDemandFeatureViews.get(PROJECT, onDemandFeatureView.Spec.Name)
	assert.Truef(t, ok, "Expected to find on-demand feature view in cache")
	compareFeaturesToFields(t, updatedFeatures, odfv.Base.Features)

	// Verify no unexpected calls were made
	for path, count := range callMap {
		switch path {
		case fsUrl, entityUrl, fvUrl, sortedFvUrl, onDemandFvUrl:
			assert.Equal(t, 1, count, "Expected exactly one call to %s after refresh", path)
		default:
			assert.Equal(t, 0, count, "Expected no calls to %s", path)
		}
	}
}

func TestGetOnlineFeaturesS3Registry(t *testing.T) {
	mockS3Client := &MockS3Client{
		GetObjectFn: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader("mock data")),
			}, nil
		},
		DeleteObjectFn: func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			return &s3.DeleteObjectOutput{}, nil
		},
	}

	tests := []struct {
		name   string
		config *RepoConfig
	}{
		{
			name: "redis with simple features",
			config: &RepoConfig{
				Project: "feature_repo",
				Registry: map[string]interface{}{
					"path": "s3://test-bucket/path/to/registry.db",
				},
				Provider: "aws",
			},
		},
	}
	for _, test := range tests {
		registryConfig, err := test.config.GetRegistryConfig()
		if err != nil {
			t.Errorf("Error getting registry config. msg: %s", err.Error())
		}
		ttl := time.Duration(registryConfig.CacheTtlSeconds) * time.Second
		r := &Registry{
			project:                    test.config.Project,
			cachedFeatureServices:      newCacheMap[*model.FeatureService](ttl),
			cachedEntities:             newCacheMap[*model.Entity](ttl),
			cachedFeatureViews:         newCacheMap[*model.FeatureView](ttl),
			cachedSortedFeatureViews:   newCacheMap[*model.SortedFeatureView](ttl),
			cachedOnDemandFeatureViews: newCacheMap[*model.OnDemandFeatureView](ttl),
			cachedRegistryProtoTtl:     ttl,
		}
		_ = registryConfig.RegistryStoreType
		registryPath := registryConfig.Path
		uri, err := url.Parse(registryPath)
		if err != nil {
			t.Errorf("Error parsing registry path. msg: %s", err.Error())
		}
		if registryStoreType, ok := REGISTRY_STORE_CLASS_FOR_SCHEME[uri.Scheme]; ok {
			switch registryStoreType {
			case "S3RegistryStore":
				registryStore := &S3RegistryStore{
					filePath: registryConfig.Path,
					s3Client: mockS3Client,
				}
				r.registryStore = registryStore
				err := r.InitializeRegistry()
				if err != nil {
					t.Errorf("Error initializing registry. msg: %s. registry path=%q", err.Error(), registryPath)
				}
			default:
				t.Errorf("Only S3RegistryStore is supported on this testing. got=%s", registryStoreType)
			}
		}
	}
}

// MockS3Client is mock client for testing s3 registry store
type MockS3Client struct {
	GetObjectFn    func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObjectFn func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

func (m *MockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.GetObjectFn != nil {
		return m.GetObjectFn(ctx, params)
	}
	return nil, goErrors.New("not implemented")
}

func (m *MockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.DeleteObjectFn != nil {
		return m.DeleteObjectFn(ctx, params)
	}
	return nil, goErrors.New("not implemented")
}

func TestExpireCachedModels_DeletesCacheOnNotFoundError(t *testing.T) {
	type testModel struct{ Name string }
	cache := newCacheMap[*testModel](time.Millisecond)
	project := "test_project"
	modelName := "test_model"
	model := &testModel{Name: modelName}
	cache.set(project, modelName, model)

	time.Sleep(2 * time.Millisecond)

	getModel := func(name, proj string) (*testModel, error) {
		return nil, errors.GrpcNotFoundErrorf("not found")
	}

	cache.expireCachedModels(getModel)

	_, ok := cache.get(project, modelName)
	assert.False(t, ok, "Expected model to be deleted from cache on not found error")
}

func TestExpireCachedModels_DoesNotDeleteCacheOnOtherError(t *testing.T) {
	type testModel struct{ Name string }
	cache := newCacheMap[*testModel](time.Millisecond)
	project := "test_project"
	modelName := "test_model"
	model := &testModel{Name: modelName}
	cache.set(project, modelName, model)

	time.Sleep(2 * time.Millisecond)

	getModel := func(name, proj string) (*testModel, error) {
		return nil, assert.AnError
	}

	cache.expireCachedModels(getModel)

	_, ok := cache.get(project, modelName)
	assert.True(t, ok, "Expected model to remain in cache on non-not found error")
}
