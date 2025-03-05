package registry

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestMakeHttpRequestReturnsResponseOnSuccess(t *testing.T) {
	// Create a new HttpRegistryStore
	store := &HttpRegistryStore{
		endpoint: "http://localhost",
		project:  "test_project",
	}

	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Call the method under test
	resp, err := store.makeHttpRequest(server.URL)

	// Assert that there was no error and the response status is OK
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestMakeHttpRequestReturnsErrorOnInvalidUrl(t *testing.T) {
	// Create a new HttpRegistryStore
	store := &HttpRegistryStore{
		endpoint: "http://localhost",
		project:  "test_project",
	}

	// Call the method under test with an invalid URL
	resp, err := store.makeHttpRequest("http://invalid_url")

	// Assert that there was an error
	assert.NotNil(t, err)
	assert.Nil(t, resp)
}

func TestMakeHttpRequestReturnsErrorOnNonOkStatus(t *testing.T) {
	// Create a new HttpRegistryStore
	store := &HttpRegistryStore{
		endpoint: "http://localhost",
		project:  "test_project",
	}

	// Create a mock HTTP server that returns a status code other than OK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// Call the method under test
	resp, err := store.makeHttpRequest(server.URL)

	// Assert that there was an error
	assert.NotNil(t, err)
	// If resp is not nil, then check the StatusCode
	if resp != nil {
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	}
}
func TestMakeHttpRequestIncludesClientId(t *testing.T) {
	// Create a new HttpRegistryStore
	store := &HttpRegistryStore{
		endpoint: "http://localhost",
		project:  "test_project",
		clientId: "test_client_id",
	}

	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Assert that the request includes the correct Client-Id header
		assert.Equal(t, "test_client_id", r.Header.Get("Client-Id"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Call the method under test
	_, err := store.makeHttpRequest(server.URL)

	// Assert that there was no error
	assert.Nil(t, err)
}
func TestLoadProtobufMessages(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write some data to the response
		w.Write([]byte("test_data"))
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  "test_project",
	}

	// Create a messageProcessor that checks the data it receives
	messageProcessor := func(data []byte) error {
		if string(data) != "test_data" {
			return errors.New("messageProcessor received unexpected data")
		}
		return nil
	}

	// Call the method under test
	err := store.loadProtobufMessages(server.URL+"/test_path", messageProcessor)

	// Assert that there was no error
	assert.Nil(t, err)
}

func TestLoadProtobufMessages_Error(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write some data to the response
		w.Write([]byte("test_data"))
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  "test_project",
	}

	// Create a messageProcessor that always returns an error
	messageProcessor := func(data []byte) error {
		return errors.New("test error")
	}

	// Call the method under test
	err := store.loadProtobufMessages(server.URL+"/test_path", messageProcessor)

	// Assert that the method returns the error from the messageProcessor
	assert.NotNil(t, err)
	assert.Equal(t, "test error", err.Error())
}

func TestLoadEntities(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy EntityList
		entityList := &core.EntityList{
			Entities: []*core.Entity{
				{Spec: &core.EntitySpecV2{Name: "test_entity"}},
			},
		}
		// Marshal it to protobuf
		data, err := proto.Marshal(entityList)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal EntityList")
		}
		// Write the protobuf data to the response
		w.Write(data)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  "test_project",
	}

	// Create a new empty Registry
	registry := &core.Registry{}

	// Call the method under test
	err := store.loadEntities(registry)

	// Assert that there was no error and that the registry now contains the entity
	assert.Nil(t, err)
	assert.Equal(t, 1, len(registry.Entities))
	assert.Equal(t, "test_entity", registry.Entities[0].Spec.Name)
}

func TestLoadDatasources(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy DataSourceList
		dataSourceList := &core.DataSourceList{
			Datasources: []*core.DataSource{
				{Name: "test_datasource"},
			},
		}
		// Marshal it to protobuf
		data, err := proto.Marshal(dataSourceList)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal DataSourceList")
		}
		// Write the protobuf data to the response
		w.Write(data)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  "test_project",
	}

	// Create a new empty Registry
	registry := &core.Registry{}

	// Call the method under test
	err := store.loadDatasources(registry)

	// Assert that there was no error and that the registry now contains the datasource
	assert.Nil(t, err)
	assert.Equal(t, 1, len(registry.DataSources))
	assert.Equal(t, "test_datasource", registry.DataSources[0].Name)
}

func TestLoadFeatureViews(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy FeatureViewList
		featureViewList := &core.FeatureViewList{
			Featureviews: []*core.FeatureView{
				{Spec: &core.FeatureViewSpec{Name: "test_feature_view"}},
			},
		}
		// Marshal it to protobuf
		data, err := proto.Marshal(featureViewList)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal FeatureViewList")
		}
		// Write the protobuf data to the response
		w.Write(data)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  "test_project",
	}

	// Create a new empty Registry
	registry := &core.Registry{}

	// Call the method under test
	err := store.loadFeatureViews(registry)

	// Assert that there was no error and that the registry now contains the feature view
	assert.Nil(t, err)
	assert.Equal(t, 1, len(registry.FeatureViews))
	assert.Equal(t, "test_feature_view", registry.FeatureViews[0].Spec.Name)
}

func TestLoadOnDemandFeatureViews(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy OnDemandFeatureViewList
		odFeatureViewList := &core.OnDemandFeatureViewList{
			Ondemandfeatureviews: []*core.OnDemandFeatureView{
				{Spec: &core.OnDemandFeatureViewSpec{Name: "test_view"}},
			},
		}
		// Marshal it to protobuf
		data, err := proto.Marshal(odFeatureViewList)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal OnDemandFeatureViewList")
		}
		// Write the protobuf data to the response
		w.Write(data)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  "test_project",
	}

	// Create a new empty Registry
	registry := &core.Registry{}

	// Call the method under test
	err := store.loadOnDemandFeatureViews(registry)

	// Assert that there was no error and that the registry now contains the on-demand feature view
	assert.Nil(t, err)
	assert.Equal(t, 1, len(registry.OnDemandFeatureViews))
	assert.Equal(t, "test_view", registry.OnDemandFeatureViews[0].Spec.Name)
}

func TestLoadFeatureServices(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy FeatureServiceList
		featureServiceList := &core.FeatureServiceList{
			Featureservices: []*core.FeatureService{
				{Spec: &core.FeatureServiceSpec{Name: "test_feature_service"}},
			},
		}
		// Marshal it to protobuf
		data, err := proto.Marshal(featureServiceList)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal FeatureServiceList")
		}
		// Write the protobuf data to the response
		w.Write(data)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  "test_project",
	}

	// Create a new empty Registry
	registry := &core.Registry{}

	// Call the method under test
	err := store.loadFeatureServices(registry)

	// Assert that there was no error and that the registry now contains the feature service
	assert.Nil(t, err)
	assert.Equal(t, 1, len(registry.FeatureServices))
	assert.Equal(t, "test_feature_service", registry.FeatureServices[0].Spec.Name)
}

func TestGetRegistryProto(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data []byte
		var err error

		switch r.URL.Path {
		case "/projects/test_project/entities":
			entityList := &core.EntityList{
				Entities: []*core.Entity{
					{Spec: &core.EntitySpecV2{Name: "test_entity"}},
				},
			}
			data, err = proto.Marshal(entityList)
		case "/projects/test_project/data_sources":
			dataSourceList := &core.DataSourceList{
				Datasources: []*core.DataSource{
					{Name: "test_datasource"},
				},
			}
			data, err = proto.Marshal(dataSourceList)
		case "/projects/test_project/feature_views":
			featureViewList := &core.FeatureViewList{
				Featureviews: []*core.FeatureView{
					{Spec: &core.FeatureViewSpec{Name: "test_feature_view"}},
				},
			}
			data, err = proto.Marshal(featureViewList)
		case "/projects/test_project/sorted_feature_views":
			sortedFeatureViewList := &core.SortedFeatureViewList{
				SortedFeatureViews: []*core.SortedFeatureView{
					{Spec: &core.SortedFeatureViewSpec{Name: "test_sorted_feature_view"}},
				},
			}
			data, err = proto.Marshal(sortedFeatureViewList)
		case "/projects/test_project/on_demand_feature_views":
			odFeatureViewList := &core.OnDemandFeatureViewList{
				Ondemandfeatureviews: []*core.OnDemandFeatureView{
					{Spec: &core.OnDemandFeatureViewSpec{Name: "test_view"}},
				},
			}
			data, err = proto.Marshal(odFeatureViewList)
		case "/projects/test_project/feature_services":
			featureServiceList := &core.FeatureServiceList{
				Featureservices: []*core.FeatureService{
					{Spec: &core.FeatureServiceSpec{Name: "test_feature_service"}},
				},
			}
			data, err = proto.Marshal(featureServiceList)
		default:
			t.Fatalf("Unexpected path: %s", r.URL.Path)
		}

		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal list")
		}

		// Write the protobuf data to the response
		w.Write(data)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: server.URL,
		project:  "test_project",
	}

	// Call the method under test
	registry, err := store.GetRegistryProto()

	// Assert that there was no error and that the registry now contains the expected data
	assert.Nil(t, err)
	assert.Equal(t, 1, len(registry.Entities))
	assert.Equal(t, "test_entity", registry.Entities[0].Spec.Name)
	assert.Equal(t, 1, len(registry.DataSources))
	assert.Equal(t, "test_datasource", registry.DataSources[0].Name)
	assert.Equal(t, 1, len(registry.FeatureViews))
	assert.Equal(t, "test_feature_view", registry.FeatureViews[0].Spec.Name)
	assert.Equal(t, 1, len(registry.SortedFeatureViews))
	assert.Equal(t, "test_sorted_feature_view", registry.SortedFeatureViews[0].Spec.Name)
	assert.Equal(t, 1, len(registry.OnDemandFeatureViews))
	assert.Equal(t, "test_view", registry.OnDemandFeatureViews[0].Spec.Name)
	assert.Equal(t, 1, len(registry.FeatureServices))
	assert.Equal(t, "test_feature_service", registry.FeatureServices[0].Spec.Name)
}

func TestUpdateRegistryProto(t *testing.T) {
	// Create a new HttpRegistryStore
	store := &HttpRegistryStore{
		endpoint: "http://localhost",
		project:  "test_project",
	}

	// Create a new empty Registry
	registry := &core.Registry{}

	// Call the method under test
	err := store.UpdateRegistryProto(registry)

	// Assert that the method returns a NotImplementedError
	assert.NotNil(t, err)
	assert.IsType(t, &NotImplementedError{}, err)
	assert.Equal(t, "UpdateRegistryProto", err.(*NotImplementedError).FunctionName)
}
func TestTeardown(t *testing.T) {
	// Create a new HttpRegistryStore
	store := &HttpRegistryStore{
		endpoint: "http://localhost",
		project:  "test_project",
	}

	// Call the method under test
	err := store.Teardown()

	// Assert that the method returns a NotImplementedError
	assert.NotNil(t, err)
	assert.IsType(t, &NotImplementedError{}, err)
	assert.Equal(t, "Teardown", err.(*NotImplementedError).FunctionName)
}
