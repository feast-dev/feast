//go:build !integration

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

func TestGetEntity(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy EntityList
		entity := &core.Entity{Spec: &core.EntitySpecV2{Name: "test_entity"}}
		// Marshal it to protobuf
		data, err := proto.Marshal(entity)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal Entity")
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
	result, err := store.getEntity("test_entity", true)

	// Assert that there was no error and that the registry now contains the entity
	assert.Nil(t, err)
	assert.Equal(t, "test_entity", result.Spec.Name)
}

func TestGetFeatureView(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy FeatureViewList
		featureView := &core.FeatureView{Spec: &core.FeatureViewSpec{Name: "test_feature_view"}}
		// Marshal it to protobuf
		data, err := proto.Marshal(featureView)
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

	// Call the method under test
	result, err := store.getFeatureView("test_feature_view", true)

	// Assert that there was no error and that the registry now contains the feature view
	assert.Nil(t, err)
	assert.Equal(t, "test_feature_view", result.Spec.Name)
}

func TestGetOnDemandFeatureView(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy OnDemandFeatureViewList
		odFeatureView := &core.OnDemandFeatureView{Spec: &core.OnDemandFeatureViewSpec{Name: "test_view"}}
		// Marshal it to protobuf
		data, err := proto.Marshal(odFeatureView)
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

	// Call the method under test
	result, err := store.getOnDemandFeatureView("test_view", true)

	// Assert that there was no error and that the registry now contains the on-demand feature view
	assert.Nil(t, err)
	assert.Equal(t, "test_view", result.Spec.Name)
}

func TestGetSortedFeatureView(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy SortedFeatureViewList
		sortedFeatureView := &core.SortedFeatureView{Spec: &core.SortedFeatureViewSpec{Name: "test_sorted_view"}}
		// Marshal it to protobuf
		data, err := proto.Marshal(sortedFeatureView)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal SortedFeatureViewList")
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
	result, err := store.getSortedFeatureView("test_sorted_view", true)

	// Assert that there was no error and that the registry now contains the sorted feature view
	assert.Nil(t, err)
	assert.Equal(t, "test_sorted_view", result.Spec.Name)
}

func TestGetFeatureService(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a dummy FeatureServiceList
		featureService := &core.FeatureService{Spec: &core.FeatureServiceSpec{Name: "test_feature_service"}}
		// Marshal it to protobuf
		data, err := proto.Marshal(featureService)
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

	// Call the method under test
	result, err := store.getFeatureService("test_feature_service", true)

	// Assert that there was no error and that the registry now contains the feature service
	assert.Nil(t, err)
	assert.Equal(t, "test_feature_service", result.Spec.Name)
}

func TestGetRegistryProto(t *testing.T) {
	// Create a new HttpRegistryStore with the mock server's URL as the endpoint
	store := &HttpRegistryStore{
		endpoint: "http://localhost",
		project:  "test_project",
	}

	// Call the method under test
	registry, err := store.GetRegistryProto()

	// Assert that there was no error and that an empty registry is created
	assert.Nil(t, err)
	assert.IsType(t, &core.Registry{}, registry)
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
