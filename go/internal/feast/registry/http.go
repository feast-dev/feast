package registry

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

const BUFFER_SIZE = 8192 // Adjust buffer size as needed

type HttpRegistryStore struct {
	project  string
	endpoint string
	client   http.Client
}

// NotImplementedError represents an error for a function that is not yet implemented.
type NotImplementedError struct {
	FunctionName string
}

// Error implements the error interface for NotImplementedError.
func (e *NotImplementedError) Error() string {
	return fmt.Sprintf("Function '%s' not implemented", e.FunctionName)
}

func NewHttpRegistryStore(config *RegistryConfig, project string) (*HttpRegistryStore, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		IdleConnTimeout: 60 * time.Second,
	}

	hrs := &HttpRegistryStore{
		project:  project,
		endpoint: config.Path,
		client: http.Client{
			Transport: tr,
			Timeout:   5 * time.Second,
		},
	}

	if err := hrs.TestConnectivity(); err != nil {
		return nil, err
	}

	return hrs, nil
}

func (hrs *HttpRegistryStore) TestConnectivity() error {
	resp, err := hrs.client.Get(hrs.endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP Registry connecitiy check failed with status code: %d", resp.StatusCode)
	}

	return nil
}

func (r *HttpRegistryStore) makeHttpRequest(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/x-protobuf")

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP Error: %s", resp.Status)
	}

	return resp, nil
}

func (r *HttpRegistryStore) loadProtobufMessages(url string, messageProcessor func([]byte) error) error {
	resp, err := r.makeHttpRequest(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	buffer := make([]byte, BUFFER_SIZE)

	for {
		n, err := resp.Body.Read(buffer)
		if err != nil && err != io.EOF {
			return err
		}

		if n > 0 {
			if err := messageProcessor(buffer[:n]); err != nil {
				return err
			}
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

func (r *HttpRegistryStore) loadEntities(registry *core.Registry) error {
	url := fmt.Sprintf("%s/projects/%s/entities", r.endpoint, r.project)
	return r.loadProtobufMessages(url, func(data []byte) error {
		entity := &core.Entity{}
		if err := proto.Unmarshal(data, entity); err != nil {
			return err
		}
		registry.Entities = append(registry.Entities, entity)
		return nil
	})
}

func (r *HttpRegistryStore) loadDatasources(registry *core.Registry) error {
	url := fmt.Sprintf("%s/projects/%s/data_sources", r.endpoint, r.project)
	return r.loadProtobufMessages(url, func(data []byte) error {
		data_source := &core.DataSource{}
		if err := proto.Unmarshal(data, data_source); err != nil {
			return err
		}
		registry.DataSources = append(registry.DataSources, data_source)
		return nil
	})
}

func (r *HttpRegistryStore) loadFeatureViews(registry *core.Registry) error {
	url := fmt.Sprintf("%s/projects/%s/feature_views", r.endpoint, r.project)
	return r.loadProtobufMessages(url, func(data []byte) error {
		feature_view := &core.FeatureView{}
		if err := proto.Unmarshal(data, feature_view); err != nil {
			return err
		}
		registry.FeatureViews = append(registry.FeatureViews, feature_view)
		return nil
	})
}

func (r *HttpRegistryStore) loadOnDemandFeatureViews(registry *core.Registry) error {
	url := fmt.Sprintf("%s/projects/%s/on_demand_feature_views", r.endpoint, r.project)
	return r.loadProtobufMessages(url, func(data []byte) error {
		od_feature_view := &core.OnDemandFeatureView{}
		if err := proto.Unmarshal(data, od_feature_view); err != nil {
			return err
		}
		registry.OnDemandFeatureViews = append(registry.OnDemandFeatureViews, od_feature_view)
		return nil
	})
}

func (r *HttpRegistryStore) loadFeatureServices(registry *core.Registry) error {
	url := fmt.Sprintf("%s/projects/%s/feature_services", r.endpoint, r.project)
	return r.loadProtobufMessages(url, func(data []byte) error {
		feature_service := &core.FeatureService{}
		if err := proto.Unmarshal(data, feature_service); err != nil {
			return err
		}
		registry.FeatureServices = append(registry.FeatureServices, feature_service)
		return nil
	})
}

func (r *HttpRegistryStore) GetRegistryProto() (*core.Registry, error) {

	registry := core.Registry{}

	if err := r.loadEntities(&registry); err != nil {
		return nil, err
	}

	if err := r.loadDatasources(&registry); err != nil {
		return nil, err
	}

	if err := r.loadFeatureViews(&registry); err != nil {
		return nil, err
	}

	if err := r.loadOnDemandFeatureViews(&registry); err != nil {
		return nil, err
	}

	if err := r.loadFeatureServices(&registry); err != nil {
		return nil, err
	}

	return &registry, nil
}

func (r *HttpRegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return &NotImplementedError{FunctionName: "UpdateRegistryProto"}
}

func (r *HttpRegistryStore) Teardown() error {
	return &NotImplementedError{FunctionName: "Teardown"}
}