package registry

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

type HttpRegistryStore struct {
	project  string
	endpoint string
	clientId string
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
	log.Info().Msgf("Using Feature Registry: %s", config.Path)

	hrs := &HttpRegistryStore{
		project:  project,
		endpoint: config.Path,
		clientId: config.ClientId,
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
		return fmt.Errorf("HTTP Registry connectivity check failed with status code: %d", resp.StatusCode)
	}

	return nil
}

func (r *HttpRegistryStore) makeHttpRequest(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/x-protobuf")
	req.Header.Add("Client-Id", r.clientId)

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

	buffer, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if err := messageProcessor(buffer); err != nil {
		return err
	}

	return nil
}

func (r *HttpRegistryStore) getFeatureService(name string, allowCache bool) (*core.FeatureService, error) {
	url := fmt.Sprintf("%s/projects/%s/feature_services/%s?allow_cache=%t", r.endpoint, r.project, name, allowCache)
	featureService := &core.FeatureService{}
	err := r.loadProtobufMessages(url, func(data []byte) error {
		if err := proto.Unmarshal(data, featureService); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return featureService, nil
}

func (r *HttpRegistryStore) getEntity(name string, allowCache bool) (*core.Entity, error) {
	url := fmt.Sprintf("%s/projects/%s/entities/%s?allow_cache=%t", r.endpoint, r.project, name, allowCache)
	entity := &core.Entity{}
	err := r.loadProtobufMessages(url, func(data []byte) error {
		if err := proto.Unmarshal(data, entity); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return entity, nil
}

func (r *HttpRegistryStore) getFeatureView(name string, allowCache bool) (*core.FeatureView, error) {
	url := fmt.Sprintf("%s/projects/%s/feature_views/%s?allow_cache=%t", r.endpoint, r.project, name, allowCache)
	featureView := &core.FeatureView{}
	err := r.loadProtobufMessages(url, func(data []byte) error {
		if err := proto.Unmarshal(data, featureView); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return featureView, nil
}

func (r *HttpRegistryStore) getOnDemandFeatureView(name string, allowCache bool) (*core.OnDemandFeatureView, error) {
	url := fmt.Sprintf("%s/projects/%s/on_demand_feature_views/%s?allow_cache=%t", r.endpoint, r.project, name, allowCache)
	onDemandFeatureView := &core.OnDemandFeatureView{}
	err := r.loadProtobufMessages(url, func(data []byte) error {
		if err := proto.Unmarshal(data, onDemandFeatureView); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return onDemandFeatureView, nil
}

func (r *HttpRegistryStore) getSortedFeatureView(name string, allowCache bool) (*core.SortedFeatureView, error) {
	url := fmt.Sprintf("%s/projects/%s/sorted_feature_views/%s?allow_cache=%t", r.endpoint, r.project, name, allowCache)
	sortedFeatureView := &core.SortedFeatureView{}
	err := r.loadProtobufMessages(url, func(data []byte) error {
		if err := proto.Unmarshal(data, sortedFeatureView); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return sortedFeatureView, nil
}

func (r *HttpRegistryStore) GetRegistryProto() (*core.Registry, error) {
	registry := core.Registry{}
	return &registry, nil
}

func (r *HttpRegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return &NotImplementedError{FunctionName: "UpdateRegistryProto"}
}

func (r *HttpRegistryStore) Teardown() error {
	return &NotImplementedError{FunctionName: "Teardown"}
}

func (r *HttpRegistryStore) HasFallback() bool {
	return true
}
