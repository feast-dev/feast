package registry

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/protobuf/proto"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

// GCSObjectReader defines the interface for reading GCS objects to allow mocking in tests.
type GCSObjectReader interface {
	GetObject(ctx context.Context, bucket string, object string) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, bucket string, object string) error
}

// GCSClient implements GCSObjectReader using the real GCS SDK.
type GCSClient struct {
	client *storage.Client
}

func (g *GCSClient) GetObject(ctx context.Context, bucket string, object string) (io.ReadCloser, error) {
	return g.client.Bucket(bucket).Object(object).NewReader(ctx)
}

func (g *GCSClient) DeleteObject(ctx context.Context, bucket string, object string) error {
	return g.client.Bucket(bucket).Object(object).Delete(ctx)
}

// GCSRegistryStore is a GCS bucket-based implementation of the RegistryStore interface.
type GCSRegistryStore struct {
	registryPath string
	client       GCSObjectReader
}

// NewGCSRegistryStore creates a GCSRegistryStore with the given configuration.
func NewGCSRegistryStore(config *RegistryConfig, repoPath string) *GCSRegistryStore {
	var rs GCSRegistryStore
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		rs = GCSRegistryStore{
			registryPath: config.Path,
		}
	} else {
		rs = GCSRegistryStore{
			registryPath: config.Path,
			client:       &GCSClient{client: client},
		}
	}
	return &rs
}

// GetRegistryProto reads and parses the registry proto from the GCS bucket object.
func (g *GCSRegistryStore) GetRegistryProto() (*core.Registry, error) {
	bucket, object, err := g.parseGCSPath()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reader, err := g.client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	registry := &core.Registry{}
	if err := proto.Unmarshal(data, registry); err != nil {
		return nil, err
	}
	return registry, nil
}

func (g *GCSRegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return errors.New("not implemented in GCSRegistryStore")
}

func (g *GCSRegistryStore) Teardown() error {
	bucket, object, err := g.parseGCSPath()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return g.client.DeleteObject(ctx, bucket, object)
}

func (g *GCSRegistryStore) parseGCSPath() (string, string, error) {
	uri, err := url.Parse(g.registryPath)
	if err != nil {
		return "", "", errors.New("invalid GCS registry path format")
	}
	bucket := uri.Host
	object := strings.TrimPrefix(uri.Path, "/")
	return bucket, object, nil
}
