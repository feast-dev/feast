package registry

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestCloudRegistryStores(t *testing.T) {
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

	mockGCSClient := &MockGCSClient{
		GetObjectFn: func(ctx context.Context, bucket string, object string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("mock data")), nil
		},
		DeleteObjectFn: func(ctx context.Context, bucket string, object string) error {
			return nil
		},
	}

	tests := []struct {
		name   string
		config *RepoConfig
	}{
		{
			name: "s3 registry store",
			config: &RepoConfig{
				Project: "feature_repo",
				Registry: map[string]any{
					"path": "s3://test-bucket/path/to/registry.db",
				},
				Provider: "aws",
			},
		},
		{
			name: "gcs registry store",
			config: &RepoConfig{
				Project: "feature_repo",
				Registry: map[string]any{
					"path": "gs://test-bucket/path/to/registry.db",
				},
				Provider: "gcp",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registryConfig, err := test.config.GetRegistryConfig()
			if err != nil {
				t.Errorf("Error getting registry config. msg: %s", err.Error())
			}
			r := &Registry{
				project:                test.config.Project,
				cachedRegistryProtoTtl: time.Duration(registryConfig.CacheTtlSeconds) * time.Second,
			}
			registryPath := registryConfig.Path
			uri, err := url.Parse(registryPath)
			if err != nil {
				t.Errorf("Error parsing registry path. msg: %s", err.Error())
			}
			if registryStoreType, ok := REGISTRY_STORE_CLASS_FOR_SCHEME[uri.Scheme]; ok {
				switch registryStoreType {
				case "S3RegistryStore":
					r.registryStore = &S3RegistryStore{
						filePath: registryConfig.Path,
						s3Client: mockS3Client,
					}
				case "GCSRegistryStore":
					r.registryStore = &GCSRegistryStore{
						registryPath: registryConfig.Path,
						client:       mockGCSClient,
					}
				default:
					t.Errorf("Unsupported registry store type: %s", registryStoreType)
					return
				}
				err := r.InitializeRegistry()
				if err != nil {
					t.Errorf("Error initializing registry. msg: %s. registry path=%q", err.Error(), registryPath)
				}
			}
		})
	}
}

// MockS3Client is mock client for testing S3 registry store
type MockS3Client struct {
	GetObjectFn    func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObjectFn func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

func (m *MockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.GetObjectFn != nil {
		return m.GetObjectFn(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m *MockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.DeleteObjectFn != nil {
		return m.DeleteObjectFn(ctx, params)
	}
	return nil, errors.New("not implemented")
}

// MockGCSClient is mock client for testing GCS registry store
type MockGCSClient struct {
	GetObjectFn    func(ctx context.Context, bucket string, object string) (io.ReadCloser, error)
	DeleteObjectFn func(ctx context.Context, bucket string, object string) error
}

func (m *MockGCSClient) GetObject(ctx context.Context, bucket string, object string) (io.ReadCloser, error) {
	if m.GetObjectFn != nil {
		return m.GetObjectFn(ctx, bucket, object)
	}
	return nil, errors.New("not implemented")
}

func (m *MockGCSClient) DeleteObject(ctx context.Context, bucket string, object string) error {
	if m.DeleteObjectFn != nil {
		return m.DeleteObjectFn(ctx, bucket, object)
	}
	return errors.New("not implemented")
}
