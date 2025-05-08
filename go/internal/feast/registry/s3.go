package registry

import (
	"context"
	"errors"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/feast-dev/feast/go/protos/feast/core"

	"google.golang.org/protobuf/proto"
)

// A S3RegistryStore is a S3 object storage-based implementation of the RegistryStore interface
type S3RegistryStore struct {
	filePath string
	s3Client *s3.Client
}

// NewS3RegistryStore creates a S3RegistryStore with the given configuration
func NewS3RegistryStore(config *RegistryConfig, repoPath string) *S3RegistryStore {
	var lr S3RegistryStore
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg, err := awsConfig.LoadDefaultConfig(ctx)
	if err != nil {
		lr = S3RegistryStore{
			filePath: config.Path,
		}
	} else {
		lr = S3RegistryStore{
			filePath: config.Path,
			s3Client: s3.NewFromConfig(cfg),
		}
	}
	return &lr
}

func (r *S3RegistryStore) GetRegistryProto() (*core.Registry, error) {
	bucket, key, err := r.parseS3Path()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := r.s3Client.GetObject(ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()

	data, err := ioutil.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}

	registry := &core.Registry{}
	if err := proto.Unmarshal(data, registry); err != nil {
		return nil, err
	}
	return registry, nil
}

func (r *S3RegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return errors.New("not implemented in S3RegistryStore")
}

func (r *S3RegistryStore) Teardown() error {
	bucket, key, err := r.parseS3Path()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = r.s3Client.DeleteObject(ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return err
	}
	return nil
}

func (r *S3RegistryStore) parseS3Path() (string, string, error) {
	path := strings.TrimPrefix(r.filePath, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid S3 file path format")
	}
	return parts[0], parts[1], nil
}
