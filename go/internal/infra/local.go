package infra

import (
	"github.com/feast-dev/feast/go/internal/config"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"io/ioutil"
	"os"
	"path/filepath"
)

type LocalRegistryStore struct {
	filePath string
}

func NewLocalRegistryStore(config *config.RegistryConfig, repoPath string) *LocalRegistryStore {
	lr := LocalRegistryStore{}
	registryPath := config.Path
	if filepath.IsAbs(registryPath) {
		lr.filePath = registryPath
	} else {
		lr.filePath = filepath.Join(repoPath, registryPath)
	}
	return &lr
}

func (r *LocalRegistryStore) GetRegistryProto() (*core.Registry, error) {
	registry := &core.Registry{}
	// Read the local registry
	in, err := ioutil.ReadFile(r.filePath)
	// Use an empty Registry Proto if file not exists or parse Registry Proto content from file
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(in, registry); err != nil {
		return nil, err
	}
	return registry, nil
}

func (r *LocalRegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return r.writeRegistry(rp)
}

func (r *LocalRegistryStore) Teardown() error {
	err := os.Remove(r.filePath)
	if err != nil {
		return err
	}
	return nil
}

func (r *LocalRegistryStore) writeRegistry(rp *core.Registry) error {
	rp.VersionId = uuid.New().String()
	rp.LastUpdated = timestamppb.Now()
	bytes, err := proto.Marshal(rp)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(r.filePath, bytes, 0644)
	if err != nil {
		return err
	}
	return nil
}
