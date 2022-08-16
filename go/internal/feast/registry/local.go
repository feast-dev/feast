package registry

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

// A FileRegistryStore is a file-based implementation of the RegistryStore interface.
type FileRegistryStore struct {
	filePath string
}

// NewFileRegistryStore creates a FileRegistryStore with the given configuration and infers
// the file path from the repo path and registry path.
func NewFileRegistryStore(config *RegistryConfig, repoPath string) *FileRegistryStore {
	lr := FileRegistryStore{}
	registryPath := config.Path
	if filepath.IsAbs(registryPath) {
		lr.filePath = registryPath
	} else {
		lr.filePath = filepath.Join(repoPath, registryPath)
	}
	return &lr
}

// GetRegistryProto reads and parses the registry proto from the file path.
func (r *FileRegistryStore) GetRegistryProto() (*core.Registry, error) {
	registry := &core.Registry{}
	in, err := ioutil.ReadFile(r.filePath)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(in, registry); err != nil {
		return nil, err
	}
	return registry, nil
}

func (r *FileRegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return r.writeRegistry(rp)
}

func (r *FileRegistryStore) Teardown() error {
	return os.Remove(r.filePath)
}

func (r *FileRegistryStore) writeRegistry(rp *core.Registry) error {
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
