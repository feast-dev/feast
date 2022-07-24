package registry

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

// A RegistryStore is a storage backend for the Feast registry.
type RegistryStore interface {
	GetRegistryProto() (*core.Registry, error)
	UpdateRegistryProto(*core.Registry) error
	Teardown() error
}
