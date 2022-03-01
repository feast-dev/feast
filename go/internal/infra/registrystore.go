package infra

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

type RegistryStore interface {
	GetRegistryProto() (*core.Registry, error)
	UpdateRegistryProto(*core.Registry) error
	Teardown() error
}
