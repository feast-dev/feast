/*
Copyright 2024 Feast Community.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package services

import (
	"context"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	FeastPrefix            = "feast-"
	FeatureStoreYamlEnvVar = "FEATURE_STORE_YAML_BASE64"
	RegistryPort           = int32(6570)
	LocalRegistryPath      = "/tmp/registry.db"

	RegistryFeastType FeastServiceType = "registry"
	ClientFeastType   FeastServiceType = "client"

	RegistryRemoteConfigType RegistryConfigType = "remote"
	RegistryFileConfigType   RegistryConfigType = "file"

	LocalProviderType FeastProviderType = "local"
)

// FeastServiceType is the type of feast service
type FeastServiceType string

// RegistryConfigType provider name or a class name that implements Registry
type RegistryConfigType string

// FeastProviderType defines an implementation of a feature store object
type FeastProviderType string

// FeastServices is an interface for configuring and deploying feast services
type FeastServices struct {
	client.Client
	Context      context.Context
	Scheme       *runtime.Scheme
	FeatureStore *feastdevv1alpha1.FeatureStore
}

// RepoConfig is the Repo config. Typically loaded from feature_store.yaml.
// https://rtd.feast.dev/en/stable/#feast.repo_config.RepoConfig
type RepoConfig struct {
	Project                       string            `yaml:"project,omitempty"`
	Provider                      FeastProviderType `yaml:"provider,omitempty"`
	Registry                      RegistryConfig    `yaml:"registry,omitempty"`
	EntityKeySerializationVersion int               `yaml:"entity_key_serialization_version,omitempty"`
}

// RegistryConfig is the configuration that relates to reading from and writing to the Feast registry.
type RegistryConfig struct {
	Path         string             `yaml:"path,omitempty"`
	RegistryType RegistryConfigType `yaml:"registry_type,omitempty"`
}
