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

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	FeastPrefix                     = "feast-"
	FeatureStoreYamlEnvVar          = "FEATURE_STORE_YAML_BASE64"
	FeatureStoreYamlCmKey           = "feature_store.yaml"
	DefaultRegistryEphemeralPath    = "/tmp/registry.db"
	DefaultRegistryPvcPath          = "registry.db"
	DefaultOnlineStoreEphemeralPath = "/tmp/online_store.db"
	DefaultOnlineStorePvcPath       = "online_store.db"
	svcDomain                       = ".svc.cluster.local"
	HttpPort                        = 80

	DefaultOfflineStorageRequest  = "20Gi"
	DefaultOnlineStorageRequest   = "5Gi"
	DefaultRegistryStorageRequest = "5Gi"

	OfflineFeastType  FeastServiceType = "offline"
	OnlineFeastType   FeastServiceType = "online"
	RegistryFeastType FeastServiceType = "registry"
	ClientFeastType   FeastServiceType = "client"

	OfflineRemoteConfigType                 OfflineConfigType = "remote"
	OfflineFilePersistenceDaskConfigType    OfflineConfigType = "dask"
	OfflineFilePersistenceDuckDbConfigType  OfflineConfigType = "duckdb"
	OfflineDBPersistenceSnowflakeConfigType OfflineConfigType = "snowflake.offline"

	OnlineRemoteConfigType                 OnlineConfigType = "remote"
	OnlineSqliteConfigType                 OnlineConfigType = "sqlite"
	OnlineDBPersistenceSnowflakeConfigType OnlineConfigType = "snowflake.online"
	OnlineDBPersistenceCassandraConfigType OnlineConfigType = "cassandra"

	RegistryRemoteConfigType                 RegistryConfigType = "remote"
	RegistryFileConfigType                   RegistryConfigType = "file"
	RegistryDBPersistenceSnowflakeConfigType RegistryConfigType = "snowflake.registry"
	RegistryDBPersistenceSQLConfigType       RegistryConfigType = "sql"

	LocalProviderType FeastProviderType = "local"
)

var (
	DefaultImage        = "feastdev/feature-server:" + feastversion.FeastVersion
	DefaultReplicas     = int32(1)
	NameLabelKey        = feastdevv1alpha1.GroupVersion.Group + "/name"
	ServiceTypeLabelKey = feastdevv1alpha1.GroupVersion.Group + "/service-type"

	FeastServiceConstants = map[FeastServiceType]deploymentSettings{
		OfflineFeastType: {
			Command:    []string{"feast", "serve_offline", "-h", "0.0.0.0"},
			TargetPort: 8815,
		},
		OnlineFeastType: {
			Command:    []string{"feast", "serve", "-h", "0.0.0.0"},
			TargetPort: 6566,
		},
		RegistryFeastType: {
			Command:    []string{"feast", "serve_registry"},
			TargetPort: 6570,
		},
	}

	FeastServiceConditions = map[FeastServiceType]map[metav1.ConditionStatus]metav1.Condition{
		OfflineFeastType: {
			metav1.ConditionTrue: {
				Type:    feastdevv1alpha1.OfflineStoreReadyType,
				Status:  metav1.ConditionTrue,
				Reason:  feastdevv1alpha1.ReadyReason,
				Message: feastdevv1alpha1.OfflineStoreReadyMessage,
			},
			metav1.ConditionFalse: {
				Type:   feastdevv1alpha1.OfflineStoreReadyType,
				Status: metav1.ConditionFalse,
				Reason: feastdevv1alpha1.OfflineStoreFailedReason,
			},
		},
		OnlineFeastType: {
			metav1.ConditionTrue: {
				Type:    feastdevv1alpha1.OnlineStoreReadyType,
				Status:  metav1.ConditionTrue,
				Reason:  feastdevv1alpha1.ReadyReason,
				Message: feastdevv1alpha1.OnlineStoreReadyMessage,
			},
			metav1.ConditionFalse: {
				Type:   feastdevv1alpha1.OnlineStoreReadyType,
				Status: metav1.ConditionFalse,
				Reason: feastdevv1alpha1.OnlineStoreFailedReason,
			},
		},
		RegistryFeastType: {
			metav1.ConditionTrue: {
				Type:    feastdevv1alpha1.RegistryReadyType,
				Status:  metav1.ConditionTrue,
				Reason:  feastdevv1alpha1.ReadyReason,
				Message: feastdevv1alpha1.RegistryReadyMessage,
			},
			metav1.ConditionFalse: {
				Type:   feastdevv1alpha1.RegistryReadyType,
				Status: metav1.ConditionFalse,
				Reason: feastdevv1alpha1.RegistryFailedReason,
			},
		},
		ClientFeastType: {
			metav1.ConditionTrue: {
				Type:    feastdevv1alpha1.ClientReadyType,
				Status:  metav1.ConditionTrue,
				Reason:  feastdevv1alpha1.ReadyReason,
				Message: feastdevv1alpha1.ClientReadyMessage,
			},
			metav1.ConditionFalse: {
				Type:   feastdevv1alpha1.ClientReadyType,
				Status: metav1.ConditionFalse,
				Reason: feastdevv1alpha1.ClientFailedReason,
			},
		},
	}
)

// FeastServiceType is the type of feast service
type FeastServiceType string

// OfflineConfigType provider name or a class name that implements Offline Store
type OfflineConfigType string

// RegistryConfigType provider name or a class name that implements Registry
type RegistryConfigType string

// OnlineConfigType provider name or a class name that implements Online Store
type OnlineConfigType string

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
	Project                       string             `yaml:"project,omitempty"`
	Provider                      FeastProviderType  `yaml:"provider,omitempty"`
	OfflineStore                  OfflineStoreConfig `yaml:"offline_store,omitempty"`
	OnlineStore                   OnlineStoreConfig  `yaml:"online_store,omitempty"`
	Registry                      RegistryConfig     `yaml:"registry,omitempty"`
	EntityKeySerializationVersion int                `yaml:"entity_key_serialization_version,omitempty"`
}

// OfflineStoreConfig is the configuration that relates to reading from and writing to the Feast offline store.
type OfflineStoreConfig struct {
	Host         string                 `yaml:"host,omitempty"`
	Type         OfflineConfigType      `yaml:"type,omitempty"`
	Port         int                    `yaml:"port,omitempty"`
	DBParameters map[string]interface{} `yaml:",inline,omitempty"`
}

// OnlineStoreConfig is the configuration that relates to reading from and writing to the Feast online store.
type OnlineStoreConfig struct {
	Path         string                 `yaml:"path,omitempty"`
	Type         OnlineConfigType       `yaml:"type,omitempty"`
	DBParameters map[string]interface{} `yaml:",inline,omitempty"`
}

// RegistryConfig is the configuration that relates to reading from and writing to the Feast registry.
type RegistryConfig struct {
	Path               string                 `yaml:"path,omitempty"`
	RegistryType       RegistryConfigType     `yaml:"registry_type,omitempty"`
	S3AdditionalKwargs *map[string]string     `yaml:"s3_additional_kwargs,omitempty"`
	DBParameters       map[string]interface{} `yaml:",inline,omitempty"`
}

type deploymentSettings struct {
	Command    []string
	TargetPort int32
}
