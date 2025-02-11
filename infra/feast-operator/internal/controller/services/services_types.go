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
	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	handler "github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TmpFeatureStoreYamlEnvVar = "TMP_FEATURE_STORE_YAML_BASE64"
	feastServerImageVar       = "RELATED_IMAGE_FEATURE_SERVER"
	FeatureStoreYamlCmKey     = "feature_store.yaml"
	EphemeralPath             = "/feast-data"
	FeatureRepoDir            = "/feature_repo"
	DefaultRegistryPath       = "registry.db"
	DefaultOnlineStorePath    = "online_store.db"
	svcDomain                 = ".svc.cluster.local"

	HttpPort      = 80
	HttpsPort     = 443
	HttpScheme    = "http"
	HttpsScheme   = "https"
	tlsPath       = "/tls/"
	tlsNameSuffix = "-tls"

	DefaultOfflineStorageRequest  = "20Gi"
	DefaultOnlineStorageRequest   = "5Gi"
	DefaultRegistryStorageRequest = "5Gi"

	OfflineFeastType  FeastServiceType = "offline"
	OnlineFeastType   FeastServiceType = "online"
	RegistryFeastType FeastServiceType = "registry"
	UIFeastType       FeastServiceType = "ui"
	ClientFeastType   FeastServiceType = "client"
	ClientCaFeastType FeastServiceType = "client-ca"

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

	NoAuthAuthType     AuthzType = "no_auth"
	KubernetesAuthType AuthzType = "kubernetes"
	OidcAuthType       AuthzType = "oidc"

	OidcClientId         OidcPropertyType = "client_id"
	OidcAuthDiscoveryUrl OidcPropertyType = "auth_discovery_url"
	OidcClientSecret     OidcPropertyType = "client_secret"
	OidcUsername         OidcPropertyType = "username"
	OidcPassword         OidcPropertyType = "password"

	OidcMissingSecretError string = "missing OIDC secret: %s"
)

var (
	DefaultImage          = "feastdev/feature-server:" + feastversion.FeastVersion
	DefaultReplicas       = int32(1)
	DefaultPVCAccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	NameLabelKey          = feastdevv1alpha1.GroupVersion.Group + "/name"
	ServiceTypeLabelKey   = feastdevv1alpha1.GroupVersion.Group + "/service-type"

	FeastServiceConstants = map[FeastServiceType]deploymentSettings{
		OfflineFeastType: {
			Args:            []string{"serve_offline", "-h", "0.0.0.0"},
			TargetHttpPort:  8815,
			TargetHttpsPort: 8816,
		},
		OnlineFeastType: {
			Args:            []string{"serve", "-h", "0.0.0.0"},
			TargetHttpPort:  6566,
			TargetHttpsPort: 6567,
		},
		RegistryFeastType: {
			Args:            []string{"serve_registry"},
			TargetHttpPort:  6570,
			TargetHttpsPort: 6571,
		},
		UIFeastType: {
			Args:            []string{"ui", "-h", "0.0.0.0"},
			TargetHttpPort:  8888,
			TargetHttpsPort: 8443,
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
		UIFeastType: {
			metav1.ConditionTrue: {
				Type:    feastdevv1alpha1.UIReadyType,
				Status:  metav1.ConditionTrue,
				Reason:  feastdevv1alpha1.ReadyReason,
				Message: feastdevv1alpha1.UIReadyMessage,
			},
			metav1.ConditionFalse: {
				Type:   feastdevv1alpha1.UIReadyType,
				Status: metav1.ConditionFalse,
				Reason: feastdevv1alpha1.UIFailedReason,
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

	OidcServerProperties = []OidcPropertyType{OidcClientId, OidcAuthDiscoveryUrl}
	OidcClientProperties = []OidcPropertyType{OidcClientSecret, OidcUsername, OidcPassword}
)

// Feast server types: Reserved only for server types like Online, Offline, and Registry servers. Should not be used for client types like the UI, etc.
var feastServerTypes = []FeastServiceType{
	RegistryFeastType,
	OfflineFeastType,
	OnlineFeastType,
}

// AuthzType defines the authorization type
type AuthzType string

// OidcPropertyType defines the OIDC property type
type OidcPropertyType string

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
	Handler handler.FeastHandler
}

// RepoConfig is the Repo config. Typically loaded from feature_store.yaml.
// https://rtd.feast.dev/en/stable/#feast.repo_config.RepoConfig
type RepoConfig struct {
	Project                       string             `yaml:"project,omitempty"`
	Provider                      FeastProviderType  `yaml:"provider,omitempty"`
	OfflineStore                  OfflineStoreConfig `yaml:"offline_store,omitempty"`
	OnlineStore                   OnlineStoreConfig  `yaml:"online_store,omitempty"`
	Registry                      RegistryConfig     `yaml:"registry,omitempty"`
	AuthzConfig                   AuthzConfig        `yaml:"auth,omitempty"`
	EntityKeySerializationVersion int                `yaml:"entity_key_serialization_version,omitempty"`
}

// OfflineStoreConfig is the configuration that relates to reading from and writing to the Feast offline store.
type OfflineStoreConfig struct {
	Host         string                 `yaml:"host,omitempty"`
	Type         OfflineConfigType      `yaml:"type,omitempty"`
	Port         int                    `yaml:"port,omitempty"`
	Scheme       string                 `yaml:"scheme,omitempty"`
	Cert         string                 `yaml:"cert,omitempty"`
	DBParameters map[string]interface{} `yaml:",inline,omitempty"`
}

// OnlineStoreConfig is the configuration that relates to reading from and writing to the Feast online store.
type OnlineStoreConfig struct {
	Path         string                 `yaml:"path,omitempty"`
	Type         OnlineConfigType       `yaml:"type,omitempty"`
	Cert         string                 `yaml:"cert,omitempty"`
	DBParameters map[string]interface{} `yaml:",inline,omitempty"`
}

// RegistryConfig is the configuration that relates to reading from and writing to the Feast registry.
type RegistryConfig struct {
	Path               string                 `yaml:"path,omitempty"`
	RegistryType       RegistryConfigType     `yaml:"registry_type,omitempty"`
	Cert               string                 `yaml:"cert,omitempty"`
	S3AdditionalKwargs *map[string]string     `yaml:"s3_additional_kwargs,omitempty"`
	DBParameters       map[string]interface{} `yaml:",inline,omitempty"`
}

// AuthzConfig is the RBAC authorization configuration.
type AuthzConfig struct {
	Type           AuthzType              `yaml:"type,omitempty"`
	OidcParameters map[string]interface{} `yaml:",inline,omitempty"`
}

type deploymentSettings struct {
	Args            []string
	TargetHttpPort  int32
	TargetHttpsPort int32
}
