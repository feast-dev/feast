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

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Feast phases:
	ReadyPhase   = "Ready"
	PendingPhase = "Pending"
	FailedPhase  = "Failed"

	// Feast condition types:
	ClientReadyType        = "Client"
	OfflineStoreReadyType  = "OfflineStore"
	OnlineStoreReadyType   = "OnlineStore"
	RegistryReadyType      = "Registry"
	UIReadyType            = "UI"
	ReadyType              = "FeatureStore"
	AuthorizationReadyType = "Authorization"

	// Feast condition reasons:
	ReadyReason                  = "Ready"
	FailedReason                 = "FeatureStoreFailed"
	DeploymentNotAvailableReason = "DeploymentNotAvailable"
	OfflineStoreFailedReason     = "OfflineStoreDeploymentFailed"
	OnlineStoreFailedReason      = "OnlineStoreDeploymentFailed"
	RegistryFailedReason         = "RegistryDeploymentFailed"
	UIFailedReason               = "UIDeploymentFailed"
	ClientFailedReason           = "ClientDeploymentFailed"
	KubernetesAuthzFailedReason  = "KubernetesAuthorizationDeploymentFailed"

	// Feast condition messages:
	ReadyMessage                  = "FeatureStore installation complete"
	OfflineStoreReadyMessage      = "Offline Store installation complete"
	OnlineStoreReadyMessage       = "Online Store installation complete"
	RegistryReadyMessage          = "Registry installation complete"
	UIReadyMessage                = "UI installation complete"
	ClientReadyMessage            = "Client installation complete"
	KubernetesAuthzReadyMessage   = "Kubernetes authorization installation complete"
	DeploymentNotAvailableMessage = "Deployment is not available"

	// entity_key_serialization_version
	SerializationVersion = 3
)

// FeatureStoreSpec defines the desired state of FeatureStore
type FeatureStoreSpec struct {
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9][A-Za-z0-9_]*$"
	// FeastProject is the Feast project id. This can be any alphanumeric string with underscores, but it cannot start with an underscore. Required.
	FeastProject string                `json:"feastProject"`
	Services     *FeatureStoreServices `json:"services,omitempty"`
	AuthzConfig  *AuthzConfig          `json:"authz,omitempty"`
}

// FeatureStoreServices defines the desired feast services. An ephemeral registry is deployed by default.
type FeatureStoreServices struct {
	OfflineStore *OfflineStore `json:"offlineStore,omitempty"`
	OnlineStore  *OnlineStore  `json:"onlineStore,omitempty"`
	Registry     *Registry     `json:"registry,omitempty"`
	// Creates a UI server container
	UI                 *ServerConfigs             `json:"ui,omitempty"`
	DeploymentStrategy *appsv1.DeploymentStrategy `json:"deploymentStrategy,omitempty"`
	// Disable the 'feast repo initialization' initContainer
	DisableInitContainers bool `json:"disableInitContainers,omitempty"`
	// Volumes specifies the volumes to mount in the FeatureStore deployment. A corresponding `VolumeMount` should be added to whichever feast service(s) require access to said volume(s).
	Volumes []corev1.Volume `json:"volumes,omitempty"`
}

// OfflineStore configures the deployed offline store service
type OfflineStore struct {
	// Creates a remote offline server container
	Server      *ServerConfigs           `json:"server,omitempty"`
	Persistence *OfflineStorePersistence `json:"persistence,omitempty"`
}

// OfflineStorePersistence configures the persistence settings for the offline store service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type OfflineStorePersistence struct {
	FilePersistence *OfflineStoreFilePersistence    `json:"file,omitempty"`
	DBPersistence   *OfflineStoreDBStorePersistence `json:"store,omitempty"`
}

// OfflineStoreFilePersistence configures the file-based persistence for the offline store service
type OfflineStoreFilePersistence struct {
	// +kubebuilder:validation:Enum=file;dask;duckdb
	Type      string     `json:"type,omitempty"`
	PvcConfig *PvcConfig `json:"pvc,omitempty"`
}

var ValidOfflineStoreFilePersistenceTypes = []string{
	"dask",
	"duckdb",
	"file",
}

// OfflineStoreDBStorePersistence configures the DB store persistence for the offline store service
type OfflineStoreDBStorePersistence struct {
	// Type of the persistence type you want to use.
	// +kubebuilder:validation:Enum=snowflake.offline;bigquery;redshift;spark;postgres;trino;athena;mssql
	Type string `json:"type"`
	// Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// By default, the selected store "type" is used as the SecretKeyName
	SecretKeyName string `json:"secretKeyName,omitempty"`
}

var ValidOfflineStoreDBStorePersistenceTypes = []string{
	"snowflake.offline",
	"bigquery",
	"redshift",
	"spark",
	"postgres",
	"trino",
	"athena",
	"mssql",
}

// OnlineStore configures the deployed online store service
type OnlineStore struct {
	// Creates a feature server container
	Server      *ServerConfigs          `json:"server,omitempty"`
	Persistence *OnlineStorePersistence `json:"persistence,omitempty"`
}

// OnlineStorePersistence configures the persistence settings for the online store service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type OnlineStorePersistence struct {
	FilePersistence *OnlineStoreFilePersistence    `json:"file,omitempty"`
	DBPersistence   *OnlineStoreDBStorePersistence `json:"store,omitempty"`
}

// OnlineStoreFilePersistence configures the file-based persistence for the online store service
// +kubebuilder:validation:XValidation:rule="(!has(self.pvc) && has(self.path)) ? self.path.startsWith('/') : true",message="Ephemeral stores must have absolute paths."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !self.path.startsWith('/') : true",message="PVC path must be a file name only, with no slashes."
// +kubebuilder:validation:XValidation:rule="has(self.path) ? !(self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="Online store does not support S3 or GS buckets."
type OnlineStoreFilePersistence struct {
	Path      string     `json:"path,omitempty"`
	PvcConfig *PvcConfig `json:"pvc,omitempty"`
}

// OnlineStoreDBStorePersistence configures the DB store persistence for the online store service
type OnlineStoreDBStorePersistence struct {
	// Type of the persistence type you want to use.
	// +kubebuilder:validation:Enum=snowflake.online;redis;ikv;datastore;dynamodb;bigtable;postgres;cassandra;mysql;hazelcast;singlestore;hbase;elasticsearch;qdrant;couchbase;milvus
	Type string `json:"type"`
	// Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// By default, the selected store "type" is used as the SecretKeyName
	SecretKeyName string `json:"secretKeyName,omitempty"`
}

var ValidOnlineStoreDBStorePersistenceTypes = []string{
	"snowflake.online",
	"redis",
	"ikv",
	"datastore",
	"dynamodb",
	"bigtable",
	"postgres",
	"cassandra",
	"mysql",
	"hazelcast",
	"singlestore",
	"hbase",
	"elasticsearch",
	"qdrant",
	"couchbase",
	"milvus",
}

// LocalRegistryConfig configures the deployed registry service
type LocalRegistryConfig struct {
	// Creates a registry server container
	Server      *ServerConfigs       `json:"server,omitempty"`
	Persistence *RegistryPersistence `json:"persistence,omitempty"`
}

// RegistryPersistence configures the persistence settings for the registry service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type RegistryPersistence struct {
	FilePersistence *RegistryFilePersistence    `json:"file,omitempty"`
	DBPersistence   *RegistryDBStorePersistence `json:"store,omitempty"`
}

// RegistryFilePersistence configures the file-based persistence for the registry service
// +kubebuilder:validation:XValidation:rule="(!has(self.pvc) && has(self.path)) ? (self.path.startsWith('/') || self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="Registry files must use absolute paths or be S3 ('s3://') or GS ('gs://') object store URIs."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !self.path.startsWith('/') : true",message="PVC path must be a file name only, with no slashes."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !(self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="PVC persistence does not support S3 or GS object store URIs."
// +kubebuilder:validation:XValidation:rule="(has(self.s3_additional_kwargs) && has(self.path)) ? self.path.startsWith('s3://') : true",message="Additional S3 settings are available only for S3 object store URIs."
type RegistryFilePersistence struct {
	Path               string             `json:"path,omitempty"`
	PvcConfig          *PvcConfig         `json:"pvc,omitempty"`
	S3AdditionalKwargs *map[string]string `json:"s3_additional_kwargs,omitempty"`
}

// RegistryDBStorePersistence configures the DB store persistence for the registry service
type RegistryDBStorePersistence struct {
	// Type of the persistence type you want to use.
	// +kubebuilder:validation:Enum=sql;snowflake.registry
	Type string `json:"type"`
	// Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// By default, the selected store "type" is used as the SecretKeyName
	SecretKeyName string `json:"secretKeyName,omitempty"`
}

var ValidRegistryDBStorePersistenceTypes = []string{
	"sql",
	"snowflake.registry",
}

// PvcConfig defines the settings for a persistent file store based on PVCs.
// We can refer to an existing PVC using the `Ref` field, or create a new one using the `Create` field.
// +kubebuilder:validation:XValidation:rule="[has(self.ref), has(self.create)].exists_one(c, c)",message="One selection is required between ref and create."
// +kubebuilder:validation:XValidation:rule="self.mountPath.matches('^/[^:]*$')",message="Mount path must start with '/' and must not contain ':'"
type PvcConfig struct {
	// Reference to an existing field
	Ref *corev1.LocalObjectReference `json:"ref,omitempty"`
	// Settings for creating a new PVC
	Create *PvcCreate `json:"create,omitempty"`
	// MountPath within the container at which the volume should be mounted.
	// Must start by "/" and cannot contain ':'.
	MountPath string `json:"mountPath"`
}

// PvcCreate defines the immutable settings to create a new PVC mounted at the given path.
// The PVC name is the same as the associated deployment & feast service name.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="PvcCreate is immutable"
type PvcCreate struct {
	// AccessModes k8s persistent volume access modes. Defaults to ["ReadWriteOnce"].
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
	// StorageClassName is the name of an existing StorageClass to which this persistent volume belongs. Empty value
	// means that this volume does not belong to any StorageClass and the cluster default will be used.
	StorageClassName *string `json:"storageClassName,omitempty"`
	// Resources describes the storage resource requirements for a volume.
	// Default requested storage size depends on the associated service:
	// - 10Gi for offline store
	// - 5Gi for online store
	// - 5Gi for registry
	Resources corev1.VolumeResourceRequirements `json:"resources,omitempty"`
}

// Registry configures the registry service. One selection is required. Local is the default setting.
// +kubebuilder:validation:XValidation:rule="[has(self.local), has(self.remote)].exists_one(c, c)",message="One selection required."
type Registry struct {
	Local  *LocalRegistryConfig  `json:"local,omitempty"`
	Remote *RemoteRegistryConfig `json:"remote,omitempty"`
}

// RemoteRegistryConfig points to a remote feast registry server. When set, the operator will not deploy a registry for this FeatureStore CR.
// Instead, this FeatureStore CR's online/offline services will use a remote registry. One selection is required.
// +kubebuilder:validation:XValidation:rule="[has(self.hostname), has(self.feastRef)].exists_one(c, c)",message="One selection required."
type RemoteRegistryConfig struct {
	// Host address of the remote registry service - <domain>:<port>, e.g. `registry.<namespace>.svc.cluster.local:80`
	Hostname *string `json:"hostname,omitempty"`
	// Reference to an existing `FeatureStore` CR in the same k8s cluster.
	FeastRef *FeatureStoreRef          `json:"feastRef,omitempty"`
	TLS      *TlsRemoteRegistryConfigs `json:"tls,omitempty"`
}

// FeatureStoreRef defines which existing FeatureStore's registry should be used
type FeatureStoreRef struct {
	// Name of the FeatureStore
	Name string `json:"name"`
	// Namespace of the FeatureStore
	Namespace string `json:"namespace,omitempty"`
}

// ServerConfigs creates a server for the feast service, with specified container configurations.
type ServerConfigs struct {
	ContainerConfigs `json:",inline"`
	TLS              *TlsConfigs `json:"tls,omitempty"`
	// LogLevel sets the logging level for the server
	// Allowed values: "debug", "info", "warning", "error", "critical".
	// +kubebuilder:validation:Enum=debug;info;warning;error;critical
	LogLevel *string `json:"logLevel,omitempty"`
	// VolumeMounts defines the list of volumes that should be mounted into the feast container.
	// This allows attaching persistent storage, config files, secrets, or other resources
	// required by the Feast components. Ensure that each volume mount has a corresponding
	// volume definition in the Volumes field.
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
}

// ContainerConfigs k8s container settings for the server
type ContainerConfigs struct {
	DefaultCtrConfigs  `json:",inline"`
	OptionalCtrConfigs `json:",inline"`
}

// DefaultCtrConfigs k8s container settings that are applied by default
type DefaultCtrConfigs struct {
	Image *string `json:"image,omitempty"`
}

// OptionalCtrConfigs k8s container settings that are optional
type OptionalCtrConfigs struct {
	Env             *[]corev1.EnvVar             `json:"env,omitempty"`
	EnvFrom         *[]corev1.EnvFromSource      `json:"envFrom,omitempty"`
	ImagePullPolicy *corev1.PullPolicy           `json:"imagePullPolicy,omitempty"`
	Resources       *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// AuthzConfig defines the authorization settings for the deployed Feast services.
// +kubebuilder:validation:XValidation:rule="[has(self.kubernetes), has(self.oidc)].exists_one(c, c)",message="One selection required between kubernetes or oidc."
type AuthzConfig struct {
	KubernetesAuthz *KubernetesAuthz `json:"kubernetes,omitempty"`
	OidcAuthz       *OidcAuthz       `json:"oidc,omitempty"`
}

// KubernetesAuthz provides a way to define the authorization settings using Kubernetes RBAC resources.
// https://kubernetes.io/docs/reference/access-authn-authz/rbac/
type KubernetesAuthz struct {
	// The Kubernetes RBAC roles to be deployed in the same namespace of the FeatureStore.
	// Roles are managed by the operator and created with an empty list of rules.
	// See the Feast permission model at https://docs.feast.dev/getting-started/concepts/permission
	// The feature store admin is not obligated to manage roles using the Feast operator, roles can be managed independently.
	// This configuration option is only providing a way to automate this procedure.
	// Important note: the operator cannot ensure that these roles will match the ones used in the configured Feast permissions.
	Roles []string `json:"roles,omitempty"`
}

// OidcAuthz defines the authorization settings for deployments using an Open ID Connect identity provider.
// https://auth0.com/docs/authenticate/protocols/openid-connect-protocol
type OidcAuthz struct {
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// TlsConfigs configures server TLS for a feast service. in an openshift cluster, this is configured by default using service serving certificates.
// +kubebuilder:validation:XValidation:rule="(!has(self.disable) || !self.disable) ? has(self.secretRef) : true",message="`secretRef` required if `disable` is false."
type TlsConfigs struct {
	// references the local k8s secret where the TLS key and cert reside
	SecretRef      *corev1.LocalObjectReference `json:"secretRef,omitempty"`
	SecretKeyNames SecretKeyNames               `json:"secretKeyNames,omitempty"`
	// will disable TLS for the feast service. useful in an openshift cluster, for example, where TLS is configured by default
	Disable *bool `json:"disable,omitempty"`
}

// `secretRef` required if `disable` is false.
func (tls *TlsConfigs) IsTLS() bool {
	if tls != nil {
		if tls.Disable != nil && *tls.Disable {
			return false
		} else if tls.SecretRef == nil {
			return false
		}
		return true
	}
	return false
}

// TlsRemoteRegistryConfigs configures client TLS for a remote feast registry. in an openshift cluster, this is configured by default when the remote feast registry is using service serving certificates.
type TlsRemoteRegistryConfigs struct {
	// references the local k8s configmap where the TLS cert resides
	ConfigMapRef corev1.LocalObjectReference `json:"configMapRef"`
	// defines the configmap key name for the client TLS cert.
	CertName string `json:"certName"`
}

// SecretKeyNames defines the secret key names for the TLS key and cert.
type SecretKeyNames struct {
	// defaults to "tls.crt"
	TlsCrt string `json:"tlsCrt,omitempty"`
	// defaults to "tls.key"
	TlsKey string `json:"tlsKey,omitempty"`
}

// FeatureStoreStatus defines the observed state of FeatureStore
type FeatureStoreStatus struct {
	// Shows the currently applied feast configuration, including any pertinent defaults
	Applied FeatureStoreSpec `json:"applied,omitempty"`
	// ConfigMap in this namespace containing a client `feature_store.yaml` for this feast deployment
	ClientConfigMap  string             `json:"clientConfigMap,omitempty"`
	Conditions       []metav1.Condition `json:"conditions,omitempty"`
	FeastVersion     string             `json:"feastVersion,omitempty"`
	Phase            string             `json:"phase,omitempty"`
	ServiceHostnames ServiceHostnames   `json:"serviceHostnames,omitempty"`
}

// ServiceHostnames defines the service hostnames in the format of <domain>:<port>, e.g. example.svc.cluster.local:80
type ServiceHostnames struct {
	OfflineStore string `json:"offlineStore,omitempty"`
	OnlineStore  string `json:"onlineStore,omitempty"`
	Registry     string `json:"registry,omitempty"`
	UI           string `json:"ui,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=feast
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// FeatureStore is the Schema for the featurestores API
type FeatureStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FeatureStoreSpec   `json:"spec,omitempty"`
	Status FeatureStoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// FeatureStoreList contains a list of FeatureStore
type FeatureStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FeatureStore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FeatureStore{}, &FeatureStoreList{})
}
