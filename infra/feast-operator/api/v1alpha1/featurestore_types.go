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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Feast phases:
	ReadyPhase   = "Ready"
	PendingPhase = "Pending"
	FailedPhase  = "Failed"

	// Feast condition types:
	ClientReadyType       = "Client"
	OfflineStoreReadyType = "OfflineStore"
	OnlineStoreReadyType  = "OnlineStore"
	RegistryReadyType     = "Registry"
	ReadyType             = "FeatureStore"

	// Feast condition reasons:
	ReadyReason              = "Ready"
	FailedReason             = "FeatureStoreFailed"
	OfflineStoreFailedReason = "OfflineStoreDeploymentFailed"
	OnlineStoreFailedReason  = "OnlineStoreDeploymentFailed"
	RegistryFailedReason     = "RegistryDeploymentFailed"
	ClientFailedReason       = "ClientDeploymentFailed"

	// Feast condition messages:
	ReadyMessage             = "FeatureStore installation complete"
	OfflineStoreReadyMessage = "Offline Store installation complete"
	OnlineStoreReadyMessage  = "Online Store installation complete"
	RegistryReadyMessage     = "Registry installation complete"
	ClientReadyMessage       = "Client installation complete"

	// entity_key_serialization_version
	SerializationVersion = 3
)

// FeatureStoreSpec defines the desired state of FeatureStore
type FeatureStoreSpec struct {
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9][A-Za-z0-9_]*$"
	// FeastProject is the Feast project id. This can be any alphanumeric string with underscores, but it cannot start with an underscore. Required.
	FeastProject string                `json:"feastProject"`
	Services     *FeatureStoreServices `json:"services,omitempty"`
}

// FeatureStoreServices defines the desired feast service deployments. ephemeral registry is deployed by default.
type FeatureStoreServices struct {
	OfflineStore *OfflineStore `json:"offlineStore,omitempty"`
	OnlineStore  *OnlineStore  `json:"onlineStore,omitempty"`
	Registry     *Registry     `json:"registry,omitempty"`
}

// OfflineStore configures the deployed offline store service
type OfflineStore struct {
	ServiceConfigs `json:",inline"`
	// +optional
	Persistence *OfflineStorePersistence `json:"persistence,omitempty"`
}

// OfflineStorePersistence configures the persistence settings for the offline store service
type OfflineStorePersistence struct {
	// +optional
	FilePersistence *OfflineStoreFilePersistence `json:"file,omitempty"`
}

// OfflineStorePersistence configures the file-based persistence for the offline store service
type OfflineStoreFilePersistence struct {
	// +optional
	// +default:value=dask
	// +kubebuilder:validation:Enum=dask;duckdb
	Type string `json:"type,omitempty"`
}

// OnlineStore configures the deployed online store service
type OnlineStore struct {
	ServiceConfigs `json:",inline"`
	// +optional
	Persistence *OnlineStorePersistence `json:"persistence,omitempty"`
}

// OnlineStorePersistence configures the persistence settings for the online store service
type OnlineStorePersistence struct {
	// +optional
	FilePersistence *OnlineStoreFilePersistence `json:"file,omitempty"`
}

// OnlineStoreFilePersistence configures the file-based persistence for the offline store service
type OnlineStoreFilePersistence struct {
	// +optional
	Path string `json:"path,omitempty"`
}

// LocalRegistryConfig configures the deployed registry service
type LocalRegistryConfig struct {
	ServiceConfigs `json:",inline"`
	// +optional
	Persistence *RegistryPersistence `json:"persistence,omitempty"`
}

// RegistryPersistence configures the persistence settings for the registry service
type RegistryPersistence struct {
	// +optional
	FilePersistence *RegistryFilePersistence `json:"file,omitempty"`
}

// RegistryFilePersistence configures the file-based persistence for the registry service
type RegistryFilePersistence struct {
	// +optional
	Path string `json:"path,omitempty"`
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
	FeastRef *FeatureStoreRef `json:"feastRef,omitempty"`
}

// FeatureStoreRef defines which existing FeatureStore's registry should be used
type FeatureStoreRef struct {
	// Name of the FeatureStore
	Name string `json:"name"`
	// Namespace of the FeatureStore
	Namespace string `json:"namespace,omitempty"`
}

// ServiceConfigs k8s container settings
type ServiceConfigs struct {
	DefaultConfigs  `json:",inline"`
	OptionalConfigs `json:",inline"`
}

// DefaultConfigs k8s container settings that are applied by default
type DefaultConfigs struct {
	Image *string `json:"image,omitempty"`
}

// OptionalConfigs k8s container settings that are optional
type OptionalConfigs struct {
	Env             *[]corev1.EnvVar             `json:"env,omitempty"`
	ImagePullPolicy *corev1.PullPolicy           `json:"imagePullPolicy,omitempty"`
	Resources       *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// FeatureStoreStatus defines the observed state of FeatureStore
type FeatureStoreStatus struct {
	// Shows the currently applied feast configuration, including any pertinent defaults
	Applied FeatureStoreSpec `json:"applied,omitempty"`
	// ConfigMap in this namespace containing a client `feature_store.yaml` for this feast deployment
	ClientConfigMap string             `json:"clientConfigMap,omitempty"`
	Conditions      []metav1.Condition `json:"conditions,omitempty"`
	// Version of feast that's currently deployed
	FeastVersion     string           `json:"feastVersion,omitempty"`
	Phase            string           `json:"phase,omitempty"`
	ServiceHostnames ServiceHostnames `json:"serviceHostnames,omitempty"`
}

// ServiceHostnames defines the service hostnames in the format of <domain>:<port>, e.g. example.svc.cluster.local:80
type ServiceHostnames struct {
	OfflineStore string `json:"offlineStore,omitempty"`
	OnlineStore  string `json:"onlineStore,omitempty"`
	Registry     string `json:"registry,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=feast
//+kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// FeatureStore is the Schema for the featurestores API
type FeatureStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FeatureStoreSpec   `json:"spec,omitempty"`
	Status FeatureStoreStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// FeatureStoreList contains a list of FeatureStore
type FeatureStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FeatureStore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FeatureStore{}, &FeatureStoreList{})
}
