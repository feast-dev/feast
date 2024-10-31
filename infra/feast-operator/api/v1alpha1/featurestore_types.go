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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Feast phases:
	ReadyPhase   = "Ready"
	PendingPhase = "Pending"
	FailedPhase  = "Failed"

	// Feast condition types:
	ClientReadyType   = "Client"
	RegistryReadyType = "Registry"
	ReadyType         = "FeatureStore"

	// Feast condition reasons:
	ReadyReason          = "Ready"
	FailedReason         = "FeatureStoreFailed"
	RegistryFailedReason = "RegistryDeploymentFailed"
	ClientFailedReason   = "ClientDeploymentFailed"

	// Feast condition messages:
	ReadyMessage         = "FeatureStore installation complete"
	RegistryReadyMessage = "Registry installation complete"
	ClientReadyMessage   = "Client installation complete"

	// entity_key_serialization_version
	SerializationVersion = 3
)

// FeatureStoreSpec defines the desired state of FeatureStore
type FeatureStoreSpec struct {
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9][A-Za-z0-9_]*$"
	// FeastProject is the Feast project id. This can be any alphanumeric string with underscores, but it cannot start with an underscore. Required.
	FeastProject string `json:"feastProject"`
	// +optional
	Services *Services `json:"services"`
}

type Services struct {
	// +optional
	OnlineStore *OnlineStore `json:"onlineStore,omitempty"`
	// +optional
	OfflineStore *OfflineStore `json:"offlineStore,omitempty"`
	// +optional
	Registry *Registry `json:"registry,omitempty"`
}
type OnlineStore struct {
	// +optional
	Persistence *OnlineStorePersistence `json:"persistence,omitempty"`
}
type OfflineStore struct {
	// +optional
	Persistence *OfflineStorePersistence `json:"persistence,omitempty"`
}
type Registry struct {
	// +optional
	Persistence *RegistryPersistence `json:"persistence,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!(has(self.file) && has(self.store))",message="One file or store persistence is allowed."
// +kubebuilder:validation:XValidation:rule="has(self.file) || has(self.store)", message="Either file or store persistence are required."
type OnlineStorePersistence struct {
	// +optional
	FilePersistence *OnlineStoreFilePersistence `json:"file,omitempty"`
	// +optional
	StorePersistence *OnlineStoreStorePersistence `json:"store,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!(has(self.file) && has(self.store))",message="One file or store persistence is allowed."
// +kubebuilder:validation:XValidation:rule="has(self.file) || has(self.store)", message="Either file or store persistence are required."
type OfflineStorePersistence struct {
	// +optional
	FilePersistence *OfflineStoreFilePersistence `json:"file,omitempty"`
	// +optional
	StorePersistence *OfflineStoreStorePersistence `json:"store,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!(has(self.file) && has(self.store))",message="One file or store persistence is allowed."
// +kubebuilder:validation:XValidation:rule="has(self.file) || has(self.store)", message="Either file or store persistence are required."
type RegistryPersistence struct {
	// +optional
	FilePersistence *RegistryFilePersistence `json:"file,omitempty"`
	// +optional
	StorePersistence *RegistryStorePersistence `json:"store,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.pvc) ? self.path.startsWith('/') : true",message="Ephemeral stores must have absolute paths."
// +kubebuilder:validation:XValidation:rule="has(self.pvc) ? !self.path.startsWith('/') : true",message="PVC path must be a file name only, with no slashes."
// +kubebuilder:validation:XValidation:rule="!self.path.startsWith('s3://') && !self.path.startsWith('gs://')",message="Online store does not support S3 or GS buckets."
type OnlineStoreFilePersistence struct {
	// +optional
	Path string `json:"path,omitempty"`
	// +optional
	PvcStore *PvcStore `json:"pvc,omitempty"`
}
type OfflineStoreFilePersistence struct {
	// +optional
	// +default:value=dask
	// +kubebuilder:validation:Enum=dask;duckdb
	Type string `json:"type,omitempty"`
	// +optional
	PvcStore *PvcStore `json:"pvc,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.pvc) ? (self.path.startsWith('/') || self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="Ephemeral stores must have absolute paths."
// +kubebuilder:validation:XValidation:rule="has(self.pvc) ? !self.path.startsWith('/') : true",message="PVC path must be a file name only, with no slashes."
// +kubebuilder:validation:XValidation:rule="has(self.pvc) ? !(self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="PVC persistence does not support S3 or GS buckets."
// +kubebuilder:validation:XValidation:rule="has(self.s3_additional_kwargs) ? self.path.startsWith('s3://') : true",message="Additional S3 settings are available only for S3 buckets."
type RegistryFilePersistence struct {
	// +optional
	Path string `json:"path,omitempty"`
	// +optional
	PvcStore *PvcStore `json:"pvc,omitempty"`
	// +optional
	S3AddtlKwargs *map[string]string `json:"s3_additional_kwargs,omitempty"`
}
type OnlineStoreStorePersistence struct {
	// +kubebuilder:validation:Enum=snowflake;redis;ikv;datastore;dynamodb;bigtable;postgres;cassandra;mysql;hazelcast;singlestore
	Type string `json:"type"`
	// +optional
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}
type OfflineStoreStorePersistence struct {
	// +kubebuilder:validation:Enum=snowflake;bigquery;redshift;spark;postgres;trino;mssql
	Type string `json:"type"`
	// +optional
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}
type RegistryStorePersistence struct {
	// +kubebuilder:validation:Enum=sql;snowflake
	Type string `json:"type"`
	// +optional
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

type PvcStore struct {
	// The name of the PersistenceVolumeClaim to be created, or reused it is already there
	Name string `json:"name,omitempty"`
	// Name of the StorageClass required by the claim.
	// Only for newly created PVC.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
	// Default value depend on Feast service.
	// Only for newly created PVC.
	// +optional
	Storage *resource.Quantity `json:"storage,omitempty"`
	// Default path depend on Feast service.
	// +optional
	MountPath *string `json:"mountPath,omitempty"`
}

// FeatureStoreStatus defines the observed state of FeatureStore
type FeatureStoreStatus struct {
	Applied         FeatureStoreSpec   `json:"applied,omitempty"`
	ClientConfigMap string             `json:"clientConfigMap,omitempty"`
	Conditions      []metav1.Condition `json:"conditions,omitempty"`
	FeastVersion    string             `json:"feastVersion,omitempty"`
	Phase           string             `json:"phase,omitempty"`
	ServiceUrls     ServiceUrls        `json:"serviceUrls,omitempty"`
}

// ServiceUrls
type ServiceUrls struct {
	Registry string `json:"registry,omitempty"`
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
