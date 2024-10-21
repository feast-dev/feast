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
