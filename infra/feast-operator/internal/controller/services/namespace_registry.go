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
	"encoding/json"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// NamespaceRegistryData represents the structure of data stored in the namespace registry ConfigMap
type NamespaceRegistryData struct {
	Namespaces map[string][]string `json:"namespaces"`
}

// deployNamespaceRegistry creates and manages the namespace registry ConfigMap
func (feast *FeastServices) deployNamespaceRegistry() error {
	if err := feast.createNamespaceRegistryConfigMap(); err != nil {
		return err
	}
	if err := feast.createNamespaceRegistryRoleBinding(); err != nil {
		return err
	}
	return nil
}

// createNamespaceRegistryConfigMap creates the namespace registry ConfigMap
func (feast *FeastServices) createNamespaceRegistryConfigMap() error {
	logger := log.FromContext(feast.Handler.Context)

	// Determine the target namespace based on platform
	targetNamespace, err := feast.getNamespaceRegistryNamespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace registry namespace: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NamespaceRegistryConfigMapName,
			Namespace: targetNamespace,
		},
	}
	cm.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))

	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, cm, controllerutil.MutateFn(func() error {
		return feast.setNamespaceRegistryConfigMap(cm)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled namespace registry ConfigMap", "ConfigMap", cm.Name, "Namespace", cm.Namespace, "operation", op)
	}

	return nil
}

// setNamespaceRegistryConfigMap sets the data for the namespace registry ConfigMap
func (feast *FeastServices) setNamespaceRegistryConfigMap(cm *corev1.ConfigMap) error {
	// Get existing data or initialize empty structure
	existingData := &NamespaceRegistryData{
		Namespaces: make(map[string][]string),
	}

	if cm.Data != nil && cm.Data[NamespaceRegistryDataKey] != "" {
		if err := json.Unmarshal([]byte(cm.Data[NamespaceRegistryDataKey]), existingData); err != nil {
			// If unmarshaling fails, start with empty data
			existingData = &NamespaceRegistryData{
				Namespaces: make(map[string][]string),
			}
		}
	}

	// Add current feature store instance to the registry
	featureStoreNamespace := feast.Handler.FeatureStore.Namespace
	clientConfigName := feast.Handler.FeatureStore.Status.ClientConfigMap

	if clientConfigName != "" {
		if existingData.Namespaces[featureStoreNamespace] == nil {
			existingData.Namespaces[featureStoreNamespace] = []string{}
		}

		// Check if client config is already in the list
		found := false
		for _, config := range existingData.Namespaces[featureStoreNamespace] {
			if config == clientConfigName {
				found = true
				break
			}
		}

		if !found {
			existingData.Namespaces[featureStoreNamespace] = append(existingData.Namespaces[featureStoreNamespace], clientConfigName)
		}
	}

	// Marshal the data back to JSON
	dataBytes, err := json.Marshal(existingData)
	if err != nil {
		return fmt.Errorf("failed to marshal namespace registry data: %w", err)
	}

	// Set the ConfigMap data
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[NamespaceRegistryDataKey] = string(dataBytes)

	// Set labels
	cm.Labels = feast.getLabels()

	return nil
}

// createNamespaceRegistryRoleBinding creates a RoleBinding to allow system:authenticated to read the ConfigMap
func (feast *FeastServices) createNamespaceRegistryRoleBinding() error {
	logger := log.FromContext(feast.Handler.Context)

	targetNamespace, err := feast.getNamespaceRegistryNamespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace registry namespace: %w", err)
	}

	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NamespaceRegistryConfigMapName + "-reader",
			Namespace: targetNamespace,
		},
	}
	roleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))

	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, roleBinding, controllerutil.MutateFn(func() error {
		return feast.setNamespaceRegistryRoleBinding(roleBinding)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled namespace registry RoleBinding", "RoleBinding", roleBinding.Name, "Namespace", roleBinding.Namespace, "operation", op)
	}

	return nil
}

// setNamespaceRegistryRoleBinding sets the RoleBinding for namespace registry access
func (feast *FeastServices) setNamespaceRegistryRoleBinding(rb *rbacv1.RoleBinding) error {
	// Create a Role that allows reading the ConfigMap
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NamespaceRegistryConfigMapName + "-reader",
			Namespace: rb.Namespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups:     []string{""},
				Resources:     []string{"configmaps"},
				ResourceNames: []string{NamespaceRegistryConfigMapName},
				Verbs:         []string{"get", "list"},
			},
		},
	}

	// Create or update the Role
	if _, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, role, controllerutil.MutateFn(func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups:     []string{""},
				Resources:     []string{"configmaps"},
				ResourceNames: []string{NamespaceRegistryConfigMapName},
				Verbs:         []string{"get", "list"},
			},
		}
		return nil
	})); err != nil {
		return err
	}

	// Set the RoleBinding
	rb.RoleRef = rbacv1.RoleRef{
		APIGroup: "rbac.authorization.k8s.io",
		Kind:     "Role",
		Name:     role.Name,
	}

	rb.Subjects = []rbacv1.Subject{
		{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Group",
			Name:     "system:authenticated",
		},
	}

	return nil
}

// getNamespaceRegistryNamespace determines the target namespace for the namespace registry ConfigMap
func (feast *FeastServices) getNamespaceRegistryNamespace() (string, error) {
	// Check if we're running on OpenShift
	logger := log.FromContext(feast.Handler.Context)
	if isOpenShift {
		// TODO: Add support for reading DSCi configuration
		if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			if ns := string(data); len(ns) > 0 {
				logger.V(1).Info("Using OpenShift namespace", "namespace", ns)
				return ns, nil
			}
		}
		// This is what notebook controller team is doing, we are following them
		// They are not defaulting to redhat-ods-applications namespace
		return "", fmt.Errorf("unable to determine the namespace")
	}

	return DefaultKubernetesNamespace, nil
}

// AddToNamespaceRegistry adds a feature store instance to the namespace registry
func (feast *FeastServices) AddToNamespaceRegistry() error {
	logger := log.FromContext(feast.Handler.Context)
	targetNamespace, err := feast.getNamespaceRegistryNamespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace registry namespace: %w", err)
	}

	// Get the existing ConfigMap
	cm := &corev1.ConfigMap{}
	err = feast.Handler.Client.Get(feast.Handler.Context, types.NamespacedName{
		Name:      NamespaceRegistryConfigMapName,
		Namespace: targetNamespace,
	}, cm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("Namespace registry ConfigMap not found, nothing to add to")
			return nil
		}
		return fmt.Errorf("failed to get namespace registry ConfigMap: %w", err)
	}

	// Parse existing data
	var existingData NamespaceRegistryData
	if cm.Data != nil && cm.Data[NamespaceRegistryDataKey] != "" {
		err = json.Unmarshal([]byte(cm.Data[NamespaceRegistryDataKey]), &existingData)
		if err != nil {
			logger.V(1).Info("Failed to unmarshal namespace registry data, nothing to add to")
			return nil
		}
	}

	// Add current feature store instance to the registry
	featureStoreNamespace := feast.Handler.FeatureStore.Namespace
	clientConfigName := feast.Handler.FeatureStore.Status.ClientConfigMap

	if clientConfigName != "" {
		// Initialize namespace map if it doesn't exist
		if existingData.Namespaces == nil {
			existingData.Namespaces = make(map[string][]string)
		}
		if existingData.Namespaces[featureStoreNamespace] == nil {
			existingData.Namespaces[featureStoreNamespace] = []string{}
		}

		// Check if client config is already in the list
		found := false
		for _, config := range existingData.Namespaces[featureStoreNamespace] {
			if config == clientConfigName {
				found = true
				break
			}
		}

		// Add if not already present
		if !found {
			existingData.Namespaces[featureStoreNamespace] = append(existingData.Namespaces[featureStoreNamespace], clientConfigName)
		}
	}

	// Marshal the updated data back to JSON
	dataBytes, err := json.Marshal(existingData)
	if err != nil {
		return fmt.Errorf("failed to marshal updated namespace registry data: %w", err)
	}

	// Update the ConfigMap
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[NamespaceRegistryDataKey] = string(dataBytes)

	// Update the ConfigMap
	if err := feast.Handler.Client.Update(feast.Handler.Context, cm); err != nil {
		return fmt.Errorf("failed to update namespace registry ConfigMap: %w", err)
	}

	logger.Info("Successfully added feature store to namespace registry",
		"namespace", featureStoreNamespace,
		"clientConfig", clientConfigName,
		"targetNamespace", targetNamespace)

	return nil
}

// RemoveFromNamespaceRegistry removes a feature store instance from the namespace registry
func (feast *FeastServices) RemoveFromNamespaceRegistry() error {
	logger := log.FromContext(feast.Handler.Context)

	// Determine the target namespace based on platform
	targetNamespace, err := feast.getNamespaceRegistryNamespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace registry namespace: %w", err)
	}

	// Get the existing ConfigMap
	cm := &corev1.ConfigMap{}
	err = feast.Handler.Client.Get(feast.Handler.Context, client.ObjectKey{
		Name:      NamespaceRegistryConfigMapName,
		Namespace: targetNamespace,
	}, cm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// ConfigMap doesn't exist, nothing to clean up
			logger.V(1).Info("Namespace registry ConfigMap not found, nothing to clean up")
			return nil
		}
		return fmt.Errorf("failed to get namespace registry ConfigMap: %w", err)
	}

	// Get existing data
	existingData := &NamespaceRegistryData{
		Namespaces: make(map[string][]string),
	}

	if cm.Data != nil && cm.Data[NamespaceRegistryDataKey] != "" {
		if err := json.Unmarshal([]byte(cm.Data[NamespaceRegistryDataKey]), existingData); err != nil {
			// If unmarshaling fails, there's nothing to clean up
			logger.V(1).Info("Failed to unmarshal namespace registry data, nothing to clean up")
			return nil
		}
	}

	// Remove current feature store instance from the registry
	featureStoreNamespace := feast.Handler.FeatureStore.Namespace
	clientConfigName := feast.Handler.FeatureStore.Status.ClientConfigMap
	featureStoreName := feast.Handler.FeatureStore.Name

	// Generate expected client config name using the same logic as creation
	expectedClientConfigName := "feast-" + featureStoreName + "-client"

	logger.Info("Removing feature store from registry",
		"featureStoreName", featureStoreName,
		"featureStoreNamespace", featureStoreNamespace,
		"clientConfigName", clientConfigName,
		"expectedClientConfigName", expectedClientConfigName)

	if existingData.Namespaces[featureStoreNamespace] != nil {
		var updatedConfigs []string
		removed := false

		for _, config := range existingData.Namespaces[featureStoreNamespace] {
			// Remove if it matches the client config name or the expected pattern
			if config == clientConfigName || config == expectedClientConfigName {
				logger.Info("Removing config from registry", "config", config)
				removed = true
			} else {
				updatedConfigs = append(updatedConfigs, config)
			}
		}

		existingData.Namespaces[featureStoreNamespace] = updatedConfigs

		// If no configs left for this namespace, remove the namespace entry
		if len(existingData.Namespaces[featureStoreNamespace]) == 0 {
			delete(existingData.Namespaces, featureStoreNamespace)
			logger.Info("Removed empty namespace entry from registry", "namespace", featureStoreNamespace)
		}

		if !removed {
			logger.V(1).Info("No matching config found to remove from registry",
				"existingConfigs", existingData.Namespaces[featureStoreNamespace])
		}
	} else {
		logger.V(1).Info("Namespace not found in registry", "namespace", featureStoreNamespace)
	}

	// Marshal the updated data back to JSON
	dataBytes, err := json.Marshal(existingData)
	if err != nil {
		return fmt.Errorf("failed to marshal updated namespace registry data: %w", err)
	}

	// Update the ConfigMap
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[NamespaceRegistryDataKey] = string(dataBytes)

	// Update the ConfigMap
	if err := feast.Handler.Client.Update(feast.Handler.Context, cm); err != nil {
		return fmt.Errorf("failed to update namespace registry ConfigMap: %w", err)
	}

	logger.Info("Updated namespace registry ConfigMap",
		"namespace", featureStoreNamespace,
		"clientConfig", clientConfigName,
		"remainingConfigs", existingData.Namespaces[featureStoreNamespace],
		"targetNamespace", targetNamespace)

	logger.Info("Successfully removed feature store from namespace registry",
		"namespace", featureStoreNamespace,
		"clientConfig", clientConfigName,
		"targetNamespace", targetNamespace)

	return nil
}
