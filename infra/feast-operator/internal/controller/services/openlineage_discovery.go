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

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// OpenLineageEndpointInfo describes one OpenLineage consumer endpoint.
type OpenLineageEndpointInfo struct {
	URL      string `json:"url"`
	Endpoint string `json:"endpoint"`
	TLS      bool   `json:"tls"`
}

// OpenLineageDiscoveryData is the structure stored in the discovery ConfigMap.
type OpenLineageDiscoveryData struct {
	Consumers map[string]OpenLineageEndpointInfo `json:"consumers"`
}

// hasOpenLineageConsumer returns true when the CR has an enabled OpenLineage consumer
// (either embedded or standalone).
func (feast *FeastServices) hasOpenLineageConsumer() bool {
	applied := feast.Handler.FeatureStore.Status.Applied
	return applied.OpenLineage != nil &&
		applied.OpenLineage.Consumer != nil &&
		applied.OpenLineage.Consumer.Enabled
}

// getOpenLineageConsumerURL builds the full service URL for this CR's consumer.
func (feast *FeastServices) getOpenLineageConsumerURL() (url string, isTLS bool) {
	cr := feast.Handler.FeatureStore

	if feast.isLineageServer() {
		svcName := GetFeastServiceName(cr, LineageFeastType)
		port := int32(HttpPort)
		scheme := HttpScheme
		isTLS = false

		if svr := cr.Status.Applied.OpenLineage.Consumer.LineageServer.Server; svr != nil && svr.TLS.IsTLS() {
			port = int32(HttpsPort)
			scheme = HttpsScheme
			isTLS = true
		}
		url = fmt.Sprintf("%s://%s.%s%s:%d", scheme, svcName, cr.Namespace, svcDomain, port)
		return url, isTLS
	}

	// Embedded consumer: prefer registry REST service (always running) over UI (optional).
	if feast.isRegistryRestEnabled() {
		svcName := feast.GetFeastRestServiceName(RegistryFeastType)
		port := int32(HttpPort)
		scheme := HttpScheme
		isTLS = false

		if cr.Status.Applied.Services != nil && cr.Status.Applied.Services.Registry != nil &&
			cr.Status.Applied.Services.Registry.Local != nil &&
			cr.Status.Applied.Services.Registry.Local.Server.TLS.IsTLS() {
			port = int32(HttpsPort)
			scheme = HttpsScheme
			isTLS = true
		}
		url = fmt.Sprintf("%s://%s.%s%s:%d", scheme, svcName, cr.Namespace, svcDomain, port)
		return url, isTLS
	}

	// Fallback to UI server if registry REST is not enabled.
	if feast.isUiServer() {
		svcName := GetFeastServiceName(cr, UIFeastType)
		port := int32(HttpPort)
		scheme := HttpScheme
		isTLS = false

		if cr.Status.Applied.Services != nil && cr.Status.Applied.Services.UI != nil &&
			cr.Status.Applied.Services.UI.TLS.IsTLS() {
			port = int32(HttpsPort)
			scheme = HttpsScheme
			isTLS = true
		}
		url = fmt.Sprintf("%s://%s.%s%s:%d", scheme, svcName, cr.Namespace, svcDomain, port)
		return url, isTLS
	}

	return "", false
}

// deployOpenLineageDiscovery creates and manages the OpenLineage discovery ConfigMap
// in the controller namespace so that external producers can discover the consumer.
func (feast *FeastServices) deployOpenLineageDiscovery() error {
	if !feast.hasOpenLineageConsumer() {
		return nil
	}

	if feast.isProtectedProject() {
		return nil
	}

	targetNamespace, err := feast.getNamespaceRegistryNamespace()
	if err != nil {
		logger := log.FromContext(feast.Handler.Context)
		logger.V(1).Info("Skipping OpenLineage discovery deployment: unable to determine target namespace", "error", err)
		return nil
	}

	if err := feast.createOpenLineageDiscoveryConfigMap(targetNamespace); err != nil {
		return err
	}
	if err := feast.createOpenLineageDiscoveryRoleBinding(targetNamespace); err != nil {
		return err
	}
	return nil
}

func (feast *FeastServices) createOpenLineageDiscoveryConfigMap(targetNamespace string) error {
	logger := log.FromContext(feast.Handler.Context)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      OpenLineageDiscoveryConfigMapName,
			Namespace: targetNamespace,
		},
	}
	cm.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))

	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, cm, controllerutil.MutateFn(func() error {
		return feast.setOpenLineageDiscoveryConfigMap(cm)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled OpenLineage discovery ConfigMap",
			"ConfigMap", cm.Name, "Namespace", cm.Namespace, "operation", op)
	}
	return nil
}

func (feast *FeastServices) setOpenLineageDiscoveryConfigMap(cm *corev1.ConfigMap) error {
	existing := &OpenLineageDiscoveryData{
		Consumers: make(map[string]OpenLineageEndpointInfo),
	}

	if cm.Data != nil && cm.Data[OpenLineageDiscoveryEndpointsKey] != "" {
		if err := json.Unmarshal([]byte(cm.Data[OpenLineageDiscoveryEndpointsKey]), existing); err != nil {
			existing = &OpenLineageDiscoveryData{
				Consumers: make(map[string]OpenLineageEndpointInfo),
			}
		}
	}

	cr := feast.Handler.FeatureStore
	key := cr.Namespace + "/" + cr.Name

	consumerURL, isTLS := feast.getOpenLineageConsumerURL()
	if consumerURL == "" {
		return nil
	}

	endpoint := "api/v1/lineage"
	existing.Consumers[key] = OpenLineageEndpointInfo{
		URL:      consumerURL,
		Endpoint: endpoint,
		TLS:      isTLS,
	}

	endpointsBytes, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("failed to marshal OpenLineage discovery data: %w", err)
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[OpenLineageDiscoveryEndpointsKey] = string(endpointsBytes)
	cm.Data[OpenLineageDiscoveryUrlKey] = consumerURL

	// Generate a ready-to-mount openlineage.yml for single-consumer convenience.
	yamlContent := fmt.Sprintf("transport:\n  type: http\n  url: %s\n  endpoint: %s\n", consumerURL, endpoint)
	cm.Data[OpenLineageDiscoveryYamlKey] = yamlContent

	cm.Labels = feast.getLabels()
	return nil
}

func (feast *FeastServices) createOpenLineageDiscoveryRoleBinding(targetNamespace string) error {
	logger := log.FromContext(feast.Handler.Context)

	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      OpenLineageDiscoveryConfigMapName + "-reader",
			Namespace: targetNamespace,
		},
	}
	roleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))

	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, roleBinding, controllerutil.MutateFn(func() error {
		return feast.setOpenLineageDiscoveryRoleBinding(roleBinding)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled OpenLineage discovery RoleBinding",
			"RoleBinding", roleBinding.Name, "Namespace", roleBinding.Namespace, "operation", op)
	}
	return nil
}

func (feast *FeastServices) setOpenLineageDiscoveryRoleBinding(rb *rbacv1.RoleBinding) error {
	roleName := OpenLineageDiscoveryConfigMapName + "-reader"

	desiredRules := []rbacv1.PolicyRule{
		{
			APIGroups:     []string{""},
			Resources:     []string{"configmaps"},
			ResourceNames: []string{OpenLineageDiscoveryConfigMapName},
			Verbs:         []string{"get", "list"},
		},
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: rb.Namespace,
		},
	}
	role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))

	if _, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, role, func() error {
		role.Labels = feast.getLabels()
		role.Rules = desiredRules
		return nil
	}); err != nil {
		return fmt.Errorf("failed to reconcile OpenLineage discovery Role: %w", err)
	}

	rb.Labels = feast.getLabels()
	rb.RoleRef = rbacv1.RoleRef{
		APIGroup: "rbac.authorization.k8s.io",
		Kind:     "Role",
		Name:     roleName,
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

// AddToOpenLineageDiscovery registers this Feast instance's consumer endpoint
// in the central OpenLineage discovery ConfigMap.
func (feast *FeastServices) AddToOpenLineageDiscovery() error {
	if !feast.hasOpenLineageConsumer() {
		return nil
	}
	if feast.isProtectedProject() {
		return nil
	}

	logger := log.FromContext(feast.Handler.Context)
	targetNamespace, err := feast.getNamespaceRegistryNamespace()
	if err != nil {
		logger.V(1).Info("Skipping OpenLineage discovery addition: unable to determine target namespace", "error", err)
		return nil
	}

	cm := &corev1.ConfigMap{}
	err = feast.Handler.Client.Get(feast.Handler.Context, types.NamespacedName{
		Name:      OpenLineageDiscoveryConfigMapName,
		Namespace: targetNamespace,
	}, cm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("OpenLineage discovery ConfigMap not found, creating it")
			return feast.deployOpenLineageDiscovery()
		}
		return fmt.Errorf("failed to get OpenLineage discovery ConfigMap: %w", err)
	}

	existing := &OpenLineageDiscoveryData{
		Consumers: make(map[string]OpenLineageEndpointInfo),
	}
	if cm.Data != nil && cm.Data[OpenLineageDiscoveryEndpointsKey] != "" {
		if err := json.Unmarshal([]byte(cm.Data[OpenLineageDiscoveryEndpointsKey]), existing); err != nil {
			existing = &OpenLineageDiscoveryData{
				Consumers: make(map[string]OpenLineageEndpointInfo),
			}
		}
	}

	cr := feast.Handler.FeatureStore
	key := cr.Namespace + "/" + cr.Name

	consumerURL, isTLS := feast.getOpenLineageConsumerURL()
	if consumerURL == "" {
		return nil
	}

	endpoint := "api/v1/lineage"
	existing.Consumers[key] = OpenLineageEndpointInfo{
		URL:      consumerURL,
		Endpoint: endpoint,
		TLS:      isTLS,
	}

	endpointsBytes, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("failed to marshal OpenLineage discovery data: %w", err)
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[OpenLineageDiscoveryEndpointsKey] = string(endpointsBytes)
	cm.Data[OpenLineageDiscoveryUrlKey] = consumerURL

	yamlContent := fmt.Sprintf("transport:\n  type: http\n  url: %s\n  endpoint: %s\n", consumerURL, endpoint)
	cm.Data[OpenLineageDiscoveryYamlKey] = yamlContent

	if err := feast.Handler.Client.Update(feast.Handler.Context, cm); err != nil {
		return fmt.Errorf("failed to update OpenLineage discovery ConfigMap: %w", err)
	}

	logger.Info("Successfully added to OpenLineage discovery",
		"key", key, "url", consumerURL, "targetNamespace", targetNamespace)
	return nil
}

// RemoveFromOpenLineageDiscovery removes this Feast instance from the central
// OpenLineage discovery ConfigMap.
func (feast *FeastServices) RemoveFromOpenLineageDiscovery() error {
	logger := log.FromContext(feast.Handler.Context)

	targetNamespace, err := feast.getNamespaceRegistryNamespace()
	if err != nil {
		logger.V(1).Info("Skipping OpenLineage discovery removal: unable to determine target namespace", "error", err)
		return nil
	}

	cm := &corev1.ConfigMap{}
	err = feast.Handler.Client.Get(feast.Handler.Context, client.ObjectKey{
		Name:      OpenLineageDiscoveryConfigMapName,
		Namespace: targetNamespace,
	}, cm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get OpenLineage discovery ConfigMap: %w", err)
	}

	existing := &OpenLineageDiscoveryData{
		Consumers: make(map[string]OpenLineageEndpointInfo),
	}
	if cm.Data != nil && cm.Data[OpenLineageDiscoveryEndpointsKey] != "" {
		if err := json.Unmarshal([]byte(cm.Data[OpenLineageDiscoveryEndpointsKey]), existing); err != nil {
			return nil
		}
	}

	cr := feast.Handler.FeatureStore
	key := cr.Namespace + "/" + cr.Name
	if _, found := existing.Consumers[key]; !found {
		logger.V(1).Info("FeatureStore not found in OpenLineage discovery, nothing to remove", "key", key)
		return nil
	}

	delete(existing.Consumers, key)

	endpointsBytes, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("failed to marshal updated OpenLineage discovery data: %w", err)
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[OpenLineageDiscoveryEndpointsKey] = string(endpointsBytes)

	// Update url and yml keys: if consumers remain pick the first; otherwise clear them.
	if len(existing.Consumers) > 0 {
		for _, info := range existing.Consumers {
			cm.Data[OpenLineageDiscoveryUrlKey] = info.URL
			cm.Data[OpenLineageDiscoveryYamlKey] = fmt.Sprintf(
				"transport:\n  type: http\n  url: %s\n  endpoint: %s\n", info.URL, info.Endpoint)
			break
		}
	} else {
		delete(cm.Data, OpenLineageDiscoveryUrlKey)
		delete(cm.Data, OpenLineageDiscoveryYamlKey)
	}

	if err := feast.Handler.Client.Update(feast.Handler.Context, cm); err != nil {
		return fmt.Errorf("failed to update OpenLineage discovery ConfigMap: %w", err)
	}

	logger.Info("Successfully removed from OpenLineage discovery",
		"key", key, "targetNamespace", targetNamespace)
	return nil
}
