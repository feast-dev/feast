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

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var mlflowGVK = schema.GroupVersionKind{
	Group:   "mlflow.opendatahub.io",
	Version: "v1",
	Kind:    "MLflow",
}

// MlflowDiscoveryResult holds the discovered MLflow URIs from the cluster CR.
type MlflowDiscoveryResult struct {
	// TrackingUri is the in-cluster URI for API calls (status.address.url).
	TrackingUri string
	// UiUrl is the external/browser-reachable URL for hyperlinks (status.url).
	// Empty when no external route is configured.
	UiUrl string
}

// DiscoverMlflow lists all MLflow CRs in the cluster and returns the URIs
// from the first one that is Available/Ready. The MLflow CRD enforces a
// singleton named "mlflow", but listing is used for forward-compatibility.
// Returns (zero-value, false) when MLflow is not installed, not available, or
// has no tracking URI. This function never returns an error — it is designed
// for best-effort discovery so that FeatureStore reconcile is not blocked.
func DiscoverMlflow(ctx context.Context, c client.Client) (MlflowDiscoveryResult, bool) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   mlflowGVK.Group,
		Version: mlflowGVK.Version,
		Kind:    mlflowGVK.Kind + "List",
	})

	if err := c.List(ctx, list); err != nil || len(list.Items) == 0 {
		return MlflowDiscoveryResult{}, false
	}

	for i := range list.Items {
		item := &list.Items[i]
		status, found, _ := unstructured.NestedMap(item.Object, "status")
		if !found || !isMlflowReady(status) {
			continue
		}

		result := extractMlflowURIs(status)
		if result.TrackingUri != "" {
			return result, true
		}
	}

	return MlflowDiscoveryResult{}, false
}

// extractMlflowURIs reads the tracking URI and UI URL from a MLflow CR status map.
func extractMlflowURIs(status map[string]interface{}) MlflowDiscoveryResult {
	result := MlflowDiscoveryResult{}

	// In-cluster address (HTTPS service URL) — used for API calls
	if addr, ok := status["address"].(map[string]interface{}); ok {
		if url, ok := addr["url"].(string); ok && url != "" {
			result.TrackingUri = url
		}
	}

	// External gateway URL — used for browser-reachable hyperlinks
	if url, ok := status["url"].(string); ok && url != "" {
		result.UiUrl = url
	}

	// If no in-cluster address, use external URL as tracking URI too
	if result.TrackingUri == "" {
		result.TrackingUri = result.UiUrl
	}

	return result
}

// isMlflowReady checks status.conditions for an Available=True or Ready=True
// condition. The RHOAI MLflow operator uses "Available" as its readiness
// signal; we also accept "Ready" for forward-compatibility with other operators.
// Returns false when conditions are absent or none indicate readiness.
func isMlflowReady(status map[string]interface{}) bool {
	conditions, ok := status["conditions"].([]interface{})
	if !ok || len(conditions) == 0 {
		return false
	}
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _ := cond["type"].(string)
		condStatus, _ := cond["status"].(string)
		if (condType == "Available" || condType == "Ready") && condStatus == "True" {
			return true
		}
	}
	return false
}

// legacyMlflowRoleBindingSuffix is the suffix used by earlier operator versions
// that created a RoleBinding for MLflow API access. That RoleBinding is no longer
// needed — authentication uses the pod SA token via MLFLOW_TRACKING_AUTH.
const legacyMlflowRoleBindingSuffix = "-mlflow-integration"

// cleanupLegacyMlflowRoleBinding deletes the RoleBinding created by older
// operator versions (if present). Safe no-op when the object does not exist.
func (feast *FeastServices) cleanupLegacyMlflowRoleBinding() error {
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetFeastName(feast.Handler.FeatureStore) + legacyMlflowRoleBindingSuffix,
			Namespace: feast.Handler.FeatureStore.Namespace,
		},
	}
	rb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))
	return feast.Handler.DeleteOwnedFeastObj(rb)
}
