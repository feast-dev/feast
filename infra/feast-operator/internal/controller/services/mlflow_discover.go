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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var mlflowGVK = schema.GroupVersionKind{
	Group:   "mlflow.opendatahub.io",
	Version: "v1",
	Kind:    "MLflow",
}

// DiscoverMlflowTrackingUri attempts to find the cluster-scoped MLflow CR
// (singleton named "mlflow") and returns its in-cluster tracking URI.
// Returns ("", false) when MLflow is not installed, not ready, or not available.
// This function never returns an error — it is designed for best-effort discovery
// so that the FeatureStore reconcile is not blocked by MLflow absence.
func DiscoverMlflowTrackingUri(ctx context.Context, c client.Client) (string, bool) {
	mlflow := &unstructured.Unstructured{}
	mlflow.SetGroupVersionKind(mlflowGVK)

	if err := c.Get(ctx, client.ObjectKey{Name: "mlflow"}, mlflow); err != nil {
		return "", false
	}

	status, found, _ := unstructured.NestedMap(mlflow.Object, "status")
	if !found {
		return "", false
	}

	if !isMlflowReady(status) {
		return "", false
	}

	// Prefer in-cluster address (HTTPS service URL)
	if addr, ok := status["address"].(map[string]interface{}); ok {
		if url, ok := addr["url"].(string); ok && url != "" {
			return url, true
		}
	}

	// Fallback to gateway URL (external, browser-reachable)
	if url, ok := status["url"].(string); ok && url != "" {
		return url, true
	}

	return "", false
}

// isMlflowReady checks status.conditions for a Ready=True condition.
// Returns true when conditions are absent (best-effort: older MLflow CRs may
// not report conditions, so we fall through to URL-based discovery).
func isMlflowReady(status map[string]interface{}) bool {
	conditions, ok := status["conditions"].([]interface{})
	if !ok || len(conditions) == 0 {
		return true
	}
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _ := cond["type"].(string)
		condStatus, _ := cond["status"].(string)
		if condType == "Ready" {
			return condStatus == "True"
		}
	}
	return true
}

const mlflowRoleBindingSuffix = "-mlflow-integration"

// deployMlflowRoleBinding creates or updates a RoleBinding that grants the
// FeatureStore's ServiceAccount access to the MLflow API via the
// mlflow-integration ClusterRole. This is required for Feast UI and service
// pods to query MLflow for lineage data.
// When MLflow is disabled, the RoleBinding is removed.
func (feast *FeastServices) deployMlflowRoleBinding() error {
	applied := feast.Handler.FeatureStore.Status.Applied.Mlflow
	rbName := GetFeastName(feast.Handler.FeatureStore) + mlflowRoleBindingSuffix

	rb := &rbacv1.RoleBinding{}
	rb.Name = rbName
	rb.Namespace = feast.Handler.FeatureStore.Namespace
	rb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))

	if applied == nil || !applied.Enabled {
		return feast.Handler.DeleteOwnedFeastObj(rb)
	}

	logger := log.FromContext(feast.Handler.Context)
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, rb, controllerutil.MutateFn(func() error {
		rb.Labels = feast.getLabels()
		rb.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      GetFeastName(feast.Handler.FeatureStore),
				Namespace: feast.Handler.FeatureStore.Namespace,
			},
		}
		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "mlflow-integration",
		}
		return controllerutil.SetControllerReference(feast.Handler.FeatureStore, rb, feast.Handler.Scheme)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled MLflow RoleBinding", "RoleBinding", rbName, "operation", op)
	}
	return nil
}
