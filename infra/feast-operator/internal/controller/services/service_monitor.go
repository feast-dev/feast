/*
Copyright 2026 Feast Community.

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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var serviceMonitorGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "ServiceMonitor",
}

// createOrDeleteServiceMonitor reconciles the ServiceMonitor for the
// FeatureStore's online store metrics endpoint. When the Prometheus Operator
// CRD is not present in the cluster, this is a no-op. When metrics are enabled
// on the online store, a ServiceMonitor is created; otherwise any existing
// ServiceMonitor is deleted.
func (feast *FeastServices) createOrDeleteServiceMonitor() error {
	if !hasServiceMonitorCRD {
		return nil
	}

	if feast.isOnlineStore() && feast.isMetricsEnabled(OnlineFeastType) {
		return feast.createServiceMonitor()
	}

	return feast.deleteServiceMonitor()
}

func (feast *FeastServices) createServiceMonitor() error {
	logger := log.FromContext(feast.Handler.Context)
	sm := feast.initServiceMonitor()
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, sm, controllerutil.MutateFn(func() error {
		return feast.setServiceMonitor(sm)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ServiceMonitor", sm.GetName(), "operation", op)
	}
	return nil
}

func (feast *FeastServices) deleteServiceMonitor() error {
	sm := feast.initServiceMonitor()
	return feast.Handler.DeleteOwnedFeastObj(sm)
}

func (feast *FeastServices) initServiceMonitor() *unstructured.Unstructured {
	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	sm.SetName(feast.GetFeastServiceName(OnlineFeastType))
	sm.SetNamespace(feast.Handler.FeatureStore.Namespace)
	return sm
}

func (feast *FeastServices) setServiceMonitor(sm *unstructured.Unstructured) error {
	cr := feast.Handler.FeatureStore

	sm.SetLabels(feast.getFeastTypeLabels(OnlineFeastType))

	sm.Object["spec"] = map[string]interface{}{
		"endpoints": []interface{}{
			map[string]interface{}{
				"port": "metrics",
				"path": "/metrics",
			},
		},
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{
				NameLabelKey:        cr.Name,
				ServiceTypeLabelKey: string(OnlineFeastType),
			},
		},
	}

	return controllerutil.SetControllerReference(cr, sm, feast.Handler.Scheme)
}
