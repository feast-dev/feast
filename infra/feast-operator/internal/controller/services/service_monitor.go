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
	"encoding/json"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	monitoringv1apply "github.com/prometheus-operator/prometheus-operator/pkg/client/applyconfiguration/monitoring/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var serviceMonitorGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "ServiceMonitor",
}

// createOrDeleteServiceMonitor reconciles the ServiceMonitor for the
// FeatureStore's online store metrics endpoint using Server-Side Apply.
// When the Prometheus Operator CRD is not present in the cluster, this is
// a no-op. When metrics are enabled on the online store, a ServiceMonitor
// is applied; otherwise any existing ServiceMonitor is deleted.
func (feast *FeastServices) createOrDeleteServiceMonitor() error {
	if !hasServiceMonitorCRD {
		return nil
	}

	if feast.isOnlineStore() && feast.isMetricsEnabled(OnlineFeastType) {
		return feast.applyServiceMonitor()
	}

	return feast.deleteServiceMonitor()
}

func (feast *FeastServices) applyServiceMonitor() error {
	smApply := feast.buildServiceMonitorApplyConfig()
	data, err := json.Marshal(smApply)
	if err != nil {
		return err
	}

	sm := feast.initServiceMonitor()
	logger := log.FromContext(feast.Handler.Context)
	if err := feast.Handler.Client.Patch(feast.Handler.Context, sm,
		client.RawPatch(types.ApplyPatchType, data),
		client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return err
	}
	logger.Info("Successfully applied", "ServiceMonitor", sm.GetName())

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

// buildServiceMonitorApplyConfig constructs the fully desired ServiceMonitor
// state for Server-Side Apply.
func (feast *FeastServices) buildServiceMonitorApplyConfig() *monitoringv1apply.ServiceMonitorApplyConfiguration {
	cr := feast.Handler.FeatureStore
	objMeta := feast.GetObjectMetaType(OnlineFeastType)

	return monitoringv1apply.ServiceMonitor(objMeta.Name, objMeta.Namespace).
		WithLabels(feast.getFeastTypeLabels(OnlineFeastType)).
		WithOwnerReferences(
			metav1apply.OwnerReference().
				WithAPIVersion(feastdevv1.GroupVersion.String()).
				WithKind("FeatureStore").
				WithName(cr.Name).
				WithUID(cr.UID).
				WithController(true).
				WithBlockOwnerDeletion(true),
		).
		WithSpec(monitoringv1apply.ServiceMonitorSpec().
			WithEndpoints(
				monitoringv1apply.Endpoint().
					WithPort("metrics").
					WithPath("/metrics"),
			).
			WithSelector(metav1apply.LabelSelector().
				WithMatchLabels(map[string]string{
					NameLabelKey:        cr.Name,
					ServiceTypeLabelKey: string(OnlineFeastType),
				}),
			),
		)
}
