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
	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultHPACPUUtilization int32 = 80
	defaultHPAMinReplicas    int32 = 1
	fieldManager                   = "feast-operator"
)

// getDesiredReplicas returns the replica count the operator should set on the
// Deployment. When autoscaling is configured the Deployment replicas field is
// left to the HPA (nil is returned). Otherwise the static replica count from
// spec.replicas is returned.
func (feast *FeastServices) getDesiredReplicas() *int32 {
	cr := feast.Handler.FeatureStore
	services := cr.Status.Applied.Services
	if services != nil && services.Scaling != nil && services.Scaling.Autoscaling != nil {
		return nil
	}
	if cr.Status.Applied.Replicas != nil {
		r := *cr.Status.Applied.Replicas
		return &r
	}
	return nil
}

// createOrDeleteHPA reconciles the HorizontalPodAutoscaler for the FeatureStore
// deployment using Server-Side Apply. If autoscaling is not configured, any
// existing HPA is deleted.
func (feast *FeastServices) createOrDeleteHPA() error {
	cr := feast.Handler.FeatureStore

	scaling := cr.Status.Applied.Services.Scaling
	if scaling == nil || scaling.Autoscaling == nil {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: feast.GetObjectMeta(),
		}
		hpa.SetGroupVersionKind(autoscalingv2.SchemeGroupVersion.WithKind("HorizontalPodAutoscaler"))
		return feast.Handler.DeleteOwnedFeastObj(hpa)
	}

	hpa := feast.buildHPA()
	logger := log.FromContext(feast.Handler.Context)
	if err := feast.Handler.Client.Patch(feast.Handler.Context, hpa,
		client.Apply, client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return err
	}
	logger.Info("Successfully applied", "HorizontalPodAutoscaler", hpa.Name)

	return nil
}

// buildHPA constructs the fully desired HPA state for Server-Side Apply.
func (feast *FeastServices) buildHPA() *autoscalingv2.HorizontalPodAutoscaler {
	cr := feast.Handler.FeatureStore
	autoscaling := cr.Status.Applied.Services.Scaling.Autoscaling

	deploy := feast.initFeastDeploy()
	minReplicas := defaultHPAMinReplicas
	if autoscaling.MinReplicas != nil {
		minReplicas = *autoscaling.MinReplicas
	}

	metrics := defaultHPAMetrics()
	if len(autoscaling.Metrics) > 0 {
		metrics = autoscaling.Metrics
	}

	isController := true
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			APIVersion: autoscalingv2.SchemeGroupVersion.String(),
			Kind:       "HorizontalPodAutoscaler",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      feast.GetObjectMeta().Name,
			Namespace: feast.GetObjectMeta().Namespace,
			Labels:    feast.getLabels(),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         feastdevv1.GroupVersion.String(),
					Kind:               "FeatureStore",
					Name:               cr.Name,
					UID:                cr.UID,
					Controller:         &isController,
					BlockOwnerDeletion: &isController,
				},
			},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: appsv1.SchemeGroupVersion.String(),
				Kind:       "Deployment",
				Name:       deploy.Name,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: autoscaling.MaxReplicas,
			Metrics:     metrics,
			Behavior:    autoscaling.Behavior,
		},
	}

	return hpa
}

func defaultHPAMetrics() []autoscalingv2.MetricSpec {
	utilization := defaultHPACPUUtilization
	return []autoscalingv2.MetricSpec{
		{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: &utilization,
				},
			},
		},
	}
}

// updateScalingStatus updates the scaling status fields using the deployment
func (feast *FeastServices) updateScalingStatus(deploy *appsv1.Deployment) {
	cr := feast.Handler.FeatureStore

	cr.Status.Replicas = deploy.Status.ReadyReplicas
	labels := feast.getLabels()
	cr.Status.Selector = metav1.FormatLabelSelector(metav1.SetAsLabelSelector(labels))

	if !isScalingEnabled(cr) {
		cr.Status.ScalingStatus = nil
		return
	}

	var desired int32
	if deploy.Spec.Replicas != nil {
		desired = *deploy.Spec.Replicas
	}

	cr.Status.ScalingStatus = &feastdevv1.ScalingStatus{
		CurrentReplicas: deploy.Status.ReadyReplicas,
		DesiredReplicas: desired,
	}
}
