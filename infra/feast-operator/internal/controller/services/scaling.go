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
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	hpaac "k8s.io/client-go/applyconfigurations/autoscaling/v2"
	metaac "k8s.io/client-go/applyconfigurations/meta/v1"
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
// deployment using Server-Side Apply with typed apply configurations. If
// autoscaling is not configured, any existing HPA is deleted.
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

	hpaAC := feast.buildHPAApplyConfig()
	data, err := json.Marshal(hpaAC)
	if err != nil {
		return err
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: feast.GetObjectMeta()}
	logger := log.FromContext(feast.Handler.Context)
	if err := feast.Handler.Client.Patch(feast.Handler.Context, hpa,
		client.RawPatch(types.ApplyPatchType, data),
		client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return err
	}
	logger.Info("Successfully applied", "HorizontalPodAutoscaler", hpa.Name)

	return nil
}

// buildHPAApplyConfig constructs the fully desired HPA state as a typed apply
// configuration for Server-Side Apply.
func (feast *FeastServices) buildHPAApplyConfig() *hpaac.HorizontalPodAutoscalerApplyConfiguration {
	cr := feast.Handler.FeatureStore
	autoscaling := cr.Status.Applied.Services.Scaling.Autoscaling
	objMeta := feast.GetObjectMeta()
	deploy := feast.initFeastDeploy()

	minReplicas := defaultHPAMinReplicas
	if autoscaling.MinReplicas != nil {
		minReplicas = *autoscaling.MinReplicas
	}

	hpa := hpaac.HorizontalPodAutoscaler(objMeta.Name, objMeta.Namespace).
		WithLabels(feast.getLabels()).
		WithOwnerReferences(
			metaac.OwnerReference().
				WithAPIVersion(feastdevv1.GroupVersion.String()).
				WithKind("FeatureStore").
				WithName(cr.Name).
				WithUID(cr.UID).
				WithController(true).
				WithBlockOwnerDeletion(true),
		).
		WithSpec(hpaac.HorizontalPodAutoscalerSpec().
			WithScaleTargetRef(
				hpaac.CrossVersionObjectReference().
					WithAPIVersion(appsv1.SchemeGroupVersion.String()).
					WithKind("Deployment").
					WithName(deploy.Name),
			).
			WithMinReplicas(minReplicas).
			WithMaxReplicas(autoscaling.MaxReplicas),
		)

	if len(autoscaling.Metrics) > 0 {
		hpa.Spec.Metrics = convertMetrics(autoscaling.Metrics)
	} else {
		hpa.Spec.Metrics = defaultHPAMetrics()
	}

	if autoscaling.Behavior != nil {
		hpa.Spec.Behavior = convertBehavior(autoscaling.Behavior)
	}

	return hpa
}

func defaultHPAMetrics() []hpaac.MetricSpecApplyConfiguration {
	return []hpaac.MetricSpecApplyConfiguration{
		*hpaac.MetricSpec().
			WithType(autoscalingv2.ResourceMetricSourceType).
			WithResource(
				hpaac.ResourceMetricSource().
					WithName(corev1.ResourceCPU).
					WithTarget(
						hpaac.MetricTarget().
							WithType(autoscalingv2.UtilizationMetricType).
							WithAverageUtilization(defaultHPACPUUtilization),
					),
			),
	}
}

// convertMetrics converts standard API metric specs to their apply configuration
// equivalents via JSON round-trip (the types share identical JSON schemas).
func convertMetrics(metrics []autoscalingv2.MetricSpec) []hpaac.MetricSpecApplyConfiguration {
	data, err := json.Marshal(metrics)
	if err != nil {
		return nil
	}
	var result []hpaac.MetricSpecApplyConfiguration
	if err := json.Unmarshal(data, &result); err != nil {
		return nil
	}
	return result
}

// convertBehavior converts a standard API behavior spec to its apply configuration
// equivalent via JSON round-trip.
func convertBehavior(behavior *autoscalingv2.HorizontalPodAutoscalerBehavior) *hpaac.HorizontalPodAutoscalerBehaviorApplyConfiguration {
	data, err := json.Marshal(behavior)
	if err != nil {
		return nil
	}
	result := &hpaac.HorizontalPodAutoscalerBehaviorApplyConfiguration{}
	if err := json.Unmarshal(data, result); err != nil {
		return nil
	}
	return result
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
