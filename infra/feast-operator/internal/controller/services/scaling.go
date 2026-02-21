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
	"errors"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultHPACPUUtilization int32 = 80
	defaultHPAMinReplicas    int32 = 1
)

// validateScaling checks that scaling configuration is compatible with the
// persistence backends. File-based stores (SQLite, DuckDB, registry.db)
// cannot be safely shared across replicas.
func (feast *FeastServices) validateScaling() error {
	cr := feast.Handler.FeatureStore
	if !isScalingEnabled(cr) {
		return nil
	}
	if isFilePersistence(cr) {
		return errors.New(
			"horizontal scaling (replicas > 1 or autoscaling) requires DB-backed persistence " +
				"for all enabled services. File-based persistence (SQLite, DuckDB, registry.db) " +
				"is incompatible with multiple replicas")
	}
	return nil
}

// getDesiredReplicas returns the replica count the operator should set on the
// Deployment. When autoscaling is configured the Deployment replicas field is
// left to the HPA (nil is returned). Otherwise the static replica count is
// returned, defaulting to 1 when no scaling config is present.
func (feast *FeastServices) getDesiredReplicas() *int32 {
	cr := feast.Handler.FeatureStore
	if cr.Status.Applied.Services == nil || cr.Status.Applied.Services.Scaling == nil {
		return nil
	}
	scaling := cr.Status.Applied.Services.Scaling
	if scaling.Autoscaling != nil {
		// HPA manages replicas; do not set them on the Deployment
		return nil
	}
	if scaling.Replicas != nil {
		r := *scaling.Replicas
		return &r
	}
	return nil
}

// createOrDeleteHPA reconciles the HorizontalPodAutoscaler for the FeatureStore
// deployment. If autoscaling is not configured, any existing HPA is deleted.
func (feast *FeastServices) createOrDeleteHPA() error {
	cr := feast.Handler.FeatureStore
	hpa := feast.initHPA()

	scaling := cr.Status.Applied.Services.Scaling
	if scaling == nil || scaling.Autoscaling == nil {
		return feast.Handler.DeleteOwnedFeastObj(hpa)
	}

	logger := log.FromContext(feast.Handler.Context)
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, hpa, controllerutil.MutateFn(func() error {
		return feast.setHPA(hpa)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "HorizontalPodAutoscaler", hpa.Name, "operation", op)
	}

	return nil
}

func (feast *FeastServices) initHPA() *autoscalingv2.HorizontalPodAutoscaler {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: feast.GetObjectMeta(),
	}
	hpa.SetGroupVersionKind(autoscalingv2.SchemeGroupVersion.WithKind("HorizontalPodAutoscaler"))
	return hpa
}

func (feast *FeastServices) setHPA(hpa *autoscalingv2.HorizontalPodAutoscaler) error {
	cr := feast.Handler.FeatureStore
	scaling := cr.Status.Applied.Services.Scaling
	if scaling == nil || scaling.Autoscaling == nil {
		return nil
	}
	autoscaling := scaling.Autoscaling

	hpa.Labels = feast.getLabels()

	deploy := feast.initFeastDeploy()
	minReplicas := defaultHPAMinReplicas
	if autoscaling.MinReplicas != nil {
		minReplicas = *autoscaling.MinReplicas
	}

	hpa.Spec = autoscalingv2.HorizontalPodAutoscalerSpec{
		ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       deploy.Name,
		},
		MinReplicas: &minReplicas,
		MaxReplicas: autoscaling.MaxReplicas,
	}

	if len(autoscaling.Metrics) > 0 {
		hpa.Spec.Metrics = autoscaling.Metrics
	} else {
		hpa.Spec.Metrics = defaultHPAMetrics()
	}

	if autoscaling.Behavior != nil {
		hpa.Spec.Behavior = autoscaling.Behavior
	}

	return controllerutil.SetControllerReference(cr, hpa, feast.Handler.Scheme)
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

// updateScalingStatus updates the scaling status fields based on the current deployment state.
func (feast *FeastServices) updateScalingStatus() {
	cr := feast.Handler.FeatureStore
	if !isScalingEnabled(cr) {
		cr.Status.ScalingStatus = nil
		return
	}

	deployment, err := feast.GetDeployment()
	if err != nil {
		return
	}

	var desired int32
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}

	cr.Status.ScalingStatus = &feastdevv1.ScalingStatus{
		CurrentReplicas: deployment.Status.ReadyReplicas,
		DesiredReplicas: desired,
	}
}
