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
	"encoding/base64"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Deploy the feast services
func (feast *FeastServices) Deploy() error {
	logger := log.FromContext(feast.Context)
	cr := feast.FeatureStore

	if err := feast.deployRegistry(); err != nil {
		apimeta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:    feastdevv1alpha1.RegistryReadyType,
			Status:  metav1.ConditionFalse,
			Reason:  feastdevv1alpha1.RegistryFailedReason,
			Message: "Error: " + err.Error(),
		})
		logger.Error(err, "Error deploying the FeatureStore "+string(RegistryFeastType)+" service")
		return err
	} else {
		apimeta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:    feastdevv1alpha1.RegistryReadyType,
			Status:  metav1.ConditionTrue,
			Reason:  feastdevv1alpha1.ReadyReason,
			Message: feastdevv1alpha1.RegistryReadyMessage,
		})
	}

	if err := feast.deployClient(); err != nil {
		apimeta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:    feastdevv1alpha1.ClientReadyType,
			Status:  metav1.ConditionFalse,
			Reason:  feastdevv1alpha1.ClientFailedReason,
			Message: "Error: " + err.Error(),
		})
		logger.Error(err, "Error deploying the FeatureStore "+string(ClientFeastType)+" service")
		return err
	} else {
		apimeta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:    feastdevv1alpha1.ClientReadyType,
			Status:  metav1.ConditionTrue,
			Reason:  feastdevv1alpha1.ReadyReason,
			Message: feastdevv1alpha1.ClientReadyMessage,
		})
	}
	return nil
}

func (feast *FeastServices) deployRegistry() error {
	if err := feast.createRegistryDeployment(); err != nil {
		return err
	}
	if err := feast.createRegistryService(); err != nil {
		return err
	}
	return nil
}

func (feast *FeastServices) createRegistryDeployment() error {
	logger := log.FromContext(feast.Context)
	deploy := &appsv1.Deployment{
		ObjectMeta: feast.GetObjectMeta(RegistryFeastType),
	}
	deploy.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	if op, err := controllerutil.CreateOrUpdate(feast.Context, feast.Client, deploy, controllerutil.MutateFn(func() error {
		return feast.setDeployment(deploy, RegistryFeastType)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Deployment", deploy.Name, "operation", op)
	}

	return nil
}

func (feast *FeastServices) createRegistryService() error {
	logger := log.FromContext(feast.Context)
	svc := &corev1.Service{
		ObjectMeta: feast.GetObjectMeta(RegistryFeastType),
	}
	svc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if op, err := controllerutil.CreateOrUpdate(feast.Context, feast.Client, svc, controllerutil.MutateFn(func() error {
		return feast.setService(svc, RegistryFeastType)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Service", svc.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) setDeployment(deploy *appsv1.Deployment, feastType FeastServiceType) error {
	fsYamlB64, err := feast.GetServiceFeatureStoreYamlBase64()
	if err != nil {
		return err
	}
	replicas := int32(1)
	deploy.Labels = feast.getLabels(feastType)
	deploy.Spec = appsv1.DeploymentSpec{
		Replicas: &replicas,
		Selector: metav1.SetAsLabelSelector(deploy.GetLabels()),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: deploy.GetLabels(),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:            string(feastType),
						Image:           "feastdev/feature-server:" + feast.FeatureStore.Status.FeastVersion,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env: []corev1.EnvVar{
							{
								Name:  FeatureStoreYamlEnvVar,
								Value: fsYamlB64,
							},
						},
					},
				},
			},
		},
	}
	if feastType == RegistryFeastType {
		deploy.Spec.Template.Spec.Containers[0].Command = []string{
			"feast", "serve_registry",
		}
		deploy.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{
			{
				Name:          string(feastType),
				ContainerPort: RegistryPort,
				Protocol:      corev1.ProtocolTCP,
			},
		}
		probeHandler := corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt(int(RegistryPort)),
			},
		}
		deploy.Spec.Template.Spec.Containers[0].LivenessProbe = &corev1.Probe{
			ProbeHandler:        probeHandler,
			InitialDelaySeconds: 30,
			PeriodSeconds:       30,
		}
		deploy.Spec.Template.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
			ProbeHandler:        probeHandler,
			InitialDelaySeconds: 20,
			PeriodSeconds:       10,
		}
	}
	return controllerutil.SetControllerReference(feast.FeatureStore, deploy, feast.Scheme)
}

func (feast *FeastServices) setService(svc *corev1.Service, feastType FeastServiceType) error {
	svc.Labels = feast.getLabels(feastType)
	svc.Spec = corev1.ServiceSpec{
		Selector: svc.GetLabels(),
		Type:     corev1.ServiceTypeClusterIP,
	}
	if feastType == RegistryFeastType {
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "http",
				Port:       int32(80),
				Protocol:   corev1.ProtocolTCP,
				TargetPort: intstr.FromInt(int(RegistryPort)),
			},
		}
		feast.FeatureStore.Status.ServiceUrls.Registry = svc.Name + "." + svc.Namespace + ".svc.cluster.local:80"
	}
	return controllerutil.SetControllerReference(feast.FeatureStore, svc, feast.Scheme)
}

// GetObjectMeta returns the feast k8s object metadata
func (feast *FeastServices) GetObjectMeta(feastType FeastServiceType) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: feast.GetFeastServiceName(feastType), Namespace: feast.FeatureStore.Namespace}
}

func (feast *FeastServices) getLabels(feastType FeastServiceType) map[string]string {
	return map[string]string{
		feastdevv1alpha1.GroupVersion.Group + "/name":         feast.FeatureStore.Name,
		feastdevv1alpha1.GroupVersion.Group + "/service-type": string(feastType),
	}
}

func (feast *FeastServices) getFeastName() string {
	return FeastPrefix + feast.FeatureStore.Name
}

// GetFeastServiceName returns the feast service object name based on service type
func (feast *FeastServices) GetFeastServiceName(feastType FeastServiceType) string {
	return feast.getFeastName() + "-" + string(feastType)
}

// GetServiceFeatureStoreYamlBase64 returns a base64 encoded feature_store.yaml config for the feast service
func (feast *FeastServices) GetServiceFeatureStoreYamlBase64() (string, error) {
	fsYaml, err := feast.getServiceFeatureStoreYaml()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(fsYaml), nil
}

func (feast *FeastServices) getServiceFeatureStoreYaml() ([]byte, error) {
	return yaml.Marshal(feast.getServiceRepoConfig())
}

func (feast *FeastServices) getServiceRepoConfig() RepoConfig {
	appliedSpec := feast.FeatureStore.Status.Applied
	return RepoConfig{
		Project:                       appliedSpec.FeastProject,
		Provider:                      LocalProviderType,
		EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
		Registry: RegistryConfig{
			RegistryType: RegistryFileConfigType,
			Path:         LocalRegistryPath,
		},
	}
}
