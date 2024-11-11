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
	"errors"
	"strconv"
	"strings"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Deploy the feast services
func (feast *FeastServices) Deploy() error {
	if err := feast.setServiceHostnames(); err != nil {
		return err
	}

	services := feast.FeatureStore.Status.Applied.Services
	if services != nil {
		if services.OfflineStore != nil {
			if services.OfflineStore.Persistence != nil &&
				services.OfflineStore.Persistence.FilePersistence != nil &&
				len(services.OfflineStore.Persistence.FilePersistence.Type) > 0 {
				if err := checkOfflineStoreFilePersistenceType(services.OfflineStore.Persistence.FilePersistence.Type); err != nil {
					return err
				}
			}
			if err := feast.deployFeastServiceByType(OfflineFeastType); err != nil {
				return err
			}
		} else {
			if err := feast.removeFeastServiceByType(OfflineFeastType); err != nil {
				return err
			}
		}

		if services.OnlineStore != nil {
			if err := feast.deployFeastServiceByType(OnlineFeastType); err != nil {
				return err
			}
		} else {
			if err := feast.removeFeastServiceByType(OnlineFeastType); err != nil {
				return err
			}
		}

		if feast.isLocalRegistry() {
			if err := feast.deployFeastServiceByType(RegistryFeastType); err != nil {
				return err
			}
		} else {
			if err := feast.removeFeastServiceByType(RegistryFeastType); err != nil {
				return err
			}
		}
	}

	if err := feast.deployClient(); err != nil {
		return err
	}

	return nil
}

func (feast *FeastServices) deployFeastServiceByType(feastType FeastServiceType) error {
	if err := feast.createService(feastType); err != nil {
		return feast.setFeastServiceCondition(err, feastType)
	}
	if err := feast.createDeployment(feastType); err != nil {
		return feast.setFeastServiceCondition(err, feastType)
	}
	if pvcCreate, shouldCreate := shouldCreatePvc(feast.FeatureStore, feastType); shouldCreate {
		if err := feast.createPVC(pvcCreate, feastType); err != nil {
			return feast.setFeastServiceCondition(err, feastType)
		}
	} else {
		_ = feast.deleteOwnedFeastObj(feast.initPVC(feastType))
	}
	return feast.setFeastServiceCondition(nil, feastType)
}

func (feast *FeastServices) removeFeastServiceByType(feastType FeastServiceType) error {
	if err := feast.deleteOwnedFeastObj(feast.initFeastDeploy(feastType)); err != nil {
		return err
	}
	if err := feast.deleteOwnedFeastObj(feast.initFeastSvc(feastType)); err != nil {
		return err
	}
	if err := feast.deleteOwnedFeastObj(feast.initPVC(feastType)); err != nil {
		return err
	}
	apimeta.RemoveStatusCondition(&feast.FeatureStore.Status.Conditions, FeastServiceConditions[feastType][metav1.ConditionTrue].Type)
	return nil
}

func (feast *FeastServices) createService(feastType FeastServiceType) error {
	logger := log.FromContext(feast.Context)
	svc := feast.initFeastSvc(feastType)
	if op, err := controllerutil.CreateOrUpdate(feast.Context, feast.Client, svc, controllerutil.MutateFn(func() error {
		return feast.setService(svc, feastType)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Service", svc.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) createDeployment(feastType FeastServiceType) error {
	logger := log.FromContext(feast.Context)
	deploy := feast.initFeastDeploy(feastType)
	if op, err := controllerutil.CreateOrUpdate(feast.Context, feast.Client, deploy, controllerutil.MutateFn(func() error {
		return feast.setDeployment(deploy, feastType)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Deployment", deploy.Name, "operation", op)
	}

	return nil
}

func (feast *FeastServices) createPVC(pvcCreate *feastdevv1alpha1.PvcCreate, feastType FeastServiceType) error {
	logger := log.FromContext(feast.Context)
	pvc, err := feast.createNewPVC(pvcCreate, feastType)
	if err != nil {
		return err
	}

	err = feast.Client.Get(feast.Context, client.ObjectKeyFromObject(pvc), pvc)
	if err != nil && apierrors.IsNotFound(err) {
		err = feast.Client.Create(feast.Context, pvc)
		if err != nil {
			return err
		}
		logger.Info("Successfully created", "PersistentVolumeClaim", pvc.Name)
	}

	return nil
}

func (feast *FeastServices) setDeployment(deploy *appsv1.Deployment, feastType FeastServiceType) error {
	fsYamlB64, err := feast.GetServiceFeatureStoreYamlBase64(feastType)
	if err != nil {
		return err
	}
	deploy.Labels = feast.getLabels(feastType)
	deploySettings := FeastServiceConstants[feastType]
	serviceConfigs := feast.getServiceConfigs(feastType)
	defaultServiceConfigs := serviceConfigs.DefaultConfigs

	// standard configs are applied here
	probeHandler := corev1.ProbeHandler{
		TCPSocket: &corev1.TCPSocketAction{
			Port: intstr.FromInt(int(deploySettings.TargetPort)),
		},
	}
	deploy.Spec = appsv1.DeploymentSpec{
		Replicas: &DefaultReplicas,
		Selector: metav1.SetAsLabelSelector(deploy.GetLabels()),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: deploy.GetLabels(),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:    string(feastType),
						Image:   *defaultServiceConfigs.Image,
						Command: deploySettings.Command,
						Ports: []corev1.ContainerPort{
							{
								Name:          string(feastType),
								ContainerPort: deploySettings.TargetPort,
								Protocol:      corev1.ProtocolTCP,
							},
						},
						Env: []corev1.EnvVar{
							{
								Name:  FeatureStoreYamlEnvVar,
								Value: fsYamlB64,
							},
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler:        probeHandler,
							InitialDelaySeconds: 30,
							PeriodSeconds:       30,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:        probeHandler,
							InitialDelaySeconds: 20,
							PeriodSeconds:       10,
						},
					},
				},
			},
		},
	}

	// configs are applied here
	container := &deploy.Spec.Template.Spec.Containers[0]
	applyOptionalContainerConfigs(container, serviceConfigs.OptionalConfigs)
	if pvcConfig, hasPvcConfig := hasPvcConfig(feast.FeatureStore, feastType); hasPvcConfig {
		mountPvcConfig(&deploy.Spec.Template.Spec, pvcConfig, deploy.Name)
	}

	return controllerutil.SetControllerReference(feast.FeatureStore, deploy, feast.Scheme)
}

func (feast *FeastServices) setService(svc *corev1.Service, feastType FeastServiceType) error {
	svc.Labels = feast.getLabels(feastType)
	deploySettings := FeastServiceConstants[feastType]

	svc.Spec = corev1.ServiceSpec{
		Selector: svc.GetLabels(),
		Type:     corev1.ServiceTypeClusterIP,
		Ports: []corev1.ServicePort{
			{
				Name:       strings.ToLower(string(corev1.URISchemeHTTP)),
				Port:       HttpPort,
				Protocol:   corev1.ProtocolTCP,
				TargetPort: intstr.FromInt(int(deploySettings.TargetPort)),
			},
		},
	}

	return controllerutil.SetControllerReference(feast.FeatureStore, svc, feast.Scheme)
}

func (feast *FeastServices) createNewPVC(pvcCreate *feastdevv1alpha1.PvcCreate, feastType FeastServiceType) (*corev1.PersistentVolumeClaim, error) {
	pvc := feast.initPVC(feastType)

	pvc.Spec = corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		Resources:   pvcCreate.Resources,
	}
	return pvc, controllerutil.SetControllerReference(feast.FeatureStore, pvc, feast.Scheme)
}

func (feast *FeastServices) getServiceConfigs(feastType FeastServiceType) feastdevv1alpha1.ServiceConfigs {
	appliedSpec := feast.FeatureStore.Status.Applied
	if feastType == OfflineFeastType && appliedSpec.Services.OfflineStore != nil {
		return appliedSpec.Services.OfflineStore.ServiceConfigs
	}
	if feastType == OnlineFeastType && appliedSpec.Services.OnlineStore != nil {
		return appliedSpec.Services.OnlineStore.ServiceConfigs
	}
	if feastType == RegistryFeastType && appliedSpec.Services.Registry != nil {
		if appliedSpec.Services.Registry.Local != nil {
			return appliedSpec.Services.Registry.Local.ServiceConfigs
		}
	}
	return feastdevv1alpha1.ServiceConfigs{}
}

// GetObjectMeta returns the feast k8s object metadata
func (feast *FeastServices) GetObjectMeta(feastType FeastServiceType) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: feast.GetFeastServiceName(feastType), Namespace: feast.FeatureStore.Namespace}
}

// GetFeastServiceName returns the feast service object name based on service type
func (feast *FeastServices) GetFeastServiceName(feastType FeastServiceType) string {
	return feast.getFeastName() + "-" + string(feastType)
}

func (feast *FeastServices) getFeastName() string {
	return FeastPrefix + feast.FeatureStore.Name
}

func (feast *FeastServices) getLabels(feastType FeastServiceType) map[string]string {
	return map[string]string{
		NameLabelKey:        feast.FeatureStore.Name,
		ServiceTypeLabelKey: string(feastType),
	}
}

func (feast *FeastServices) setServiceHostnames() error {
	feast.FeatureStore.Status.ServiceHostnames = feastdevv1alpha1.ServiceHostnames{}
	services := feast.FeatureStore.Status.Applied.Services
	if services != nil {
		domain := svcDomain + ":" + strconv.Itoa(HttpPort)
		if services.OfflineStore != nil {
			objMeta := feast.GetObjectMeta(OfflineFeastType)
			feast.FeatureStore.Status.ServiceHostnames.OfflineStore = objMeta.Name + "." + objMeta.Namespace + domain
		}
		if services.OnlineStore != nil {
			objMeta := feast.GetObjectMeta(OnlineFeastType)
			feast.FeatureStore.Status.ServiceHostnames.OnlineStore = objMeta.Name + "." + objMeta.Namespace + domain
		}
		if feast.isLocalRegistry() {
			objMeta := feast.GetObjectMeta(RegistryFeastType)
			feast.FeatureStore.Status.ServiceHostnames.Registry = objMeta.Name + "." + objMeta.Namespace + domain
		} else if feast.isRemoteRegistry() {
			return feast.setRemoteRegistryURL()
		}
	}
	return nil
}

func (feast *FeastServices) setFeastServiceCondition(err error, feastType FeastServiceType) error {
	conditionMap := FeastServiceConditions[feastType]
	if err != nil {
		logger := log.FromContext(feast.Context)
		cond := conditionMap[metav1.ConditionFalse]
		cond.Message = "Error: " + err.Error()
		apimeta.SetStatusCondition(&feast.FeatureStore.Status.Conditions, cond)
		logger.Error(err, "Error deploying the FeatureStore "+string(ClientFeastType)+" service")
		return err
	} else {
		apimeta.SetStatusCondition(&feast.FeatureStore.Status.Conditions, conditionMap[metav1.ConditionTrue])
	}
	return nil
}

func (feast *FeastServices) setRemoteRegistryURL() error {
	if feast.isRemoteHostnameRegistry() {
		feast.FeatureStore.Status.ServiceHostnames.Registry = *feast.FeatureStore.Status.Applied.Services.Registry.Remote.Hostname
	} else if feast.IsRemoteRefRegistry() {
		feastRemoteRef := feast.FeatureStore.Status.Applied.Services.Registry.Remote.FeastRef
		// default to FeatureStore namespace if not set
		if len(feastRemoteRef.Namespace) == 0 {
			feastRemoteRef.Namespace = feast.FeatureStore.Namespace
		}

		nsName := types.NamespacedName{Name: feastRemoteRef.Name, Namespace: feastRemoteRef.Namespace}
		crNsName := client.ObjectKeyFromObject(feast.FeatureStore)
		if nsName == crNsName {
			return errors.New("FeatureStore '" + crNsName.Name + "' can't reference itself in `spec.services.registry.remote.feastRef`")
		}

		remoteFeastObj := &feastdevv1alpha1.FeatureStore{}
		if err := feast.Client.Get(feast.Context, nsName, remoteFeastObj); err != nil {
			if apierrors.IsNotFound(err) {
				return errors.New("Referenced FeatureStore '" + feastRemoteRef.Name + "' was not found")
			}
			return err
		}

		remoteFeast := FeastServices{
			Client:       feast.Client,
			Context:      feast.Context,
			FeatureStore: remoteFeastObj,
			Scheme:       feast.Scheme,
		}
		// referenced/remote registry must use the local install option and be in a 'Ready' state.
		if remoteFeast.isLocalRegistry() && apimeta.IsStatusConditionTrue(remoteFeastObj.Status.Conditions, feastdevv1alpha1.RegistryReadyType) {
			feast.FeatureStore.Status.ServiceHostnames.Registry = remoteFeastObj.Status.ServiceHostnames.Registry
		} else {
			return errors.New("Remote feast registry of referenced FeatureStore '" + feastRemoteRef.Name + "' is not ready")
		}
	}
	return nil
}

func (feast *FeastServices) isLocalRegistry() bool {
	return isLocalRegistry(feast.FeatureStore)
}

func (feast *FeastServices) isRemoteRegistry() bool {
	appliedServices := feast.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.Registry != nil && appliedServices.Registry.Remote != nil
}

func (feast *FeastServices) IsRemoteRefRegistry() bool {
	if feast.isRemoteRegistry() {
		remote := feast.FeatureStore.Status.Applied.Services.Registry.Remote
		return remote != nil && remote.FeastRef != nil
	}
	return false
}

func (feast *FeastServices) isRemoteHostnameRegistry() bool {
	if feast.isRemoteRegistry() {
		remote := feast.FeatureStore.Status.Applied.Services.Registry.Remote
		return remote != nil && remote.Hostname != nil
	}
	return false
}

func (feast *FeastServices) initFeastDeploy(feastType FeastServiceType) *appsv1.Deployment {
	deploy := &appsv1.Deployment{
		ObjectMeta: feast.GetObjectMeta(feastType),
	}
	deploy.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	return deploy
}

func (feast *FeastServices) initFeastSvc(feastType FeastServiceType) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: feast.GetObjectMeta(feastType),
	}
	svc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	return svc
}

func (feast *FeastServices) initPVC(feastType FeastServiceType) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: feast.GetObjectMeta(feastType),
	}
	pvc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"))
	return pvc
}

// delete an object if the FeatureStore is set as the object's controller/owner
func (feast *FeastServices) deleteOwnedFeastObj(obj client.Object) error {
	name := obj.GetName()
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if err := feast.Client.Get(feast.Context, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	for _, ref := range obj.GetOwnerReferences() {
		if *ref.Controller && ref.UID == feast.FeatureStore.UID {
			if err := feast.Client.Delete(feast.Context, obj); err != nil {
				return err
			}
			log.FromContext(feast.Context).Info("Successfully deleted", kind, name)
		}
	}
	return nil
}

func applyOptionalContainerConfigs(container *corev1.Container, optionalConfigs feastdevv1alpha1.OptionalConfigs) {
	if optionalConfigs.Env != nil {
		container.Env = mergeEnvVarsArrays(container.Env, optionalConfigs.Env)
	}
	if optionalConfigs.ImagePullPolicy != nil {
		container.ImagePullPolicy = *optionalConfigs.ImagePullPolicy
	}
	if optionalConfigs.Resources != nil {
		container.Resources = *optionalConfigs.Resources
	}
}

func mergeEnvVarsArrays(envVars1 []corev1.EnvVar, envVars2 *[]corev1.EnvVar) []corev1.EnvVar {
	merged := make(map[string]corev1.EnvVar)

	// Add all env vars from the first array
	for _, envVar := range envVars1 {
		merged[envVar.Name] = envVar
	}

	// Add all env vars from the second array, overriding duplicates
	for _, envVar := range *envVars2 {
		merged[envVar.Name] = envVar
	}

	// Convert the map back to an array
	result := make([]corev1.EnvVar, 0, len(merged))
	for _, envVar := range merged {
		result = append(result, envVar)
	}

	return result
}

func mountPvcConfig(podSpec *corev1.PodSpec, pvcConfig *feastdevv1alpha1.PvcConfig, deployName string) {
	container := &podSpec.Containers[0]
	var pvcName string
	if pvcConfig.Create != nil {
		pvcName = deployName
	} else {
		pvcName = pvcConfig.Ref.Name
	}

	podSpec.Volumes = []corev1.Volume{
		{
			Name: pvcName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		},
	}
	container.VolumeMounts = []corev1.VolumeMount{
		{
			Name:      pvcName,
			MountPath: pvcConfig.MountPath,
		},
	}
}
