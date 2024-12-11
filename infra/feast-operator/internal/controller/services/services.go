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
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
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

// Apply defaults and set service hostnames in FeatureStore status
func (feast *FeastServices) ApplyDefaults() error {
	ApplyDefaultsToStatus(feast.Handler.FeatureStore)
	if err := feast.setTlsDefaults(); err != nil {
		return err
	}
	if err := feast.setServiceHostnames(); err != nil {
		return err
	}
	return nil
}

// Deploy the feast services
func (feast *FeastServices) Deploy() error {
	openshiftTls, err := feast.checkOpenshiftTls()
	if err != nil {
		return err
	}
	if openshiftTls {
		if err := feast.createCaConfigMap(); err != nil {
			return err
		}
	} else {
		_ = feast.Handler.DeleteOwnedFeastObj(feast.initCaConfigMap())
	}

	services := feast.Handler.FeatureStore.Status.Applied.Services
	if feast.isOfflinStore() {
		err := feast.validateOfflineStorePersistence(services.OfflineStore.Persistence)
		if err != nil {
			return err
		}

		if err = feast.deployFeastServiceByType(OfflineFeastType); err != nil {
			return err
		}
	} else {
		if err := feast.removeFeastServiceByType(OfflineFeastType); err != nil {
			return err
		}
	}

	if feast.isOnlinStore() {
		err := feast.validateOnlineStorePersistence(services.OnlineStore.Persistence)
		if err != nil {
			return err
		}

		if err = feast.deployFeastServiceByType(OnlineFeastType); err != nil {
			return err
		}
	} else {
		if err := feast.removeFeastServiceByType(OnlineFeastType); err != nil {
			return err
		}
	}

	if feast.isLocalRegistry() {
		err := feast.validateRegistryPersistence(services.Registry.Local.Persistence)
		if err != nil {
			return err
		}

		if err = feast.deployFeastServiceByType(RegistryFeastType); err != nil {
			return err
		}
	} else {
		if err := feast.removeFeastServiceByType(RegistryFeastType); err != nil {
			return err
		}
	}

	if err := feast.deployClient(); err != nil {
		return err
	}

	return nil
}

func (feast *FeastServices) validateRegistryPersistence(registryPersistence *feastdevv1alpha1.RegistryPersistence) error {
	if registryPersistence != nil {
		dbPersistence := registryPersistence.DBPersistence

		if dbPersistence != nil && len(dbPersistence.Type) > 0 {
			if err := checkRegistryDBStorePersistenceType(dbPersistence.Type); err != nil {
				return err
			}

			if len(dbPersistence.SecretRef.Name) > 0 {
				secretRef := dbPersistence.SecretRef.Name
				if _, err := feast.getSecret(secretRef); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (feast *FeastServices) validateOnlineStorePersistence(onlinePersistence *feastdevv1alpha1.OnlineStorePersistence) error {
	if onlinePersistence != nil {
		dbPersistence := onlinePersistence.DBPersistence

		if dbPersistence != nil && len(dbPersistence.Type) > 0 {
			if err := checkOnlineStoreDBStorePersistenceType(dbPersistence.Type); err != nil {
				return err
			}

			if len(dbPersistence.SecretRef.Name) > 0 {
				secretRef := dbPersistence.SecretRef.Name
				if _, err := feast.getSecret(secretRef); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (feast *FeastServices) validateOfflineStorePersistence(offlinePersistence *feastdevv1alpha1.OfflineStorePersistence) error {
	if offlinePersistence != nil {
		filePersistence := offlinePersistence.FilePersistence
		dbPersistence := offlinePersistence.DBPersistence

		if filePersistence != nil && len(filePersistence.Type) > 0 {
			if err := checkOfflineStoreFilePersistenceType(filePersistence.Type); err != nil {
				return err
			}
		} else if dbPersistence != nil &&
			len(dbPersistence.Type) > 0 {
			if err := checkOfflineStoreDBStorePersistenceType(dbPersistence.Type); err != nil {
				return err
			}

			if len(dbPersistence.SecretRef.Name) > 0 {
				secretRef := dbPersistence.SecretRef.Name
				if _, err := feast.getSecret(secretRef); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (feast *FeastServices) deployFeastServiceByType(feastType FeastServiceType) error {
	if pvcCreate, shouldCreate := shouldCreatePvc(feast.Handler.FeatureStore, feastType); shouldCreate {
		if err := feast.createPVC(pvcCreate, feastType); err != nil {
			return feast.setFeastServiceCondition(err, feastType)
		}
	} else {
		_ = feast.Handler.DeleteOwnedFeastObj(feast.initPVC(feastType))
	}
	if err := feast.createService(feastType); err != nil {
		return feast.setFeastServiceCondition(err, feastType)
	}
	if err := feast.createServiceAccount(feastType); err != nil {
		return feast.setFeastServiceCondition(err, feastType)
	}
	if err := feast.createDeployment(feastType); err != nil {
		return feast.setFeastServiceCondition(err, feastType)
	}
	return feast.setFeastServiceCondition(nil, feastType)
}

func (feast *FeastServices) removeFeastServiceByType(feastType FeastServiceType) error {
	if err := feast.Handler.DeleteOwnedFeastObj(feast.initFeastSvc(feastType)); err != nil {
		return err
	}
	if err := feast.Handler.DeleteOwnedFeastObj(feast.initFeastDeploy(feastType)); err != nil {
		return err
	}
	if err := feast.Handler.DeleteOwnedFeastObj(feast.initFeastSA(feastType)); err != nil {
		return err
	}
	if err := feast.Handler.DeleteOwnedFeastObj(feast.initPVC(feastType)); err != nil {
		return err
	}
	apimeta.RemoveStatusCondition(&feast.Handler.FeatureStore.Status.Conditions, FeastServiceConditions[feastType][metav1.ConditionTrue].Type)
	return nil
}

func (feast *FeastServices) createService(feastType FeastServiceType) error {
	logger := log.FromContext(feast.Handler.Context)
	svc := feast.initFeastSvc(feastType)
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, svc, controllerutil.MutateFn(func() error {
		return feast.setService(svc, feastType)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Service", svc.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) createServiceAccount(feastType FeastServiceType) error {
	logger := log.FromContext(feast.Handler.Context)
	sa := feast.initFeastSA(feastType)
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, sa, controllerutil.MutateFn(func() error {
		return feast.setServiceAccount(sa, feastType)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ServiceAccount", sa.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) createDeployment(feastType FeastServiceType) error {
	logger := log.FromContext(feast.Handler.Context)
	deploy := feast.initFeastDeploy(feastType)
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, deploy, controllerutil.MutateFn(func() error {
		return feast.setDeployment(deploy, feastType)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Deployment", deploy.Name, "operation", op)
	}

	return nil
}

func (feast *FeastServices) createPVC(pvcCreate *feastdevv1alpha1.PvcCreate, feastType FeastServiceType) error {
	logger := log.FromContext(feast.Handler.Context)
	pvc, err := feast.createNewPVC(pvcCreate, feastType)
	if err != nil {
		return err
	}

	// PVCs are immutable, so we only create... we don't update an existing one.
	err = feast.Handler.Client.Get(feast.Handler.Context, client.ObjectKeyFromObject(pvc), pvc)
	if err != nil && apierrors.IsNotFound(err) {
		err = feast.Handler.Client.Create(feast.Handler.Context, pvc)
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
	sa := feast.initFeastSA(feastType)
	tls := feast.getTlsConfigs(feastType)
	serviceConfigs := feast.getServiceConfigs(feastType)
	defaultServiceConfigs := serviceConfigs.DefaultConfigs
	probeHandler := getProbeHandler(feastType, tls)

	deploy.Spec = appsv1.DeploymentSpec{
		Replicas: &DefaultReplicas,
		Selector: metav1.SetAsLabelSelector(deploy.GetLabels()),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: deploy.GetLabels(),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: sa.Name,
				Containers: []corev1.Container{
					{
						Name:    string(feastType),
						Image:   *defaultServiceConfigs.Image,
						Command: feast.getContainerCommand(feastType),
						Ports: []corev1.ContainerPort{
							{
								Name:          string(feastType),
								ContainerPort: getTargetPort(feastType, tls),
								Protocol:      corev1.ProtocolTCP,
							},
						},
						Env: []corev1.EnvVar{
							{
								Name:  FeatureStoreYamlEnvVar,
								Value: fsYamlB64,
							},
						},
						StartupProbe: &corev1.Probe{
							ProbeHandler:     probeHandler,
							PeriodSeconds:    3,
							FailureThreshold: 40,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler:     probeHandler,
							PeriodSeconds:    20,
							FailureThreshold: 6,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:  probeHandler,
							PeriodSeconds: 10,
						},
					},
				},
			},
		},
	}

	// configs are applied here
	podSpec := &deploy.Spec.Template.Spec
	applyOptionalContainerConfigs(&podSpec.Containers[0], serviceConfigs.OptionalConfigs)
	feast.mountTlsConfig(feastType, podSpec)
	if pvcConfig, hasPvcConfig := hasPvcConfig(feast.Handler.FeatureStore, feastType); hasPvcConfig {
		mountPvcConfig(podSpec, pvcConfig, deploy.Name)
	}

	switch feastType {
	case OfflineFeastType:
		feast.registryClientPodConfigs(podSpec)
	case OnlineFeastType:
		feast.registryClientPodConfigs(podSpec)
		feast.offlineClientPodConfigs(podSpec)
	}

	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, deploy, feast.Handler.Scheme)
}

func (feast *FeastServices) getContainerCommand(feastType FeastServiceType) []string {
	baseCommand := "feast"
	options := []string{}
	logLevel := feast.getLogLevelForType(feastType)
	if logLevel != nil {
		options = append(options, "--log-level", strings.ToUpper(*logLevel))
	}

	deploySettings := FeastServiceConstants[feastType]
	targetPort := deploySettings.TargetHttpPort
	tls := feast.getTlsConfigs(feastType)
	if tls.IsTLS() {
		targetPort = deploySettings.TargetHttpsPort
		feastTlsPath := GetTlsPath(feastType)
		deploySettings.Args = append(deploySettings.Args, []string{"--key", feastTlsPath + tls.SecretKeyNames.TlsKey,
			"--cert", feastTlsPath + tls.SecretKeyNames.TlsCrt}...)
	}
	deploySettings.Args = append(deploySettings.Args, []string{"-p", strconv.Itoa(int(targetPort))}...)

	if feastType == OfflineFeastType {
		if tls.IsTLS() && feast.Handler.FeatureStore.Status.Applied.Services.OfflineStore.TLS.VerifyClient != nil {
			deploySettings.Args = append(deploySettings.Args,
				[]string{"--verify_client", strconv.FormatBool(*feast.Handler.FeatureStore.Status.Applied.Services.OfflineStore.TLS.VerifyClient)}...)
		}
	}

	// Combine base command, options, and arguments
	feastCommand := append([]string{baseCommand}, options...)
	feastCommand = append(feastCommand, deploySettings.Args...)

	return feastCommand
}

func (feast *FeastServices) offlineClientPodConfigs(podSpec *corev1.PodSpec) {
	feast.mountTlsConfig(OfflineFeastType, podSpec)
}

func (feast *FeastServices) registryClientPodConfigs(podSpec *corev1.PodSpec) {
	feast.setRegistryClientInitContainer(podSpec)
	feast.mountRegistryClientTls(podSpec)
}

func (feast *FeastServices) setRegistryClientInitContainer(podSpec *corev1.PodSpec) {
	hostname := feast.Handler.FeatureStore.Status.ServiceHostnames.Registry
	// add grpc init container if registry is not configured via 'remote.hostname'
	if len(hostname) > 0 && !feast.isRemoteHostnameRegistry() {
		grpcurlFlag := "-plaintext"
		hostSplit := strings.Split(hostname, ":")
		if len(hostSplit) > 1 && hostSplit[1] == "443" {
			grpcurlFlag = "-insecure"
		}
		podSpec.InitContainers = []corev1.Container{
			{
				Name:  "init-registry",
				Image: "fullstorydev/grpcurl:v1.9.1-alpine",
				Command: []string{
					"sh", "-c",
					"until grpcurl -H \"authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\" " +
						grpcurlFlag + " -d '' -format text " + hostname + " grpc.health.v1.Health/Check; do echo waiting for registry; sleep 2; done",
				},
			},
		}
	}
}

func (feast *FeastServices) setService(svc *corev1.Service, feastType FeastServiceType) error {
	svc.Labels = feast.getLabels(feastType)
	if feast.isOpenShiftTls(feastType) {
		svc.Annotations = map[string]string{
			"service.beta.openshift.io/serving-cert-secret-name": svc.Name + tlsNameSuffix,
		}
	}

	var port int32 = HttpPort
	scheme := HttpScheme
	tls := feast.getTlsConfigs(feastType)
	if tls.IsTLS() {
		port = HttpsPort
		scheme = HttpsScheme
	}
	svc.Spec = corev1.ServiceSpec{
		Selector: svc.GetLabels(),
		Type:     corev1.ServiceTypeClusterIP,
		Ports: []corev1.ServicePort{
			{
				Name:       scheme,
				Port:       port,
				Protocol:   corev1.ProtocolTCP,
				TargetPort: intstr.FromInt(int(getTargetPort(feastType, tls))),
			},
		},
	}

	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, svc, feast.Handler.Scheme)
}

func (feast *FeastServices) setServiceAccount(sa *corev1.ServiceAccount, feastType FeastServiceType) error {
	sa.Labels = feast.getLabels(feastType)
	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, sa, feast.Handler.Scheme)
}

func (feast *FeastServices) createNewPVC(pvcCreate *feastdevv1alpha1.PvcCreate, feastType FeastServiceType) (*corev1.PersistentVolumeClaim, error) {
	pvc := feast.initPVC(feastType)

	pvc.Spec = corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		Resources:   pvcCreate.Resources,
	}
	if pvcCreate.StorageClassName != nil {
		pvc.Spec.StorageClassName = pvcCreate.StorageClassName
	}
	return pvc, controllerutil.SetControllerReference(feast.Handler.FeatureStore, pvc, feast.Handler.Scheme)
}

func (feast *FeastServices) getServiceConfigs(feastType FeastServiceType) feastdevv1alpha1.ServiceConfigs {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	switch feastType {
	case OfflineFeastType:
		if feast.isOfflinStore() {
			return appliedServices.OfflineStore.ServiceConfigs
		}
	case OnlineFeastType:
		if feast.isOnlinStore() {
			return appliedServices.OnlineStore.ServiceConfigs
		}
	case RegistryFeastType:
		if feast.isLocalRegistry() {
			return appliedServices.Registry.Local.ServiceConfigs
		}
	}
	return feastdevv1alpha1.ServiceConfigs{}
}

func (feast *FeastServices) getLogLevelForType(feastType FeastServiceType) *string {
	services := feast.Handler.FeatureStore.Status.Applied.Services
	switch feastType {
	case OfflineFeastType:
		if services.OfflineStore != nil && services.OfflineStore.LogLevel != "" {
			return &services.OfflineStore.LogLevel
		}
	case OnlineFeastType:
		if services.OnlineStore != nil && services.OnlineStore.LogLevel != "" {
			return &services.OnlineStore.LogLevel
		}
	case RegistryFeastType:
		if services.Registry != nil && services.Registry.Local.LogLevel != "" {
			return &services.Registry.Local.LogLevel
		}
	}
	return nil
}

// GetObjectMeta returns the feast k8s object metadata
func (feast *FeastServices) GetObjectMeta(feastType FeastServiceType) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: feast.GetFeastServiceName(feastType), Namespace: feast.Handler.FeatureStore.Namespace}
}

func (feast *FeastServices) GetFeastServiceName(feastType FeastServiceType) string {
	return GetFeastServiceName(feast.Handler.FeatureStore, feastType)
}

// GetFeastServiceName returns the feast service object name based on service type
func GetFeastServiceName(featureStore *feastdevv1alpha1.FeatureStore, feastType FeastServiceType) string {
	return GetFeastName(featureStore) + "-" + string(feastType)
}

func GetFeastName(featureStore *feastdevv1alpha1.FeatureStore) string {
	return handler.FeastPrefix + featureStore.Name
}

func (feast *FeastServices) getLabels(feastType FeastServiceType) map[string]string {
	return map[string]string{
		NameLabelKey:        feast.Handler.FeatureStore.Name,
		ServiceTypeLabelKey: string(feastType),
	}
}

func (feast *FeastServices) setServiceHostnames() error {
	feast.Handler.FeatureStore.Status.ServiceHostnames = feastdevv1alpha1.ServiceHostnames{}
	domain := svcDomain + ":"
	if feast.isOfflinStore() {
		objMeta := feast.GetObjectMeta(OfflineFeastType)
		port := strconv.Itoa(HttpPort)
		if feast.offlineTls() {
			port = strconv.Itoa(HttpsPort)
		}
		feast.Handler.FeatureStore.Status.ServiceHostnames.OfflineStore = objMeta.Name + "." + objMeta.Namespace + domain + port
	}
	if feast.isOnlinStore() {
		objMeta := feast.GetObjectMeta(OnlineFeastType)
		feast.Handler.FeatureStore.Status.ServiceHostnames.OnlineStore = objMeta.Name + "." + objMeta.Namespace + domain +
			getPortStr(feast.Handler.FeatureStore.Status.Applied.Services.OnlineStore.TLS)
	}
	if feast.isLocalRegistry() {
		objMeta := feast.GetObjectMeta(RegistryFeastType)
		feast.Handler.FeatureStore.Status.ServiceHostnames.Registry = objMeta.Name + "." + objMeta.Namespace + domain +
			getPortStr(feast.Handler.FeatureStore.Status.Applied.Services.Registry.Local.TLS)
	} else if feast.isRemoteRegistry() {
		return feast.setRemoteRegistryURL()
	}
	return nil
}

func (feast *FeastServices) setFeastServiceCondition(err error, feastType FeastServiceType) error {
	conditionMap := FeastServiceConditions[feastType]
	if err != nil {
		logger := log.FromContext(feast.Handler.Context)
		cond := conditionMap[metav1.ConditionFalse]
		cond.Message = "Error: " + err.Error()
		apimeta.SetStatusCondition(&feast.Handler.FeatureStore.Status.Conditions, cond)
		logger.Error(err, "Error deploying the FeatureStore "+string(ClientFeastType)+" service")
		return err
	} else {
		apimeta.SetStatusCondition(&feast.Handler.FeatureStore.Status.Conditions, conditionMap[metav1.ConditionTrue])
	}
	return nil
}

func (feast *FeastServices) setRemoteRegistryURL() error {
	if feast.isRemoteHostnameRegistry() {
		feast.Handler.FeatureStore.Status.ServiceHostnames.Registry = *feast.Handler.FeatureStore.Status.Applied.Services.Registry.Remote.Hostname
	} else if feast.IsRemoteRefRegistry() {
		remoteFeast, err := feast.getRemoteRegistryFeastHandler()
		if err != nil {
			return err
		}
		// referenced/remote registry must use the local install option and be in a 'Ready' state.
		if remoteFeast != nil &&
			remoteFeast.isLocalRegistry() &&
			apimeta.IsStatusConditionTrue(remoteFeast.Handler.FeatureStore.Status.Conditions, feastdevv1alpha1.RegistryReadyType) {
			feast.Handler.FeatureStore.Status.ServiceHostnames.Registry = remoteFeast.Handler.FeatureStore.Status.ServiceHostnames.Registry
		} else {
			return errors.New("Remote feast registry of referenced FeatureStore '" + remoteFeast.Handler.FeatureStore.Name + "' is not ready")
		}
	}
	return nil
}

func (feast *FeastServices) getRemoteRegistryFeastHandler() (*FeastServices, error) {
	if feast.IsRemoteRefRegistry() {
		feastRemoteRef := feast.Handler.FeatureStore.Status.Applied.Services.Registry.Remote.FeastRef
		// default to FeatureStore namespace if not set
		if len(feastRemoteRef.Namespace) == 0 {
			feastRemoteRef.Namespace = feast.Handler.FeatureStore.Namespace
		}
		nsName := types.NamespacedName{Name: feastRemoteRef.Name, Namespace: feastRemoteRef.Namespace}
		crNsName := client.ObjectKeyFromObject(feast.Handler.FeatureStore)
		if nsName == crNsName {
			return nil, errors.New("FeatureStore '" + crNsName.Name + "' can't reference itself in `spec.services.registry.remote.feastRef`")
		}
		remoteFeastObj := &feastdevv1alpha1.FeatureStore{}
		if err := feast.Handler.Client.Get(feast.Handler.Context, nsName, remoteFeastObj); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, errors.New("Referenced FeatureStore '" + feastRemoteRef.Name + "' was not found")
			}
			return nil, err
		}
		return &FeastServices{
			Handler: handler.FeastHandler{
				Client:       feast.Handler.Client,
				Context:      feast.Handler.Context,
				FeatureStore: remoteFeastObj,
				Scheme:       feast.Handler.Scheme,
			},
		}, nil
	}
	return nil, nil
}

func (feast *FeastServices) isLocalRegistry() bool {
	return IsLocalRegistry(feast.Handler.FeatureStore)
}

func (feast *FeastServices) isRemoteRegistry() bool {
	return isRemoteRegistry(feast.Handler.FeatureStore)
}

func (feast *FeastServices) IsRemoteRefRegistry() bool {
	return feast.isRemoteRegistry() &&
		feast.Handler.FeatureStore.Status.Applied.Services.Registry.Remote.FeastRef != nil
}

func (feast *FeastServices) isRemoteHostnameRegistry() bool {
	return feast.isRemoteRegistry() &&
		feast.Handler.FeatureStore.Status.Applied.Services.Registry.Remote.Hostname != nil
}

func (feast *FeastServices) isOfflinStore() bool {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.OfflineStore != nil
}

func (feast *FeastServices) isOnlinStore() bool {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.OnlineStore != nil
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

func (feast *FeastServices) initFeastSA(feastType FeastServiceType) *corev1.ServiceAccount {
	sa := &corev1.ServiceAccount{
		ObjectMeta: feast.GetObjectMeta(feastType),
	}
	sa.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ServiceAccount"))
	return sa
}

func (feast *FeastServices) initPVC(feastType FeastServiceType) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: feast.GetObjectMeta(feastType),
	}
	pvc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"))
	return pvc
}

func applyOptionalContainerConfigs(container *corev1.Container, optionalConfigs feastdevv1alpha1.OptionalConfigs) {
	if optionalConfigs.Env != nil {
		container.Env = envOverride(container.Env, *optionalConfigs.Env)
	}
	if optionalConfigs.ImagePullPolicy != nil {
		container.ImagePullPolicy = *optionalConfigs.ImagePullPolicy
	}
	if optionalConfigs.Resources != nil {
		container.Resources = *optionalConfigs.Resources
	}
}

func mountPvcConfig(podSpec *corev1.PodSpec, pvcConfig *feastdevv1alpha1.PvcConfig, deployName string) {
	if podSpec != nil && pvcConfig != nil {
		container := &podSpec.Containers[0]
		var pvcName string
		if pvcConfig.Create != nil {
			pvcName = deployName
		} else {
			pvcName = pvcConfig.Ref.Name
		}

		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: pvcName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      pvcName,
			MountPath: pvcConfig.MountPath,
		})
	}
}

func getTargetPort(feastType FeastServiceType, tls *feastdevv1alpha1.TlsConfigs) int32 {
	if tls.IsTLS() {
		return FeastServiceConstants[feastType].TargetHttpsPort
	}
	return FeastServiceConstants[feastType].TargetHttpPort
}

func getProbeHandler(feastType FeastServiceType, tls *feastdevv1alpha1.TlsConfigs) corev1.ProbeHandler {
	targetPort := getTargetPort(feastType, tls)
	if feastType == OnlineFeastType {
		probeHandler := corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/health",
				Port: intstr.FromInt(int(targetPort)),
			},
		}
		if tls.IsTLS() {
			probeHandler.HTTPGet.Scheme = corev1.URISchemeHTTPS
		}
		return probeHandler
	}
	return corev1.ProbeHandler{
		TCPSocket: &corev1.TCPSocketAction{
			Port: intstr.FromInt(int(targetPort)),
		},
	}
}
