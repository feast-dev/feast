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
	routev1 "github.com/openshift/api/route/v1"

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
	if feast.noLocalCoreServerConfigured() {
		return errors.New("at least one local server must be configured. e.g. registry / online / offline")
	}
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
	if feast.isOfflineStore() {
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

	if feast.isOnlineStore() {
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
	if feast.isUiServer() {
		if err = feast.deployFeastServiceByType(UIFeastType); err != nil {
			return err
		}
		if err = feast.createRoute(UIFeastType); err != nil {
			return err
		}
	} else {
		if err := feast.removeFeastServiceByType(UIFeastType); err != nil {
			return err
		}
		if err := feast.removeRoute(UIFeastType); err != nil {
			return err
		}
	}

	if err := feast.createServiceAccount(); err != nil {
		return err
	}
	if err := feast.createDeployment(); err != nil {
		return err
	}
	if err := feast.deployClient(); err != nil {
		return err
	}
	if err := feast.deployCronJob(); err != nil {
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
	if serviceConfig := feast.getServerConfigs(feastType); serviceConfig != nil {
		// For registry service, only create service if gRPC is enabled
		if feastType == RegistryFeastType && feast.isRegistryServer() {
			registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry
			if registry.Local.Server.GRPC == nil || *registry.Local.Server.GRPC {
				if err := feast.createService(feastType); err != nil {
					return feast.setFeastServiceCondition(err, feastType)
				}
			} else {
				// Delete service if gRPC is disabled
				_ = feast.Handler.DeleteOwnedFeastObj(feast.initFeastSvc(feastType))
			}
		} else {
			// For non-registry services, always create service
			if err := feast.createService(feastType); err != nil {
				return feast.setFeastServiceCondition(err, feastType)
			}
		}

		// Create REST API service if needed
		if feastType == RegistryFeastType && feast.isRegistryServer() {
			registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry
			if registry.Local.Server.RestAPI != nil && *registry.Local.Server.RestAPI {
				if err := feast.createRestService(feastType); err != nil {
					return feast.setFeastServiceCondition(err, feastType)
				}
			} else {
				// Delete REST API service if REST API is disabled
				_ = feast.Handler.DeleteOwnedFeastObj(&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      feast.GetFeastRestServiceName(feastType),
						Namespace: feast.Handler.FeatureStore.Namespace,
					},
				})
			}
		}
	} else {
		_ = feast.Handler.DeleteOwnedFeastObj(feast.initFeastSvc(feastType))
		// Delete REST API service if it exists
		_ = feast.Handler.DeleteOwnedFeastObj(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      feast.GetFeastRestServiceName(feastType),
				Namespace: feast.Handler.FeatureStore.Namespace,
			},
		})
	}
	return feast.setFeastServiceCondition(nil, feastType)
}

func (feast *FeastServices) removeFeastServiceByType(feastType FeastServiceType) error {
	if err := feast.Handler.DeleteOwnedFeastObj(feast.initFeastSvc(feastType)); err != nil {
		return err
	}
	if err := feast.Handler.DeleteOwnedFeastObj(feast.initPVC(feastType)); err != nil {
		return err
	}
	apimeta.RemoveStatusCondition(&feast.Handler.FeatureStore.Status.Conditions, FeastServiceConditions[feastType][metav1.ConditionTrue].Type)
	return nil
}

func (feast *FeastServices) removeRoute(feastType FeastServiceType) error {
	if !isOpenShift {
		return nil
	}
	route := feast.initRoute(feastType)
	if err := feast.Handler.DeleteOwnedFeastObj(route); err != nil {
		return err
	}
	return nil
}

func (feast *FeastServices) createService(feastType FeastServiceType) error {
	logger := log.FromContext(feast.Handler.Context)
	svc := feast.initFeastSvc(feastType)
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, svc, controllerutil.MutateFn(func() error {
		return feast.setService(svc, feastType, false)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Service", svc.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) createServiceAccount() error {
	logger := log.FromContext(feast.Handler.Context)
	sa := feast.initFeastSA()
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, sa, controllerutil.MutateFn(func() error {
		return feast.setServiceAccount(sa)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ServiceAccount", sa.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) createDeployment() error {
	logger := log.FromContext(feast.Handler.Context)
	deploy := feast.initFeastDeploy()
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, deploy, controllerutil.MutateFn(func() error {
		return feast.setDeployment(deploy)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Deployment", deploy.Name, "operation", op)
	}

	return nil
}

func (feast *FeastServices) createRoute(feastType FeastServiceType) error {
	logger := log.FromContext(feast.Handler.Context)
	if !isOpenShift {
		return nil
	}
	logger.Info("Reconciling route for Feast service", "ServiceType", feastType)
	route := feast.initRoute(feastType)
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, route, controllerutil.MutateFn(func() error {
		return feast.setRoute(route, feastType)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Route", route.Name, "operation", op)
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

func (feast *FeastServices) setDeployment(deploy *appsv1.Deployment) error {
	cr := feast.Handler.FeatureStore
	replicas := deploy.Spec.Replicas

	deploy.Labels = feast.getLabels()
	deploy.Spec = appsv1.DeploymentSpec{
		Replicas: replicas,
		Selector: metav1.SetAsLabelSelector(deploy.GetLabels()),
		Strategy: feast.getDeploymentStrategy(),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: deploy.GetLabels(),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: feast.initFeastSA().Name,
				SecurityContext:    cr.Status.Applied.Services.SecurityContext,
			},
		},
	}
	if err := feast.setPod(&deploy.Spec.Template.Spec); err != nil {
		return err
	}
	return controllerutil.SetControllerReference(cr, deploy, feast.Handler.Scheme)
}

func (feast *FeastServices) setPod(podSpec *corev1.PodSpec) error {
	if err := feast.setContainers(podSpec); err != nil {
		return err
	}
	feast.mountTlsConfigs(podSpec)
	feast.mountPvcConfigs(podSpec)
	feast.mountEmptyDirVolumes(podSpec)
	feast.mountUserDefinedVolumes(podSpec)

	return nil
}

func (feast *FeastServices) setContainers(podSpec *corev1.PodSpec) error {
	fsYamlB64, err := feast.GetServiceFeatureStoreYamlBase64()
	if err != nil {
		return err
	}

	feast.setInitContainer(podSpec, fsYamlB64)
	if feast.isRegistryServer() {
		feast.setContainer(&podSpec.Containers, RegistryFeastType, fsYamlB64)
	}
	if feast.isOnlineServer() {
		feast.setContainer(&podSpec.Containers, OnlineFeastType, fsYamlB64)
	}
	if feast.isOfflineServer() {
		feast.setContainer(&podSpec.Containers, OfflineFeastType, fsYamlB64)
	}
	if feast.isUiServer() {
		feast.setContainer(&podSpec.Containers, UIFeastType, fsYamlB64)
	}
	return nil
}

func (feast *FeastServices) setContainer(containers *[]corev1.Container, feastType FeastServiceType, fsYamlB64 string) {
	if serverConfigs := feast.getServerConfigs(feastType); serverConfigs != nil {
		name := string(feastType)
		workingDir := feast.getFeatureRepoDir()
		cmd := feast.getContainerCommand(feastType)
		container := getContainer(name, workingDir, cmd, serverConfigs.ContainerConfigs, fsYamlB64)
		tls := feast.getTlsConfigs(feastType)
		probeHandler := feast.getProbeHandler(feastType, tls)
		container.Ports = []corev1.ContainerPort{}

		isRegistry := feastType == RegistryFeastType && feast.isRegistryServer()
		registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry

		grpcEnabled := !isRegistry || registry.Local.Server.GRPC == nil || *registry.Local.Server.GRPC
		if grpcEnabled {
			container.Ports = append(container.Ports, corev1.ContainerPort{
				Name:          name,
				ContainerPort: getTargetPort(feastType, tls),
				Protocol:      corev1.ProtocolTCP,
			})
		}

		if isRegistry && registry.Local.Server.RestAPI != nil && *registry.Local.Server.RestAPI {
			container.Ports = append(container.Ports, corev1.ContainerPort{
				Name:          name + "-rest",
				ContainerPort: getTargetRestPort(feastType, tls),
				Protocol:      corev1.ProtocolTCP,
			})
		}

		container.StartupProbe = &corev1.Probe{
			ProbeHandler:     probeHandler,
			PeriodSeconds:    3,
			FailureThreshold: 40,
		}
		container.LivenessProbe = &corev1.Probe{
			ProbeHandler:     probeHandler,
			PeriodSeconds:    20,
			FailureThreshold: 6,
		}
		container.ReadinessProbe = &corev1.Probe{
			ProbeHandler:  probeHandler,
			PeriodSeconds: 10,
		}
		volumeMounts := feast.getVolumeMounts(feastType)
		if len(volumeMounts) > 0 {
			container.VolumeMounts = append(container.VolumeMounts, volumeMounts...)
		}
		*containers = append(*containers, *container)
	}
}

func getContainer(name, workingDir string, cmd []string, containerConfigs feastdevv1alpha1.ContainerConfigs, fsYamlB64 string) *corev1.Container {
	container := &corev1.Container{
		Name:    name,
		Command: cmd,
	}
	if len(workingDir) > 0 {
		container.WorkingDir = workingDir
	}
	if len(fsYamlB64) > 0 {
		container.Env = []corev1.EnvVar{
			{
				Name:  TmpFeatureStoreYamlEnvVar,
				Value: fsYamlB64,
			},
		}
	}
	applyCtrConfigs(container, containerConfigs)
	return container
}

func (feast *FeastServices) mountUserDefinedVolumes(podSpec *corev1.PodSpec) {
	var volumes []corev1.Volume
	if feast.Handler.FeatureStore.Status.Applied.Services != nil {
		volumes = feast.Handler.FeatureStore.Status.Applied.Services.Volumes
	}
	if len(volumes) > 0 {
		podSpec.Volumes = append(podSpec.Volumes, volumes...)
	}
}

func (feast *FeastServices) getVolumeMounts(feastType FeastServiceType) (volumeMounts []corev1.VolumeMount) {
	if serviceConfigs := feast.getServerConfigs(feastType); serviceConfigs != nil {
		return serviceConfigs.VolumeMounts
	}
	return []corev1.VolumeMount{} // Default empty slice
}

func (feast *FeastServices) setRoute(route *routev1.Route, feastType FeastServiceType) error {

	svcName := feast.GetFeastServiceName(feastType)
	route.Labels = feast.getFeastTypeLabels(feastType)

	tls := feast.getTlsConfigs(feastType)
	route.Spec = routev1.RouteSpec{
		To: routev1.RouteTargetReference{
			Kind: "Service",
			Name: svcName,
		},
		Port: &routev1.RoutePort{
			TargetPort: intstr.FromInt(int(getTargetPort(feastType, tls))),
		},
	}
	if tls.IsTLS() {
		route.Spec.TLS = &routev1.TLSConfig{
			Termination:                   routev1.TLSTerminationReencrypt,
			InsecureEdgeTerminationPolicy: routev1.InsecureEdgeTerminationPolicyRedirect,
		}
	}

	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, route, feast.Handler.Scheme)
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

	if feastType == RegistryFeastType && feast.isRegistryServer() {
		registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry
		if registry.Local.Server.GRPC != nil {
			if *registry.Local.Server.GRPC {
				deploySettings.Args = append(deploySettings.Args, "--grpc")
			} else {
				deploySettings.Args = append(deploySettings.Args, "--no-grpc")
			}
		}
		if registry.Local.Server.RestAPI != nil && *registry.Local.Server.RestAPI {
			deploySettings.Args = append(deploySettings.Args, "--rest-api")
			deploySettings.Args = append(deploySettings.Args, "--rest-port", strconv.Itoa(int(getTargetRestPort(feastType, tls))))
		}
	}
	if tls.IsTLS() {
		targetPort = deploySettings.TargetHttpsPort
		feastTlsPath := GetTlsPath(feastType)
		deploySettings.Args = append(deploySettings.Args, []string{"--key", feastTlsPath + tls.SecretKeyNames.TlsKey,
			"--cert", feastTlsPath + tls.SecretKeyNames.TlsCrt}...)
	}
	deploySettings.Args = append(deploySettings.Args, []string{"-p", strconv.Itoa(int(targetPort))}...)

	// Combine base command, options, and arguments
	feastCommand := append([]string{baseCommand}, options...)
	feastCommand = append(feastCommand, deploySettings.Args...)

	return feastCommand
}

func (feast *FeastServices) getDeploymentStrategy() appsv1.DeploymentStrategy {
	if feast.Handler.FeatureStore.Status.Applied.Services.DeploymentStrategy != nil {
		return *feast.Handler.FeatureStore.Status.Applied.Services.DeploymentStrategy
	}
	return appsv1.DeploymentStrategy{
		Type: appsv1.RecreateDeploymentStrategyType,
	}
}

func (feast *FeastServices) setInitContainer(podSpec *corev1.PodSpec, fsYamlB64 string) {
	applied := feast.Handler.FeatureStore.Status.Applied
	if applied.FeastProjectDir != nil && !applied.Services.DisableInitContainers {
		feastProjectDir := applied.FeastProjectDir
		workingDir := getOfflineMountPath(feast.Handler.FeatureStore)
		projectPath := workingDir + "/" + applied.FeastProject
		container := corev1.Container{
			Name:  "feast-init",
			Image: getFeatureServerImage(),
			Env: []corev1.EnvVar{
				{
					Name:  TmpFeatureStoreYamlEnvVar,
					Value: fsYamlB64,
				},
			},
			Command:    []string{"bash", "-c"},
			WorkingDir: workingDir,
		}

		var createCommand string
		if feastProjectDir.Init != nil {
			initSlice := []string{"feast", "init"}
			if feastProjectDir.Init.Minimal {
				initSlice = append(initSlice, "-m")
			}
			if len(feastProjectDir.Init.Template) > 0 {
				initSlice = append(initSlice, "-t", feastProjectDir.Init.Template)
			}
			initSlice = append(initSlice, applied.FeastProject)
			createCommand = strings.Join(initSlice, " ")
		} else if feastProjectDir.Git != nil {
			gitSlice := []string{"git"}
			for key, value := range feastProjectDir.Git.Configs {
				gitSlice = append(gitSlice, "-c", key+"="+value)
			}
			gitSlice = append(gitSlice, "clone", feastProjectDir.Git.URL, projectPath)

			if len(feastProjectDir.Git.Ref) > 0 {
				gitSlice = append(gitSlice, "&&", "cd "+projectPath, "&&", "git checkout "+feastProjectDir.Git.Ref)
			}
			createCommand = strings.Join(gitSlice, " ")

			if feastProjectDir.Git.Env != nil {
				container.Env = envOverride(container.Env, *feastProjectDir.Git.Env)
			}
			if feastProjectDir.Git.EnvFrom != nil {
				container.EnvFrom = *feastProjectDir.Git.EnvFrom
			}
		}

		featureRepoDir := feast.getFeatureRepoDir()
		container.Args = []string{
			"echo \"Creating feast repository...\"\necho '" + createCommand + "'\n" +
				"if [[ ! -d " + featureRepoDir + " ]]; then " + createCommand + "; fi;\n" +
				"echo $" + TmpFeatureStoreYamlEnvVar + " | base64 -d \u003e " + featureRepoDir + "/feature_store.yaml;\necho \"Feast repo creation complete\";\n",
		}
		podSpec.InitContainers = append(podSpec.InitContainers, container)
	}
}

func (feast *FeastServices) setService(svc *corev1.Service, feastType FeastServiceType, isRestService bool) error {
	svc.Labels = feast.getFeastTypeLabels(feastType)
	if feast.isOpenShiftTls(feastType) {
		if len(svc.Annotations) == 0 {
			svc.Annotations = map[string]string{}
		}
		svc.Annotations["service.beta.openshift.io/serving-cert-secret-name"] = svc.Name + tlsNameSuffix
	}

	var port int32 = HttpPort
	scheme := HttpScheme
	tls := feast.getTlsConfigs(feastType)
	if tls.IsTLS() {
		port = HttpsPort
		scheme = HttpsScheme
	}

	var targetPort int32
	if isRestService {
		targetPort = getTargetRestPort(feastType, tls)
	} else {
		targetPort = getTargetPort(feastType, tls)
	}

	svc.Spec = corev1.ServiceSpec{
		Selector: feast.getLabels(),
		Type:     corev1.ServiceTypeClusterIP,
		Ports: []corev1.ServicePort{
			{
				Name:       scheme,
				Port:       port,
				Protocol:   corev1.ProtocolTCP,
				TargetPort: intstr.FromInt(int(targetPort)),
			},
		},
	}

	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, svc, feast.Handler.Scheme)
}

// createRestService creates a separate service for the Registry REST API
func (feast *FeastServices) createRestService(feastType FeastServiceType) error {
	if feast.isRegistryServer() {

		registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry
		if registry.Local.Server.RestAPI == nil || !*registry.Local.Server.RestAPI {
			return nil
		}

		logger := log.FromContext(feast.Handler.Context)
		svc := feast.initFeastRestSvc(feastType)
		if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, svc, controllerutil.MutateFn(func() error {
			return feast.setService(svc, feastType, true)
		})); err != nil {
			return err
		} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
			logger.Info("Successfully reconciled", "Service", svc.Name, "operation", op)
		}
	}

	return nil
}

func (feast *FeastServices) setServiceAccount(sa *corev1.ServiceAccount) error {
	sa.Labels = feast.getLabels()
	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, sa, feast.Handler.Scheme)
}

func (feast *FeastServices) createNewPVC(pvcCreate *feastdevv1alpha1.PvcCreate, feastType FeastServiceType) (*corev1.PersistentVolumeClaim, error) {
	pvc := feast.initPVC(feastType)

	pvc.Spec = corev1.PersistentVolumeClaimSpec{
		AccessModes: pvcCreate.AccessModes,
		Resources:   pvcCreate.Resources,
	}
	if pvcCreate.StorageClassName != nil {
		pvc.Spec.StorageClassName = pvcCreate.StorageClassName
	}
	return pvc, controllerutil.SetControllerReference(feast.Handler.FeatureStore, pvc, feast.Handler.Scheme)
}

func (feast *FeastServices) getServerConfigs(feastType FeastServiceType) *feastdevv1alpha1.ServerConfigs {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	switch feastType {
	case OfflineFeastType:
		if feast.isOfflineStore() {
			return appliedServices.OfflineStore.Server
		}
	case OnlineFeastType:
		if feast.isOnlineStore() {
			return appliedServices.OnlineStore.Server
		}
	case RegistryFeastType:
		if feast.isRegistryServer() {
			return &appliedServices.Registry.Local.Server.ServerConfigs
		}
	case UIFeastType:
		return appliedServices.UI
	}
	return nil
}

func (feast *FeastServices) getLogLevelForType(feastType FeastServiceType) *string {
	if serviceConfigs := feast.getServerConfigs(feastType); serviceConfigs != nil {
		return serviceConfigs.LogLevel
	}
	return nil
}

// GetObjectMeta returns the feast k8s object metadata with type
func (feast *FeastServices) GetObjectMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: GetFeastName(feast.Handler.FeatureStore), Namespace: feast.Handler.FeatureStore.Namespace}
}

// GetObjectMeta returns the feast k8s object metadata with type
func (feast *FeastServices) GetObjectMetaType(feastType FeastServiceType) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: feast.GetFeastServiceName(feastType), Namespace: feast.Handler.FeatureStore.Namespace}
}

func (feast *FeastServices) GetFeastServiceName(feastType FeastServiceType) string {
	return GetFeastServiceName(feast.Handler.FeatureStore, feastType)
}

func (feast *FeastServices) GetDeployment() (appsv1.Deployment, error) {
	deployment := appsv1.Deployment{}
	obj := feast.GetObjectMeta()
	err := feast.Handler.Get(feast.Handler.Context, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, &deployment)
	return deployment, err
}

// GetFeastServiceName returns the feast service object name based on service type
func GetFeastServiceName(featureStore *feastdevv1alpha1.FeatureStore, feastType FeastServiceType) string {
	return GetFeastName(featureStore) + "-" + string(feastType)
}

func GetFeastName(featureStore *feastdevv1alpha1.FeatureStore) string {
	return handler.FeastPrefix + featureStore.Name
}

func (feast *FeastServices) getFeastTypeLabels(feastType FeastServiceType) map[string]string {
	labels := feast.getLabels()
	labels[ServiceTypeLabelKey] = string(feastType)
	return labels
}

func (feast *FeastServices) getLabels() map[string]string {
	return map[string]string{
		NameLabelKey: feast.Handler.FeatureStore.Name,
	}
}

func (feast *FeastServices) setServiceHostnames() error {
	feast.Handler.FeatureStore.Status.ServiceHostnames = feastdevv1alpha1.ServiceHostnames{}
	domain := svcDomain + ":"
	if feast.isOfflineServer() {
		objMeta := feast.initFeastSvc(OfflineFeastType)
		feast.Handler.FeatureStore.Status.ServiceHostnames.OfflineStore = objMeta.Name + "." + objMeta.Namespace + domain +
			getPortStr(feast.Handler.FeatureStore.Status.Applied.Services.OfflineStore.Server.TLS)
	}
	if feast.isOnlineServer() {
		objMeta := feast.initFeastSvc(OnlineFeastType)
		feast.Handler.FeatureStore.Status.ServiceHostnames.OnlineStore = objMeta.Name + "." + objMeta.Namespace + domain +
			getPortStr(feast.Handler.FeatureStore.Status.Applied.Services.OnlineStore.Server.TLS)
	}
	if feast.isRegistryServer() {
		objMeta := feast.initFeastSvc(RegistryFeastType)
		registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry
		feast.Handler.FeatureStore.Status.ServiceHostnames.Registry = objMeta.Name + "." + objMeta.Namespace + domain +
			getPortStr(feast.Handler.FeatureStore.Status.Applied.Services.Registry.Local.Server.TLS)
		if registry.Local.Server.RestAPI != nil && *registry.Local.Server.RestAPI {
			// Use the REST API service name
			restSvcName := feast.GetFeastRestServiceName(RegistryFeastType)
			feast.Handler.FeatureStore.Status.ServiceHostnames.RegistryRest = restSvcName + "." + objMeta.Namespace + domain +
				getPortStr(feast.Handler.FeatureStore.Status.Applied.Services.Registry.Local.Server.TLS)
		}
	} else if feast.isRemoteRegistry() {
		return feast.setRemoteRegistryURL()
	}
	if feast.isUiServer() {
		objMeta := feast.initFeastSvc(UIFeastType)
		feast.Handler.FeatureStore.Status.ServiceHostnames.UI = objMeta.Name + "." + objMeta.Namespace + domain +
			getPortStr(feast.Handler.FeatureStore.Status.Applied.Services.UI.TLS)
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
		// referenced/remote registry must use the local registry server option and be in a 'Ready' state.
		if remoteFeast != nil &&
			remoteFeast.isRegistryServer() &&
			apimeta.IsStatusConditionTrue(remoteFeast.Handler.FeatureStore.Status.Conditions, feastdevv1alpha1.RegistryReadyType) &&
			len(remoteFeast.Handler.FeatureStore.Status.ServiceHostnames.Registry) > 0 {
			// Check if gRPC server is enabled
			registry := remoteFeast.Handler.FeatureStore.Status.Applied.Services.Registry
			if registry.Local.Server.GRPC == nil || !*registry.Local.Server.GRPC {
				return errors.New("Remote feast registry of referenced FeatureStore '" + remoteFeast.Handler.FeatureStore.Name + "' must have gRPC server enabled")
			}
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
		if feast.Handler.FeatureStore.Status.Applied.FeastProject != remoteFeastObj.Status.Applied.FeastProject {
			return nil, errors.New("FeatureStore '" + remoteFeastObj.Name + "' is using a different feast project than '" + feast.Handler.FeatureStore.Status.Applied.FeastProject + "'. Project names must match.")
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

func (feast *FeastServices) isRegistryServer() bool {
	return IsRegistryServer(feast.Handler.FeatureStore)
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

func (feast *FeastServices) isOfflineServer() bool {
	return feast.isOfflineStore() &&
		feast.Handler.FeatureStore.Status.Applied.Services.OfflineStore.Server != nil
}

func (feast *FeastServices) isOfflineStore() bool {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.OfflineStore != nil
}

func (feast *FeastServices) isOnlineServer() bool {
	return feast.isOnlineStore() &&
		feast.Handler.FeatureStore.Status.Applied.Services.OnlineStore.Server != nil
}

func (feast *FeastServices) isOnlineStore() bool {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.OnlineStore != nil
}

func (feast *FeastServices) noLocalCoreServerConfigured() bool {
	return !(feast.isRegistryServer() || feast.isOnlineServer() || feast.isOfflineServer())
}

func (feast *FeastServices) isUiServer() bool {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.UI != nil
}

func (feast *FeastServices) initFeastDeploy() *appsv1.Deployment {
	deploy := &appsv1.Deployment{
		ObjectMeta: feast.GetObjectMeta(),
	}
	deploy.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	return deploy
}

func (feast *FeastServices) initFeastSvc(feastType FeastServiceType) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: feast.GetObjectMetaType(feastType),
	}
	svc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	return svc
}

func (feast *FeastServices) initFeastRestSvc(feastType FeastServiceType) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      feast.GetFeastRestServiceName(feastType),
			Namespace: feast.Handler.FeatureStore.Namespace,
			Labels:    feast.getFeastTypeLabels(feastType),
		},
	}
	svc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	return svc
}

func (feast *FeastServices) initFeastSA() *corev1.ServiceAccount {
	sa := &corev1.ServiceAccount{
		ObjectMeta: feast.GetObjectMeta(),
	}
	sa.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ServiceAccount"))
	return sa
}

func (feast *FeastServices) initPVC(feastType FeastServiceType) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: feast.GetObjectMetaType(feastType),
	}
	pvc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"))
	return pvc
}

func (feast *FeastServices) initRoute(feastType FeastServiceType) *routev1.Route {
	route := &routev1.Route{
		ObjectMeta: feast.GetObjectMetaType(feastType),
	}
	route.SetGroupVersionKind(routev1.SchemeGroupVersion.WithKind("Route"))
	return route
}

func applyCtrConfigs(container *corev1.Container, containerConfigs feastdevv1alpha1.ContainerConfigs) {
	container.Image = *containerConfigs.DefaultCtrConfigs.Image
	// apply optional container configs
	if containerConfigs.OptionalCtrConfigs.Env != nil {
		container.Env = envOverride(container.Env, *containerConfigs.OptionalCtrConfigs.Env)
	}
	if containerConfigs.OptionalCtrConfigs.EnvFrom != nil {
		container.EnvFrom = *containerConfigs.OptionalCtrConfigs.EnvFrom
	}
	if containerConfigs.OptionalCtrConfigs.ImagePullPolicy != nil {
		container.ImagePullPolicy = *containerConfigs.OptionalCtrConfigs.ImagePullPolicy
	}
	if containerConfigs.OptionalCtrConfigs.Resources != nil {
		container.Resources = *containerConfigs.OptionalCtrConfigs.Resources
	}
}

func (feast *FeastServices) mountPvcConfigs(podSpec *corev1.PodSpec) {
	for _, feastType := range feastServerTypes {
		if pvcConfig, hasPvcConfig := hasPvcConfig(feast.Handler.FeatureStore, feastType); hasPvcConfig {
			feast.mountPvcConfig(podSpec, pvcConfig, feastType)
		}
	}
}

func (feast *FeastServices) mountPvcConfig(podSpec *corev1.PodSpec, pvcConfig *feastdevv1alpha1.PvcConfig, feastType FeastServiceType) {
	if podSpec != nil && pvcConfig != nil {
		volName := feast.initPVC(feastType).Name
		pvcName := volName
		if pvcConfig.Ref != nil {
			pvcName = pvcConfig.Ref.Name
		}
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		})
		if feastType == OfflineFeastType {
			for i := range podSpec.InitContainers {
				podSpec.InitContainers[i].VolumeMounts = append(podSpec.InitContainers[i].VolumeMounts, corev1.VolumeMount{
					Name:      volName,
					MountPath: pvcConfig.MountPath,
				})
			}
		}
		for i := range podSpec.Containers {
			podSpec.Containers[i].VolumeMounts = append(podSpec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: pvcConfig.MountPath,
			})
		}
	}
}

func (feast *FeastServices) mountEmptyDirVolumes(podSpec *corev1.PodSpec) {
	if shouldMountEmptyDir(feast.Handler.FeatureStore) {
		mountEmptyDirVolume(podSpec)
	}
}

func (feast *FeastServices) getFeatureRepoDir() string {
	applied := feast.Handler.FeatureStore.Status.Applied
	feastProjectDir := getOfflineMountPath(feast.Handler.FeatureStore) + "/" + applied.FeastProject
	if applied.FeastProjectDir != nil && applied.FeastProjectDir.Git != nil && len(applied.FeastProjectDir.Git.FeatureRepoPath) > 0 {
		return feastProjectDir + "/" + applied.FeastProjectDir.Git.FeatureRepoPath
	}
	return feastProjectDir + "/" + FeatureRepoDir
}

func mountEmptyDirVolume(podSpec *corev1.PodSpec) {
	if podSpec != nil {
		volName := strings.TrimPrefix(EphemeralPath, "/")
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
		for i := range podSpec.InitContainers {
			podSpec.InitContainers[i].VolumeMounts = append(podSpec.InitContainers[i].VolumeMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: EphemeralPath,
			})
		}
		for i := range podSpec.Containers {
			podSpec.Containers[i].VolumeMounts = append(podSpec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: EphemeralPath,
			})
		}
	}
}

func getTargetPort(feastType FeastServiceType, tls *feastdevv1alpha1.TlsConfigs) int32 {
	if tls.IsTLS() {
		return FeastServiceConstants[feastType].TargetHttpsPort
	}
	return FeastServiceConstants[feastType].TargetHttpPort
}

func getTargetRestPort(feastType FeastServiceType, tls *feastdevv1alpha1.TlsConfigs) int32 {
	if tls.IsTLS() {
		return FeastServiceConstants[feastType].TargetRestHttpsPort
	}
	return FeastServiceConstants[feastType].TargetRestHttpPort
}

func (feast *FeastServices) getProbeHandler(feastType FeastServiceType, tls *feastdevv1alpha1.TlsConfigs) corev1.ProbeHandler {
	if feastType == RegistryFeastType {
		registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry
		grpcEnabled := registry.Local.Server.GRPC == nil || *registry.Local.Server.GRPC
		restEnabled := registry.Local.Server.RestAPI != nil && *registry.Local.Server.RestAPI

		if restEnabled {
			targetPort := getTargetRestPort(feastType, tls)
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
		} else if grpcEnabled {
			targetPort := getTargetPort(feastType, tls)
			return corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt(int(targetPort)),
				},
			}
		}
	} else if feastType == OnlineFeastType {
		targetPort := getTargetPort(feastType, tls)
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
	targetPort := getTargetPort(feastType, tls)
	return corev1.ProbeHandler{
		TCPSocket: &corev1.TCPSocketAction{
			Port: intstr.FromInt(int(targetPort)),
		},
	}
}

func IsDeploymentAvailable(conditions []appsv1.DeploymentCondition) bool {
	for _, condition := range conditions {
		if condition.Type == appsv1.DeploymentAvailable {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

// GetFeastRestServiceName returns the feast REST service object name based on service type
func (feast *FeastServices) GetFeastRestServiceName(feastType FeastServiceType) string {
	return feast.GetFeastServiceName(feastType) + "-rest"
}
