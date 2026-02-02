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
	"fmt"
	"strconv"
	"strings"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
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
	if err := feast.validateLocalServers(); err != nil {
		return err
	}
	if err := feast.reconcileOpenshiftTls(); err != nil {
		return err
	}
	if err := feast.reconcileOffline(); err != nil {
		return err
	}
	if err := feast.reconcileOnline(); err != nil {
		return err
	}
	if err := feast.reconcileOnlineGrpc(); err != nil {
		return err
	}
	if err := feast.reconcileRegistry(); err != nil {
		return err
	}
	if err := feast.reconcileUI(); err != nil {
		return err
	}
	return feast.deploySupportServices()
}

func (feast *FeastServices) validateLocalServers() error {
	if feast.noLocalCoreServerConfigured() {
		return errors.New("at least one local server must be configured. e.g. registry / online / offline")
	}
	if feast.isRegistryServer() && !feast.isRegistryGrpcEnabled() && !feast.isRegistryRestEnabled() {
		return errors.New("at least one of gRPC or REST API must be enabled for registry service")
	}
	return nil
}

func (feast *FeastServices) reconcileOpenshiftTls() error {
	openshiftTls, err := feast.checkOpenshiftTls()
	if err != nil {
		return err
	}
	if openshiftTls {
		return feast.createCaConfigMap()
	}
	_ = feast.Handler.DeleteOwnedFeastObj(feast.initCaConfigMap())
	return nil
}

func (feast *FeastServices) reconcileOffline() error {
	if feast.isOfflineStore() {
		services := feast.Handler.FeatureStore.Status.Applied.Services
		if err := feast.validateOfflineStorePersistence(services.OfflineStore.Persistence); err != nil {
			return err
		}
		return feast.deployFeastServiceByType(OfflineFeastType)
	}
	return feast.removeFeastServiceByType(OfflineFeastType)
}

func (feast *FeastServices) reconcileOnline() error {
	if feast.isOnlineStore() {
		services := feast.Handler.FeatureStore.Status.Applied.Services
		if err := feast.validateOnlineStorePersistence(services.OnlineStore.Persistence); err != nil {
			return err
		}
		return feast.deployFeastServiceByType(OnlineFeastType)
	}
	return feast.removeFeastServiceByType(OnlineFeastType)
}

func (feast *FeastServices) reconcileOnlineGrpc() error {
	if feast.isOnlineGrpcServer() {
		return feast.deployFeastServiceByType(OnlineGrpcFeastType)
	}
	return feast.removeFeastServiceByType(OnlineGrpcFeastType)
}

func (feast *FeastServices) reconcileRegistry() error {
	if feast.isLocalRegistry() {
		services := feast.Handler.FeatureStore.Status.Applied.Services
		if err := feast.validateRegistryPersistence(services.Registry.Local.Persistence); err != nil {
			return err
		}
		return feast.deployFeastServiceByType(RegistryFeastType)
	}
	return feast.removeFeastServiceByType(RegistryFeastType)
}

func (feast *FeastServices) reconcileUI() error {
	if feast.isUiServer() {
		if err := feast.deployFeastServiceByType(UIFeastType); err != nil {
			return err
		}
		return feast.createRoute(UIFeastType)
	}
	if err := feast.removeFeastServiceByType(UIFeastType); err != nil {
		return err
	}
	return feast.removeRoute(UIFeastType)
}

func (feast *FeastServices) deploySupportServices() error {
	if err := feast.createServiceAccount(); err != nil {
		return err
	}
	if err := feast.deployClient(); err != nil {
		return err
	}
	if err := feast.deployNamespaceRegistry(); err != nil {
		return err
	}
	return feast.deployCronJob()
}

func (feast *FeastServices) validateRegistryPersistence(registryPersistence *feastdevv1.RegistryPersistence) error {
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

func (feast *FeastServices) validateOnlineStorePersistence(onlinePersistence *feastdevv1.OnlineStorePersistence) error {
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

func (feast *FeastServices) validateOfflineStorePersistence(offlinePersistence *feastdevv1.OfflineStorePersistence) error {
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
	hasServerConfig := false
	if feastType == OnlineGrpcFeastType {
		hasServerConfig = feast.isOnlineGrpcServer()
	} else if feast.getServerConfigs(feastType) != nil {
		hasServerConfig = true
	}

	if pvcCreate, shouldCreate := shouldCreatePvc(feast.Handler.FeatureStore, feastType); shouldCreate {
		if err := feast.createPVC(pvcCreate, feastType); err != nil {
			return feast.setFeastServiceCondition(err, feastType)
		}
	} else {
		_ = feast.Handler.DeleteOwnedFeastObj(feast.initPVC(feastType))
	}
	if hasServerConfig {
		// For registry service, handle both gRPC and REST services
		if feastType == RegistryFeastType && feast.isRegistryServer() {
			// Create gRPC service if enabled
			if feast.isRegistryGrpcEnabled() {
				if err := feast.createService(feastType); err != nil {
					return feast.setFeastServiceCondition(err, feastType)
				}
			} else {
				// Delete gRPC service if disabled
				_ = feast.Handler.DeleteOwnedFeastObj(&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      feast.GetFeastServiceName(feastType),
						Namespace: feast.Handler.FeatureStore.Namespace,
					},
				})
			}

			// Create REST service if enabled
			if feast.isRegistryRestEnabled() {
				if err := feast.createRestService(feastType); err != nil {
					return feast.setFeastServiceCondition(err, feastType)
				}
			} else {
				// Delete REST service if disabled
				_ = feast.Handler.DeleteOwnedFeastObj(&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      feast.GetFeastRestServiceName(feastType),
						Namespace: feast.Handler.FeatureStore.Namespace,
					},
				})
			}
		} else {
			// For non-registry services, always create service
			if err := feast.createService(feastType); err != nil {
				return feast.setFeastServiceCondition(err, feastType)
			}
		}
		if err := feast.createDeploymentForType(feastType); err != nil {
			return feast.setFeastServiceCondition(err, feastType)
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
	if err := feast.Handler.DeleteOwnedFeastObj(feast.initFeastDeploy(feastType)); err != nil {
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

func (feast *FeastServices) createDeploymentForType(feastType FeastServiceType) error {
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

func (feast *FeastServices) createPVC(pvcCreate *feastdevv1.PvcCreate, feastType FeastServiceType) error {
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
	cr := feast.Handler.FeatureStore
	replicas := deploy.Spec.Replicas

	deploy.Labels = feast.getFeastTypeLabels(feastType)
	if desiredReplicas := feast.getReplicasForType(feastType); desiredReplicas != nil {
		replicas = desiredReplicas
	}
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
	if err := feast.setPod(&deploy.Spec.Template.Spec, feastType); err != nil {
		return err
	}
	return controllerutil.SetControllerReference(cr, deploy, feast.Handler.Scheme)
}

func (feast *FeastServices) setPod(podSpec *corev1.PodSpec, feastType FeastServiceType) error {
	if err := feast.setContainers(podSpec, feastType); err != nil {
		return err
	}
	feast.mountTlsConfigs(podSpec)
	feast.mountPvcConfigs(podSpec, feastType)
	feast.mountEmptyDirVolumes(podSpec)
	feast.mountUserDefinedVolumes(podSpec)
	feast.applyNodeSelector(podSpec, feastType)

	return nil
}

func (feast *FeastServices) setContainers(podSpec *corev1.PodSpec, feastType FeastServiceType) error {
	fsYamlB64, err := feast.GetServiceFeatureStoreYamlBase64()
	if err != nil {
		return err
	}

	feast.setInitContainer(podSpec, fsYamlB64)
	feast.setContainer(&podSpec.Containers, feastType, fsYamlB64)
	return nil
}

func (feast *FeastServices) setContainer(containers *[]corev1.Container, feastType FeastServiceType, fsYamlB64 string) {
	if feastType == OnlineGrpcFeastType {
		grpcCfg := feast.getOnlineGrpcConfigs()
		if grpcCfg == nil {
			return
		}
		name := string(feastType)
		workingDir := feast.getFeatureRepoDir()
		cmd := feast.getGrpcContainerCommand()
		container := getContainer(name, workingDir, cmd, grpcCfg.ContainerConfigs, fsYamlB64)
		container.Ports = []corev1.ContainerPort{
			{
				Name:          "grpc",
				ContainerPort: feast.getOnlineGrpcPort(),
				Protocol:      corev1.ProtocolTCP,
			},
		}
		probeHandler := feast.getProbeHandler(feastType, &feastdevv1.TlsConfigs{})
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
		return
	}
	if serverConfigs := feast.getServerConfigs(feastType); serverConfigs != nil {
		name := string(feastType)
		workingDir := feast.getFeatureRepoDir()
		cmd := feast.getContainerCommand(feastType)
		container := getContainer(name, workingDir, cmd, serverConfigs.ContainerConfigs, fsYamlB64)
		tls := feast.getTlsConfigs(feastType)
		probeHandler := feast.getProbeHandler(feastType, tls)
		container.Ports = []corev1.ContainerPort{}

		if feastType == RegistryFeastType {
			if feast.isRegistryGrpcEnabled() {
				container.Ports = append(container.Ports, corev1.ContainerPort{
					Name:          name,
					ContainerPort: getTargetPort(feastType, tls),
					Protocol:      corev1.ProtocolTCP,
				})
			}
			if feast.isRegistryRestEnabled() {
				container.Ports = append(container.Ports, corev1.ContainerPort{
					Name:          name + "-rest",
					ContainerPort: getTargetRestPort(feastType, tls),
					Protocol:      corev1.ProtocolTCP,
				})
			}
		} else {
			container.Ports = append(container.Ports, corev1.ContainerPort{
				Name:          name,
				ContainerPort: getTargetPort(feastType, tls),
				Protocol:      corev1.ProtocolTCP,
			})
			if feastType == OnlineFeastType && feast.isMetricsEnabled(feastType) {
				container.Ports = append(container.Ports, corev1.ContainerPort{
					Name:          "metrics",
					ContainerPort: MetricsPort,
					Protocol:      corev1.ProtocolTCP,
				})
			}
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

func getContainer(name, workingDir string, cmd []string, containerConfigs feastdevv1.ContainerConfigs, fsYamlB64 string) *corev1.Container {
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
	if feastType == OnlineGrpcFeastType {
		if grpcCfg := feast.getOnlineGrpcConfigs(); grpcCfg != nil {
			return grpcCfg.VolumeMounts
		}
		return []corev1.VolumeMount{}
	}
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
	deploySettings.Args = append([]string{}, deploySettings.Args...)
	if feastType == OnlineFeastType && feast.isMetricsEnabled(feastType) {
		deploySettings.Args = append([]string{deploySettings.Args[0], "--metrics"}, deploySettings.Args[1:]...)
	}
	targetPort := deploySettings.TargetHttpPort
	tls := feast.getTlsConfigs(feastType)

	if feastType == RegistryFeastType && feast.isRegistryServer() {
		if feast.isRegistryGrpcEnabled() {
			deploySettings.Args = append(deploySettings.Args, "--grpc")
		} else {
			deploySettings.Args = append(deploySettings.Args, "--no-grpc")
		}
		if feast.isRegistryRestEnabled() {
			deploySettings.Args = append(deploySettings.Args, "--rest-api")
			deploySettings.Args = append(deploySettings.Args, "--rest-port", strconv.Itoa(int(getTargetRestPort(feastType, tls))))
		}
	}

	// Add worker configuration options for online store (feast serve)
	if feastType == OnlineFeastType {
		workerConfigs := feast.getWorkerConfigs(feastType)
		if workerConfigs != nil {
			if workerConfigs.Workers != nil {
				deploySettings.Args = append(deploySettings.Args, "--workers", strconv.Itoa(int(*workerConfigs.Workers)))
			}
			if workerConfigs.WorkerConnections != nil {
				deploySettings.Args = append(deploySettings.Args, "--worker-connections", strconv.Itoa(int(*workerConfigs.WorkerConnections)))
			}
			if workerConfigs.MaxRequests != nil {
				deploySettings.Args = append(deploySettings.Args, "--max-requests", strconv.Itoa(int(*workerConfigs.MaxRequests)))
			}
			if workerConfigs.MaxRequestsJitter != nil {
				deploySettings.Args = append(deploySettings.Args, "--max-requests-jitter", strconv.Itoa(int(*workerConfigs.MaxRequestsJitter)))
			}
			if workerConfigs.KeepAliveTimeout != nil {
				deploySettings.Args = append(deploySettings.Args, "--keep-alive-timeout", strconv.Itoa(int(*workerConfigs.KeepAliveTimeout)))
			}
			if workerConfigs.RegistryTTLSeconds != nil {
				deploySettings.Args = append(deploySettings.Args, "--registry_ttl_sec", strconv.Itoa(int(*workerConfigs.RegistryTTLSeconds)))
			}
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

func (feast *FeastServices) getGrpcContainerCommand() []string {
	address := fmt.Sprintf("0.0.0.0:%d", feast.getOnlineGrpcPort())
	cmd := []string{"feast", "listen", "--address", address}
	if grpcCfg := feast.getOnlineGrpcConfigs(); grpcCfg != nil {
		if grpcCfg.MaxWorkers != nil {
			cmd = append(cmd, "--max_workers", strconv.Itoa(int(*grpcCfg.MaxWorkers)))
		}
		if grpcCfg.RegistryTTLSeconds != nil {
			cmd = append(cmd, "--registry_ttl_sec", strconv.Itoa(int(*grpcCfg.RegistryTTLSeconds)))
		}
	}
	return cmd
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
	if feastType == OnlineGrpcFeastType {
		grpcPort := feast.getOnlineGrpcPort()
		svc.Spec = corev1.ServiceSpec{
			Selector: feast.getFeastTypeLabels(feastType),
			Type:     corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "grpc",
					Port:       grpcPort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(int(grpcPort)),
				},
			},
		}
		return controllerutil.SetControllerReference(feast.Handler.FeatureStore, svc, feast.Handler.Scheme)
	}
	if feast.isOpenShiftTls(feastType) {
		if len(svc.Annotations) == 0 {
			svc.Annotations = map[string]string{}
		}

		// For registry services, we need special handling based on which services are enabled
		if feastType == RegistryFeastType && feast.isRegistryServer() {
			grpcEnabled := feast.isRegistryGrpcEnabled()
			restEnabled := feast.isRegistryRestEnabled()

			if grpcEnabled && restEnabled {
				// Both services enabled: Only set TLS annotation on gRPC service to ensure
				// OpenShift creates certificate with gRPC service name as CN (not REST service name)
				// The certificate will include both hostnames as SANs
				if !isRestService {
					grpcSvcName := feast.initFeastSvc(RegistryFeastType).Name
					svc.Annotations["service.beta.openshift.io/serving-cert-secret-name"] = grpcSvcName + tlsNameSuffix

					// Add Subject Alternative Names (SANs) for both services
					grpcHostname := grpcSvcName + "." + svc.Namespace + ".svc.cluster.local"
					restHostname := feast.GetFeastRestServiceName(RegistryFeastType) + "." + svc.Namespace + ".svc.cluster.local"
					svc.Annotations["service.beta.openshift.io/serving-cert-sans"] = grpcHostname + "," + restHostname
				}
				// REST service should not have the annotation - it will use the same certificate
				// from the gRPC service secret (mounted in the pod)
			} else if grpcEnabled && !restEnabled {
				// Only gRPC enabled: Use gRPC service name
				grpcSvcName := feast.initFeastSvc(RegistryFeastType).Name
				svc.Annotations["service.beta.openshift.io/serving-cert-secret-name"] = grpcSvcName + tlsNameSuffix
			} else if !grpcEnabled && restEnabled {
				// Only REST enabled: Use REST service name
				svc.Annotations["service.beta.openshift.io/serving-cert-secret-name"] = svc.Name + tlsNameSuffix
			}
		} else {
			// Standard behavior for non-registry services
			svc.Annotations["service.beta.openshift.io/serving-cert-secret-name"] = svc.Name + tlsNameSuffix
		}
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
		Selector: feast.getFeastTypeLabels(feastType),
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

	if feastType == OnlineFeastType && feast.isMetricsEnabled(feastType) {
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name:       "metrics",
			Port:       MetricsPort,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt(int(MetricsPort)),
		})
	}

	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, svc, feast.Handler.Scheme)
}

// createRestService creates a separate service for the Registry REST API
func (feast *FeastServices) createRestService(feastType FeastServiceType) error {
	if feast.isRegistryServer() {
		if !feast.isRegistryRestEnabled() {
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

func (feast *FeastServices) createNewPVC(pvcCreate *feastdevv1.PvcCreate, feastType FeastServiceType) (*corev1.PersistentVolumeClaim, error) {
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

func (feast *FeastServices) getServerConfigs(feastType FeastServiceType) *feastdevv1.ServerConfigs {
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
	case OnlineGrpcFeastType:
		return nil
	case RegistryFeastType:
		if feast.isRegistryServer() {
			return &appliedServices.Registry.Local.Server.ServerConfigs
		}
	case UIFeastType:
		return appliedServices.UI
	}
	return nil
}

func (feast *FeastServices) getOnlineGrpcConfigs() *feastdevv1.GrpcServerConfigs {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	if appliedServices != nil && appliedServices.OnlineStore != nil {
		return appliedServices.OnlineStore.Grpc
	}
	return nil
}

func (feast *FeastServices) getOnlineGrpcPort() int32 {
	if grpcCfg := feast.getOnlineGrpcConfigs(); grpcCfg != nil && grpcCfg.Port != nil {
		return *grpcCfg.Port
	}
	return DefaultOnlineGrpcPort
}

func (feast *FeastServices) getLogLevelForType(feastType FeastServiceType) *string {
	if serviceConfigs := feast.getServerConfigs(feastType); serviceConfigs != nil {
		return serviceConfigs.LogLevel
	}
	return nil
}

func (feast *FeastServices) getWorkerConfigs(feastType FeastServiceType) *feastdevv1.WorkerConfigs {
	if serviceConfigs := feast.getServerConfigs(feastType); serviceConfigs != nil {
		return serviceConfigs.WorkerConfigs
	}
	return nil
}

func (feast *FeastServices) getReplicasForType(feastType FeastServiceType) *int32 {
	if feastType == OnlineGrpcFeastType {
		if grpcCfg := feast.getOnlineGrpcConfigs(); grpcCfg != nil {
			return grpcCfg.Replicas
		}
		return nil
	}
	if serviceConfigs := feast.getServerConfigs(feastType); serviceConfigs != nil {
		return serviceConfigs.Replicas
	}
	return nil
}

func (feast *FeastServices) isMetricsEnabled(feastType FeastServiceType) bool {
	if feastType != OnlineFeastType {
		return false
	}

	if serviceConfigs := feast.getServerConfigs(feastType); serviceConfigs != nil && serviceConfigs.Metrics != nil {
		return *serviceConfigs.Metrics
	}

	return false
}

func (feast *FeastServices) getNodeSelectorForType(feastType FeastServiceType) *map[string]string {
	if feastType == OnlineGrpcFeastType {
		if grpcCfg := feast.getOnlineGrpcConfigs(); grpcCfg != nil {
			return grpcCfg.ContainerConfigs.OptionalCtrConfigs.NodeSelector
		}
		return nil
	}
	if serviceConfigs := feast.getServerConfigs(feastType); serviceConfigs != nil {
		return serviceConfigs.ContainerConfigs.OptionalCtrConfigs.NodeSelector
	}
	return nil
}

func (feast *FeastServices) applyNodeSelector(podSpec *corev1.PodSpec, feastType FeastServiceType) {
	selector := feast.getNodeSelectorForType(feastType)
	if selector == nil || len(*selector) == 0 {
		return
	}

	// Merge with any existing node selectors (from ops team or other sources)
	finalNodeSelector := feast.mergeNodeSelectors(podSpec.NodeSelector, *selector)
	podSpec.NodeSelector = finalNodeSelector
}

// mergeNodeSelectors merges existing and operator node selectors
// Existing selectors are preserved, operator selectors can override existing keys
func (feast *FeastServices) mergeNodeSelectors(existing, operator map[string]string) map[string]string {
	merged := make(map[string]string)

	// Start with existing selectors (from ops team or other sources)
	for k, v := range existing {
		merged[k] = v
	}

	// Add/override with operator selectors
	for k, v := range operator {
		merged[k] = v
	}

	return merged
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

func (feast *FeastServices) GetDeployment(feastType FeastServiceType) (appsv1.Deployment, error) {
	deployment := appsv1.Deployment{}
	obj := feast.GetObjectMetaType(feastType)
	err := feast.Handler.Get(feast.Handler.Context, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, &deployment)
	return deployment, err
}

// GetFeastServiceName returns the feast service object name based on service type
func GetFeastServiceName(featureStore *feastdevv1.FeatureStore, feastType FeastServiceType) string {
	return GetFeastName(featureStore) + "-" + string(feastType)
}

func GetFeastName(featureStore *feastdevv1.FeatureStore) string {
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
	feast.Handler.FeatureStore.Status.ServiceHostnames = feastdevv1.ServiceHostnames{}
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
	if feast.isOnlineGrpcServer() {
		objMeta := feast.initFeastSvc(OnlineGrpcFeastType)
		grpcPort := feast.getOnlineGrpcPort()
		feast.Handler.FeatureStore.Status.ServiceHostnames.OnlineStoreGrpc = objMeta.Name + "." + objMeta.Namespace + domain +
			strconv.Itoa(int(grpcPort))
	}
	if feast.isRegistryServer() {
		objMeta := feast.initFeastSvc(RegistryFeastType)
		feast.Handler.FeatureStore.Status.ServiceHostnames.Registry = objMeta.Name + "." + objMeta.Namespace + domain +
			getPortStr(feast.Handler.FeatureStore.Status.Applied.Services.Registry.Local.Server.TLS)
		if feast.isRegistryRestEnabled() {
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
			apimeta.IsStatusConditionTrue(remoteFeast.Handler.FeatureStore.Status.Conditions, feastdevv1.RegistryReadyType) &&
			len(remoteFeast.Handler.FeatureStore.Status.ServiceHostnames.Registry) > 0 {
			// Check if gRPC server is enabled
			if !remoteFeast.isRegistryGrpcEnabled() {
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
		remoteFeastObj := &feastdevv1.FeatureStore{}
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

func (feast *FeastServices) isOnlineGrpcServer() bool {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.OnlineStore != nil && appliedServices.OnlineStore.Grpc != nil
}

func (feast *FeastServices) isOnlineStore() bool {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.OnlineStore != nil
}

func (feast *FeastServices) noLocalCoreServerConfigured() bool {
	return !(feast.isRegistryServer() || feast.isOnlineServer() || feast.isOnlineGrpcServer() || feast.isOfflineServer())
}

func (feast *FeastServices) isUiServer() bool {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.UI != nil
}

func (feast *FeastServices) GetDeploymentTypes() []FeastServiceType {
	deployments := []FeastServiceType{}
	if feast.isOfflineServer() {
		deployments = append(deployments, OfflineFeastType)
	}
	if feast.isOnlineServer() {
		deployments = append(deployments, OnlineFeastType)
	}
	if feast.isOnlineGrpcServer() {
		deployments = append(deployments, OnlineGrpcFeastType)
	}
	if feast.isRegistryServer() {
		deployments = append(deployments, RegistryFeastType)
	}
	if feast.isUiServer() {
		deployments = append(deployments, UIFeastType)
	}
	return deployments
}

func (feast *FeastServices) initFeastDeploy(feastType FeastServiceType) *appsv1.Deployment {
	deploy := &appsv1.Deployment{
		ObjectMeta: feast.GetObjectMetaType(feastType),
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

func applyCtrConfigs(container *corev1.Container, containerConfigs feastdevv1.ContainerConfigs) {
	if containerConfigs.DefaultCtrConfigs.Image != nil {
		container.Image = *containerConfigs.DefaultCtrConfigs.Image
	}
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

func (feast *FeastServices) mountPvcConfigs(podSpec *corev1.PodSpec, feastType FeastServiceType) {
	if pvcConfig, hasPvcConfig := hasPvcConfig(feast.Handler.FeatureStore, feastType); hasPvcConfig {
		feast.mountPvcConfig(podSpec, pvcConfig, feastType)
	}
}

func (feast *FeastServices) mountPvcConfig(podSpec *corev1.PodSpec, pvcConfig *feastdevv1.PvcConfig, feastType FeastServiceType) {
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

func getTargetPort(feastType FeastServiceType, tls *feastdevv1.TlsConfigs) int32 {
	if tls.IsTLS() {
		return FeastServiceConstants[feastType].TargetHttpsPort
	}
	return FeastServiceConstants[feastType].TargetHttpPort
}

func getTargetRestPort(feastType FeastServiceType, tls *feastdevv1.TlsConfigs) int32 {
	if tls.IsTLS() {
		return FeastServiceConstants[feastType].TargetRestHttpsPort
	}
	return FeastServiceConstants[feastType].TargetRestHttpPort
}

func (feast *FeastServices) getProbeHandler(feastType FeastServiceType, tls *feastdevv1.TlsConfigs) corev1.ProbeHandler {
	targetPort := getTargetPort(feastType, tls)

	if feastType == OnlineGrpcFeastType {
		targetPort = feast.getOnlineGrpcPort()
		return corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt(int(targetPort)),
			},
		}
	}

	if feastType == RegistryFeastType {
		if feast.isRegistryGrpcEnabled() {
			return corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt(int(targetPort)),
				},
			}
		}
		if feast.isRegistryRestEnabled() {
			targetPort = getTargetRestPort(feastType, tls)
			probeHandler := corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Port: intstr.FromInt(int(targetPort)),
				},
			}
			if tls.IsTLS() {
				probeHandler.HTTPGet.Scheme = corev1.URISchemeHTTPS
			}
			return probeHandler
		}
	}
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

// isRegistryGrpcEnabled checks if gRPC is enabled for registry service
func (feast *FeastServices) isRegistryGrpcEnabled() bool {
	if feast.isRegistryServer() {
		registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry
		return registry.Local.Server.GRPC != nil && *registry.Local.Server.GRPC
	}
	return false
}

// isRegistryRestEnabled checks if REST API is enabled for registry service
func (feast *FeastServices) isRegistryRestEnabled() bool {
	if feast.isRegistryServer() {
		registry := feast.Handler.FeatureStore.Status.Applied.Services.Registry
		return registry.Local.Server.RestAPI != nil && *registry.Local.Server.RestAPI
	}
	return false
}
