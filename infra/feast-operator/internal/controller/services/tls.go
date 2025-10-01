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
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/controller-runtime/pkg/log"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func (feast *FeastServices) setTlsDefaults() error {
	if err := feast.setOpenshiftTls(); err != nil {
		return err
	}
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	if feast.isOfflineServer() {
		tlsDefaults(appliedServices.OfflineStore.Server.TLS)
	}
	if feast.isOnlineServer() {
		tlsDefaults(appliedServices.OnlineStore.Server.TLS)
	}
	if feast.isRegistryServer() {
		tlsDefaults(appliedServices.Registry.Local.Server.TLS)
	}
	if feast.isUiServer() {
		tlsDefaults(appliedServices.UI.TLS)
	}
	return nil
}

func (feast *FeastServices) setOpenshiftTls() error {
	appliedServices := feast.Handler.FeatureStore.Status.Applied.Services
	if feast.offlineOpenshiftTls() {
		appliedServices.OfflineStore.Server.TLS = &feastdevv1alpha1.TlsConfigs{
			SecretRef: &corev1.LocalObjectReference{
				Name: feast.initFeastSvc(OfflineFeastType).Name + tlsNameSuffix,
			},
		}
	}
	if feast.onlineOpenshiftTls() {
		appliedServices.OnlineStore.Server.TLS = &feastdevv1alpha1.TlsConfigs{
			SecretRef: &corev1.LocalObjectReference{
				Name: feast.initFeastSvc(OnlineFeastType).Name + tlsNameSuffix,
			},
		}
	}
	if feast.uiOpenshiftTls() {
		appliedServices.UI.TLS = &feastdevv1alpha1.TlsConfigs{
			SecretRef: &corev1.LocalObjectReference{
				Name: feast.initFeastSvc(UIFeastType).Name + tlsNameSuffix,
			},
		}
	}
	if feast.localRegistryOpenshiftTls() {
		grpcEnabled := feast.isRegistryGrpcEnabled()
		restEnabled := feast.isRegistryRestEnabled()

		if grpcEnabled && restEnabled {
			// Both services enabled: Use gRPC service name as primary certificate
			// The certificate will include both hostnames as SANs via service annotations
			appliedServices.Registry.Local.Server.TLS = &feastdevv1alpha1.TlsConfigs{
				SecretRef: &corev1.LocalObjectReference{
					Name: feast.initFeastSvc(RegistryFeastType).Name + tlsNameSuffix,
				},
			}
		} else if grpcEnabled && !restEnabled {
			// Only gRPC enabled: Use gRPC service name
			appliedServices.Registry.Local.Server.TLS = &feastdevv1alpha1.TlsConfigs{
				SecretRef: &corev1.LocalObjectReference{
					Name: feast.initFeastSvc(RegistryFeastType).Name + tlsNameSuffix,
				},
			}
		} else if !grpcEnabled && restEnabled {
			// Only REST enabled: Use REST service name
			appliedServices.Registry.Local.Server.TLS = &feastdevv1alpha1.TlsConfigs{
				SecretRef: &corev1.LocalObjectReference{
					Name: feast.initFeastRestSvc(RegistryFeastType).Name + tlsNameSuffix,
				},
			}
		}
	} else if remote, err := feast.remoteRegistryOpenshiftTls(); remote {
		// if the remote registry reference is using openshift's service serving certificates, we can use the injected service CA bundle configMap
		if appliedServices.Registry.Remote.TLS == nil {
			appliedServices.Registry.Remote.TLS = &feastdevv1alpha1.TlsRemoteRegistryConfigs{
				ConfigMapRef: corev1.LocalObjectReference{
					Name: feast.initCaConfigMap().Name,
				},
				CertName: "service-ca.crt",
			}
		}
	} else if err != nil {
		return err
	}
	return nil
}

func (feast *FeastServices) checkOpenshiftTls() (bool, error) {
	if feast.offlineOpenshiftTls() || feast.onlineOpenshiftTls() || feast.localRegistryOpenshiftTls() || feast.uiOpenshiftTls() {
		return true, nil
	}
	return feast.remoteRegistryOpenshiftTls()
}

func (feast *FeastServices) isOpenShiftTls(feastType FeastServiceType) (isOpenShift bool) {
	switch feastType {
	case OfflineFeastType:
		isOpenShift = feast.offlineOpenshiftTls()
	case OnlineFeastType:
		isOpenShift = feast.onlineOpenshiftTls()
	case RegistryFeastType:
		isOpenShift = feast.localRegistryOpenshiftTls()
	case UIFeastType:
		isOpenShift = feast.uiOpenshiftTls()
	}

	return
}

func (feast *FeastServices) getTlsConfigs(feastType FeastServiceType) *feastdevv1alpha1.TlsConfigs {
	if serviceConfigs := feast.getServerConfigs(feastType); serviceConfigs != nil {
		return serviceConfigs.TLS
	}
	return nil
}

// True if running in an openshift cluster and Tls not configured in the service Spec
func (feast *FeastServices) offlineOpenshiftTls() bool {
	return isOpenShift &&
		feast.isOfflineServer() && feast.Handler.FeatureStore.Spec.Services.OfflineStore.Server.TLS == nil
}

// True if running in an openshift cluster and Tls not configured in the service Spec
func (feast *FeastServices) onlineOpenshiftTls() bool {
	return isOpenShift &&
		feast.isOnlineServer() &&
		(feast.Handler.FeatureStore.Spec.Services == nil ||
			feast.Handler.FeatureStore.Spec.Services.OnlineStore == nil ||
			feast.Handler.FeatureStore.Spec.Services.OnlineStore.Server == nil ||
			feast.Handler.FeatureStore.Spec.Services.OnlineStore.Server.TLS == nil)
}

// True if running in an openshift cluster and Tls not configured in the service Spec
func (feast *FeastServices) uiOpenshiftTls() bool {
	return isOpenShift &&
		feast.isUiServer() && feast.Handler.FeatureStore.Spec.Services.UI.TLS == nil
}

// True if running in an openshift cluster and Tls not configured in the service Spec
func (feast *FeastServices) localRegistryOpenshiftTls() bool {
	return isOpenShift &&
		feast.isRegistryServer() && feast.Handler.FeatureStore.Spec.Services.Registry.Local.Server.TLS == nil
}

// True if running in an openshift cluster, and using a remote registry in the same cluster, with no remote Tls set in the service Spec
func (feast *FeastServices) remoteRegistryOpenshiftTls() (bool, error) {
	if isOpenShift && feast.isRemoteRegistry() {
		remoteFeast, err := feast.getRemoteRegistryFeastHandler()
		if err != nil {
			return false, err
		}
		return (remoteFeast != nil && remoteFeast.localRegistryOpenshiftTls() &&
				feast.Handler.FeatureStore.Spec.Services.Registry.Remote.TLS == nil),
			nil
	}
	return false, nil
}

func (feast *FeastServices) localRegistryTls() bool {
	return localRegistryTls(feast.Handler.FeatureStore)
}

func (feast *FeastServices) remoteRegistryTls() bool {
	return remoteRegistryTls(feast.Handler.FeatureStore)
}

func (feast *FeastServices) mountRegistryClientTls(podSpec *corev1.PodSpec) {
	if podSpec != nil {
		if feast.localRegistryTls() {
			feast.mountTlsConfig(RegistryFeastType, podSpec)
		} else if feast.remoteRegistryTls() {
			mountTlsRemoteRegistryConfig(podSpec,
				feast.Handler.FeatureStore.Status.Applied.Services.Registry.Remote.TLS)
		}
	}
}

func (feast *FeastServices) mountTlsConfigs(podSpec *corev1.PodSpec) {
	// how deal w/ client deployment tls mounts when the time comes? new function?
	feast.mountRegistryClientTls(podSpec)
	feast.mountTlsConfig(OfflineFeastType, podSpec)
	feast.mountTlsConfig(OnlineFeastType, podSpec)
	feast.mountTlsConfig(UIFeastType, podSpec)
	feast.mountCustomCABundle(podSpec)
}

func (feast *FeastServices) mountTlsConfig(feastType FeastServiceType, podSpec *corev1.PodSpec) {
	tls := feast.getTlsConfigs(feastType)
	if tls.IsTLS() && podSpec != nil {
		volName := string(feastType) + tlsNameSuffix
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: tls.SecretRef.Name,
				},
			},
		})
		if i, container := getContainerByType(feastType, *podSpec); container != nil {
			podSpec.Containers[i].VolumeMounts = append(podSpec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: GetTlsPath(feastType),
				ReadOnly:  true,
			})
		}
	}
}

func mountTlsRemoteRegistryConfig(podSpec *corev1.PodSpec, tls *feastdevv1alpha1.TlsRemoteRegistryConfigs) {
	if tls != nil {
		volName := string(RegistryFeastType) + tlsNameSuffix
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: tls.ConfigMapRef,
				},
			},
		})
		for i := range podSpec.Containers {
			podSpec.Containers[i].VolumeMounts = append(podSpec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: GetTlsPath(RegistryFeastType),
				ReadOnly:  true,
			})
		}
	}
}

func (feast *FeastServices) mountCustomCABundle(podSpec *corev1.PodSpec) {
	customCaBundle := feast.GetCustomCertificatesBundle()
	if customCaBundle.IsDefined {
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: customCaBundle.VolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: customCaBundle.ConfigMapName},
				},
			},
		})

		for i := range podSpec.Containers {
			podSpec.Containers[i].VolumeMounts = append(podSpec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name:      customCaBundle.VolumeName,
				MountPath: tlsPathCustomCABundle,
				ReadOnly:  true,
				SubPath:   "ca-bundle.crt",
			})
		}

		log.FromContext(feast.Handler.Context).Info("Mounted custom CA bundle ConfigMap to Feast pods.")
	}
}

// GetCustomCertificatesBundle retrieves the custom CA bundle ConfigMap if it exists when deployed with RHOAI or ODH
func (feast *FeastServices) GetCustomCertificatesBundle() CustomCertificatesBundle {
	var customCertificatesBundle CustomCertificatesBundle
	configMapList := &corev1.ConfigMapList{}
	labelSelector := client.MatchingLabels{caBundleAnnotation: "true"}

	err := feast.Handler.Client.List(
		feast.Handler.Context,
		configMapList,
		client.InNamespace(feast.Handler.FeatureStore.Namespace),
		labelSelector,
	)
	if err != nil {
		log.FromContext(feast.Handler.Context).Error(err, "Error listing ConfigMaps. Not using custom CA bundle.")
		return customCertificatesBundle
	}

	// Check  if caBundleName exists
	for _, cm := range configMapList.Items {
		if cm.Name == caBundleName {
			log.FromContext(feast.Handler.Context).Info("Found trusted CA bundle ConfigMap. Using custom CA bundle.")
			customCertificatesBundle.IsDefined = true
			customCertificatesBundle.VolumeName = caBundleName
			customCertificatesBundle.ConfigMapName = caBundleName
			return customCertificatesBundle
		}
	}

	log.FromContext(feast.Handler.Context).Info("CA bundle ConfigMap named '" + caBundleName + "' not found. Not using custom CA bundle.")
	return customCertificatesBundle
}

func getPortStr(tls *feastdevv1alpha1.TlsConfigs) string {
	if tls.IsTLS() {
		return strconv.Itoa(HttpsPort)
	}
	return strconv.Itoa(HttpPort)
}

func tlsDefaults(tls *feastdevv1alpha1.TlsConfigs) {
	if tls.IsTLS() {
		if len(tls.SecretKeyNames.TlsCrt) == 0 {
			tls.SecretKeyNames.TlsCrt = "tls.crt"
		}
		if len(tls.SecretKeyNames.TlsKey) == 0 {
			tls.SecretKeyNames.TlsKey = "tls.key"
		}
	}
}

func localRegistryTls(featureStore *feastdevv1alpha1.FeatureStore) bool {
	return IsRegistryServer(featureStore) && featureStore.Status.Applied.Services.Registry.Local.Server.TLS.IsTLS()
}

func remoteRegistryTls(featureStore *feastdevv1alpha1.FeatureStore) bool {
	return isRemoteRegistry(featureStore) && featureStore.Status.Applied.Services.Registry.Remote.TLS != nil
}

func GetTlsPath(feastType FeastServiceType) string {
	return tlsPath + string(feastType) + "/"
}
