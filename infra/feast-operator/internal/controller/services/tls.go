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

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func (feast *FeastServices) setTlsDefaults() error {
	if err := feast.setOpenshiftTls(); err != nil {
		return err
	}
	appliedServices := feast.FeatureStore.Status.Applied.Services
	if feast.isOfflinStore() && appliedServices.OfflineStore.TLS != nil {
		tlsDefaults(&appliedServices.OfflineStore.TLS.TlsConfigs)
	}
	if feast.isOnlinStore() {
		tlsDefaults(appliedServices.OnlineStore.TLS)
	}
	if feast.isLocalRegistry() {
		tlsDefaults(appliedServices.Registry.Local.TLS)
	}
	return nil
}

func (feast *FeastServices) setOpenshiftTls() error {
	appliedServices := feast.FeatureStore.Status.Applied.Services
	tlsConfigs := &feastdevv1alpha1.TlsConfigs{
		SecretRef: &corev1.LocalObjectReference{},
	}
	if feast.offlineOpenshiftTls() {
		appliedServices.OfflineStore.TLS = &feastdevv1alpha1.OfflineTlsConfigs{
			TlsConfigs: *tlsConfigs,
		}
		appliedServices.OfflineStore.TLS.TlsConfigs.SecretRef.Name = feast.initFeastSvc(OfflineFeastType).Name + tlsNameSuffix
	}
	if feast.onlineOpenshiftTls() {
		appliedServices.OnlineStore.TLS = tlsConfigs
		appliedServices.OnlineStore.TLS.SecretRef.Name = feast.initFeastSvc(OnlineFeastType).Name + tlsNameSuffix
	}
	if feast.localRegistryOpenshiftTls() {
		appliedServices.Registry.Local.TLS = tlsConfigs
		appliedServices.Registry.Local.TLS.SecretRef.Name = feast.initFeastSvc(RegistryFeastType).Name + tlsNameSuffix
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
	if feast.offlineOpenshiftTls() || feast.onlineOpenshiftTls() || feast.localRegistryOpenshiftTls() {
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
	}
	return
}

func (feast *FeastServices) getTlsConfigs(feastType FeastServiceType) (tls *feastdevv1alpha1.TlsConfigs) {
	appliedServices := feast.FeatureStore.Status.Applied.Services
	switch feastType {
	case OfflineFeastType:
		if feast.isOfflinStore() && appliedServices.OfflineStore.TLS != nil {
			tls = &appliedServices.OfflineStore.TLS.TlsConfigs
		}
	case OnlineFeastType:
		if feast.isOnlinStore() {
			tls = appliedServices.OnlineStore.TLS
		}
	case RegistryFeastType:
		if feast.isLocalRegistry() {
			tls = appliedServices.Registry.Local.TLS
		}
	}
	return
}

// True if running in an openshift cluster and Tls not configured in the service Spec
func (feast *FeastServices) offlineOpenshiftTls() bool {
	return isOpenShift &&
		feast.isOfflinStore() && feast.FeatureStore.Spec.Services.OfflineStore.TLS == nil
}

// True if running in an openshift cluster and Tls not configured in the service Spec
func (feast *FeastServices) onlineOpenshiftTls() bool {
	return isOpenShift &&
		feast.isOnlinStore() && feast.FeatureStore.Spec.Services.OnlineStore.TLS == nil
}

// True if running in an openshift cluster and Tls not configured in the service Spec
func (feast *FeastServices) localRegistryOpenshiftTls() bool {
	return isOpenShift &&
		feast.isLocalRegistry() &&
		(feast.FeatureStore.Spec.Services == nil ||
			feast.FeatureStore.Spec.Services.Registry == nil ||
			feast.FeatureStore.Spec.Services.Registry.Local == nil ||
			feast.FeatureStore.Spec.Services.Registry.Local.TLS == nil)
}

// True if running in an openshift cluster, and using a remote registry in the same cluster, with no remote Tls set in the service Spec
func (feast *FeastServices) remoteRegistryOpenshiftTls() (bool, error) {
	if isOpenShift && feast.isRemoteRegistry() {
		remoteFeast, err := feast.getRemoteRegistryFeastHandler()
		if err != nil {
			return false, err
		}
		return (remoteFeast != nil && remoteFeast.localRegistryOpenshiftTls() &&
				feast.FeatureStore.Spec.Services.Registry.Remote.TLS == nil),
			nil
	}
	return false, nil
}

func (feast *FeastServices) offlineTls() bool {
	return feast.isOfflinStore() &&
		feast.FeatureStore.Status.Applied.Services.OfflineStore.TLS != nil &&
		(&feast.FeatureStore.Status.Applied.Services.OfflineStore.TLS.TlsConfigs).IsTLS()
}

func (feast *FeastServices) localRegistryTls() bool {
	return localRegistryTls(feast.FeatureStore)
}

func (feast *FeastServices) remoteRegistryTls() bool {
	return remoteRegistryTls(feast.FeatureStore)
}

func (feast *FeastServices) mountRegistryClientTls(podSpec *corev1.PodSpec) {
	if podSpec != nil {
		if feast.localRegistryTls() {
			feast.mountTlsConfig(RegistryFeastType, podSpec)
		} else if feast.remoteRegistryTls() {
			mountTlsRemoteRegistryConfig(RegistryFeastType, podSpec,
				feast.FeatureStore.Status.Applied.Services.Registry.Remote.TLS)
		}
	}
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
		container := &podSpec.Containers[0]
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: GetTlsPath(feastType),
			ReadOnly:  true,
		})
	}
}

func mountTlsRemoteRegistryConfig(feastType FeastServiceType, podSpec *corev1.PodSpec, tls *feastdevv1alpha1.TlsRemoteRegistryConfigs) {
	if tls != nil {
		volName := string(feastType) + tlsNameSuffix
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: tls.ConfigMapRef,
				},
			},
		})
		container := &podSpec.Containers[0]
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: GetTlsPath(feastType),
			ReadOnly:  true,
		})
	}
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
	return isLocalRegistry(featureStore) && featureStore.Status.Applied.Services.Registry.Local.TLS.IsTLS()
}

func remoteRegistryTls(featureStore *feastdevv1alpha1.FeatureStore) bool {
	return isRemoteRegistry(featureStore) && featureStore.Status.Applied.Services.Registry.Remote.TLS != nil
}

func GetTlsPath(feastType FeastServiceType) string {
	return tlsPath + string(feastType) + "/"
}
