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
	"path"
	"strings"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
)

// GetServiceFeatureStoreYamlBase64 returns a base64 encoded feature_store.yaml config for the feast service
func (feast *FeastServices) GetServiceFeatureStoreYamlBase64(feastType FeastServiceType) (string, error) {
	fsYaml, err := feast.getServiceFeatureStoreYaml(feastType)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(fsYaml), nil
}

func (feast *FeastServices) getServiceFeatureStoreYaml(feastType FeastServiceType) ([]byte, error) {
	repoConfig, err := feast.getServiceRepoConfig(feastType)
	if err != nil {
		return nil, err
	}
	return yaml.Marshal(repoConfig)
}

func (feast *FeastServices) getServiceRepoConfig(feastType FeastServiceType) (RepoConfig, error) {
	return getServiceRepoConfig(feastType, feast.FeatureStore)
}

func getServiceRepoConfig(feastType FeastServiceType, featureStore *feastdevv1alpha1.FeatureStore) (RepoConfig, error) {
	appliedSpec := featureStore.Status.Applied

	repoConfig := getClientRepoConfig(featureStore)
	isLocalRegistry := isLocalRegistry(featureStore)
	if appliedSpec.Services != nil {
		services := appliedSpec.Services
		switch feastType {
		case OfflineFeastType:
			// Offline server has an `offline_store` section and a remote `registry`
			if services.OfflineStore != nil {
				fileType := string(OfflineDaskConfigType)
				if services.OfflineStore.Persistence != nil &&
					services.OfflineStore.Persistence.FilePersistence != nil &&
					len(services.OfflineStore.Persistence.FilePersistence.Type) > 0 {
					fileType = services.OfflineStore.Persistence.FilePersistence.Type
				}

				repoConfig.OfflineStore = OfflineStoreConfig{
					Type: OfflineConfigType(fileType),
				}
				repoConfig.OnlineStore = OnlineStoreConfig{}
			}
		case OnlineFeastType:
			// Online server has an `online_store` section, a remote `registry` and a remote `offline_store`
			if services.OnlineStore != nil {
				path := DefaultOnlineStoreEphemeralPath
				if services.OnlineStore.Persistence != nil && services.OnlineStore.Persistence.FilePersistence != nil {
					filePersistence := services.OnlineStore.Persistence.FilePersistence
					path = getActualPath(filePersistence.Path, filePersistence.PvcConfig)
				}

				repoConfig.OnlineStore = OnlineStoreConfig{
					Type: OnlineSqliteConfigType,
					Path: path,
				}
			}
		case RegistryFeastType:
			// Registry server only has a `registry` section
			if isLocalRegistry {
				path := DefaultRegistryEphemeralPath
				if services != nil && services.Registry != nil && services.Registry.Local != nil &&
					services.Registry.Local.Persistence != nil && services.Registry.Local.Persistence.FilePersistence != nil {
					filePersistence := services.Registry.Local.Persistence.FilePersistence
					path = getActualPath(filePersistence.Path, filePersistence.PvcConfig)
				}
				repoConfig.Registry = RegistryConfig{
					RegistryType: RegistryFileConfigType,
					Path:         path,
				}
				repoConfig.OfflineStore = OfflineStoreConfig{}
				repoConfig.OnlineStore = OnlineStoreConfig{}
			}
		}
	}

	return repoConfig, nil
}

func (feast *FeastServices) getClientFeatureStoreYaml() ([]byte, error) {
	return yaml.Marshal(getClientRepoConfig(feast.FeatureStore))
}

func getClientRepoConfig(featureStore *feastdevv1alpha1.FeatureStore) RepoConfig {
	status := featureStore.Status
	clientRepoConfig := RepoConfig{
		Project:                       status.Applied.FeastProject,
		Provider:                      LocalProviderType,
		EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
	}
	if len(status.ServiceHostnames.OfflineStore) > 0 {
		clientRepoConfig.OfflineStore = OfflineStoreConfig{
			Type: OfflineRemoteConfigType,
			Host: strings.Split(status.ServiceHostnames.OfflineStore, ":")[0],
			Port: HttpPort,
		}
	}
	if len(status.ServiceHostnames.OnlineStore) > 0 {
		clientRepoConfig.OnlineStore = OnlineStoreConfig{
			Type: OnlineRemoteConfigType,
			Path: strings.ToLower(string(corev1.URISchemeHTTP)) + "://" + status.ServiceHostnames.OnlineStore,
		}
	}
	if len(status.ServiceHostnames.Registry) > 0 {
		clientRepoConfig.Registry = RegistryConfig{
			RegistryType: RegistryRemoteConfigType,
			Path:         status.ServiceHostnames.Registry,
		}
	}
	return clientRepoConfig
}

func getActualPath(filePath string, pvcConfig *feastdevv1alpha1.PvcConfig) string {
	if pvcConfig == nil {
		return filePath
	}
	return path.Join(pvcConfig.MountPath, filePath)
}
