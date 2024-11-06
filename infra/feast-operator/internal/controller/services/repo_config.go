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
	isLocalRegistry := IsLocalRegistry(featureStore)
	if appliedSpec.Services != nil {
		// Offline server has an `offline_store` section and a remote `registry`
		if feastType == OfflineFeastType && appliedSpec.Services.OfflineStore != nil {
			fileType := string(OfflineDaskConfigType)
			if appliedSpec.Services.OfflineStore.Persistence != nil && appliedSpec.Services.OfflineStore.Persistence.FilePersistence != nil {
				fileType = appliedSpec.Services.OfflineStore.Persistence.FilePersistence.Type
			}

			repoConfig.OfflineStore = OfflineStoreConfig{}
			var err error
			repoConfig.OfflineStore.Type, err = ParseOfflineConfigType(fileType)
			if err != nil {
				return repoConfig, err
			}
			repoConfig.OnlineStore = OnlineStoreConfig{}
		}
		// Online server has an `online_store` section, a remote `registry` and a remote `offline_store`
		if feastType == OnlineFeastType && appliedSpec.Services.OnlineStore != nil {
			path := DefaultOnlinePath
			if appliedSpec.Services.OnlineStore.Persistence != nil && appliedSpec.Services.OnlineStore.Persistence.FilePersistence != nil {
				path = appliedSpec.Services.OnlineStore.Persistence.FilePersistence.Path
			}

			repoConfig.OnlineStore = OnlineStoreConfig{
				Type: OnlineSqliteConfigType,
				Path: path,
			}
		}
		// Registry server only has a `registry` section
		if feastType == RegistryFeastType && isLocalRegistry {
			path := DefaultRegistryPath
			if appliedSpec.Services != nil && appliedSpec.Services.Registry != nil && appliedSpec.Services.Registry.Local != nil &&
				appliedSpec.Services.Registry.Local.Persistence != nil && appliedSpec.Services.Registry.Local.Persistence.FilePersistence != nil {
				path = appliedSpec.Services.Registry.Local.Persistence.FilePersistence.Path
			}
			repoConfig.Registry = RegistryConfig{
				RegistryType: RegistryFileConfigType,
				Path:         path,
			}
			repoConfig.OfflineStore = OfflineStoreConfig{}
			repoConfig.OnlineStore = OnlineStoreConfig{}
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
