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
	"fmt"
	"path"
	"strings"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"gopkg.in/yaml.v3"
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
	return getServiceRepoConfig(feastType, feast.Handler.FeatureStore, feast.extractConfigFromSecret)
}

func getServiceRepoConfig(feastType FeastServiceType, featureStore *feastdevv1alpha1.FeatureStore, secretExtractionFunc func(secretRef string, secretKeyName string) (map[string]interface{}, error)) (RepoConfig, error) {
	appliedSpec := featureStore.Status.Applied

	repoConfig := getClientRepoConfig(featureStore)
	if appliedSpec.Services != nil {
		services := appliedSpec.Services

		switch feastType {
		case OfflineFeastType:
			// Offline server has an `offline_store` section and a remote `registry`
			if services.OfflineStore != nil {
				err := setRepoConfigOffline(services, secretExtractionFunc, &repoConfig)
				if err != nil {
					return repoConfig, err
				}
			}
		case OnlineFeastType:
			// Online server has an `online_store` section, a remote `registry` and a remote `offline_store`
			if services.OnlineStore != nil {
				err := setRepoConfigOnline(services, secretExtractionFunc, &repoConfig)
				if err != nil {
					return repoConfig, err
				}
			}
		case RegistryFeastType:
			// Registry server only has a `registry` section
			if IsLocalRegistry(featureStore) {
				err := setRepoConfigRegistry(services, secretExtractionFunc, &repoConfig)
				if err != nil {
					return repoConfig, err
				}
			}
		}
	}

	return repoConfig, nil
}

func setRepoConfigRegistry(services *feastdevv1alpha1.FeatureStoreServices, secretExtractionFunc func(secretRef string, secretKeyName string) (map[string]interface{}, error), repoConfig *RepoConfig) error {
	repoConfig.Registry = RegistryConfig{}
	repoConfig.Registry.Path = DefaultRegistryEphemeralPath
	registryPersistence := services.Registry.Local.Persistence

	if registryPersistence != nil {
		filePersistence := registryPersistence.FilePersistence
		dbPersistence := registryPersistence.DBPersistence

		if filePersistence != nil {
			repoConfig.Registry.RegistryType = RegistryFileConfigType
			repoConfig.Registry.Path = getActualPath(filePersistence.Path, filePersistence.PvcConfig)
			repoConfig.Registry.S3AdditionalKwargs = filePersistence.S3AdditionalKwargs
		} else if dbPersistence != nil && len(dbPersistence.Type) > 0 {
			repoConfig.Registry.Path = ""
			repoConfig.Registry.RegistryType = RegistryConfigType(dbPersistence.Type)
			secretKeyName := dbPersistence.SecretKeyName
			if len(secretKeyName) == 0 {
				secretKeyName = string(repoConfig.Registry.RegistryType)
			}
			parametersMap, err := secretExtractionFunc(dbPersistence.SecretRef.Name, secretKeyName)
			if err != nil {
				return err
			}

			err = mergeStructWithDBParametersMap(&parametersMap, &repoConfig.Registry)
			if err != nil {
				return err
			}

			repoConfig.Registry.DBParameters = parametersMap
		}
	}

	repoConfig.OfflineStore = OfflineStoreConfig{}
	repoConfig.OnlineStore = OnlineStoreConfig{}

	return nil
}

func setRepoConfigOnline(services *feastdevv1alpha1.FeatureStoreServices, secretExtractionFunc func(secretRef string, secretKeyName string) (map[string]interface{}, error), repoConfig *RepoConfig) error {
	repoConfig.OnlineStore = OnlineStoreConfig{}

	repoConfig.OnlineStore.Path = DefaultOnlineStoreEphemeralPath
	repoConfig.OnlineStore.Type = OnlineSqliteConfigType
	onlineStorePersistence := services.OnlineStore.Persistence

	if onlineStorePersistence != nil {
		filePersistence := onlineStorePersistence.FilePersistence
		dbPersistence := onlineStorePersistence.DBPersistence

		if filePersistence != nil {
			repoConfig.OnlineStore.Path = getActualPath(filePersistence.Path, filePersistence.PvcConfig)
		} else if dbPersistence != nil && len(dbPersistence.Type) > 0 {
			repoConfig.OnlineStore.Path = ""
			repoConfig.OnlineStore.Type = OnlineConfigType(dbPersistence.Type)
			secretKeyName := dbPersistence.SecretKeyName
			if len(secretKeyName) == 0 {
				secretKeyName = string(repoConfig.OnlineStore.Type)
			}

			parametersMap, err := secretExtractionFunc(dbPersistence.SecretRef.Name, secretKeyName)
			if err != nil {
				return err
			}

			err = mergeStructWithDBParametersMap(&parametersMap, &repoConfig.OnlineStore)
			if err != nil {
				return err
			}

			repoConfig.OnlineStore.DBParameters = parametersMap
		}
	}

	return nil
}

func setRepoConfigOffline(services *feastdevv1alpha1.FeatureStoreServices, secretExtractionFunc func(secretRef string, secretKeyName string) (map[string]interface{}, error), repoConfig *RepoConfig) error {
	repoConfig.OfflineStore = OfflineStoreConfig{}
	repoConfig.OfflineStore.Type = OfflineFilePersistenceDaskConfigType
	offlineStorePersistence := services.OfflineStore.Persistence

	if offlineStorePersistence != nil {
		dbPersistence := offlineStorePersistence.DBPersistence
		filePersistence := offlineStorePersistence.FilePersistence

		if filePersistence != nil && len(filePersistence.Type) > 0 {
			repoConfig.OfflineStore.Type = OfflineConfigType(filePersistence.Type)
		} else if offlineStorePersistence.DBPersistence != nil && len(dbPersistence.Type) > 0 {
			repoConfig.OfflineStore.Type = OfflineConfigType(dbPersistence.Type)
			secretKeyName := dbPersistence.SecretKeyName
			if len(secretKeyName) == 0 {
				secretKeyName = string(repoConfig.OfflineStore.Type)
			}

			parametersMap, err := secretExtractionFunc(dbPersistence.SecretRef.Name, secretKeyName)
			if err != nil {
				return err
			}

			err = mergeStructWithDBParametersMap(&parametersMap, &repoConfig.OfflineStore)
			if err != nil {
				return err
			}

			repoConfig.OfflineStore.DBParameters = parametersMap
		}
	}

	repoConfig.OnlineStore = OnlineStoreConfig{}

	return nil
}

func (feast *FeastServices) getClientFeatureStoreYaml() ([]byte, error) {
	return yaml.Marshal(getClientRepoConfig(feast.Handler.FeatureStore))
}

func getClientRepoConfig(featureStore *feastdevv1alpha1.FeatureStore) RepoConfig {
	status := featureStore.Status
	appliedServices := status.Applied.Services
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
		if appliedServices.OfflineStore != nil && appliedServices.OfflineStore.TLS != nil &&
			(&appliedServices.OfflineStore.TLS.TlsConfigs).IsTLS() {
			clientRepoConfig.OfflineStore.Cert = GetTlsPath(OfflineFeastType) + appliedServices.OfflineStore.TLS.TlsConfigs.SecretKeyNames.TlsCrt
			clientRepoConfig.OfflineStore.Port = HttpsPort
			clientRepoConfig.OfflineStore.Scheme = HttpsScheme
		}
	}
	if len(status.ServiceHostnames.OnlineStore) > 0 {
		onlinePath := "://" + status.ServiceHostnames.OnlineStore
		clientRepoConfig.OnlineStore = OnlineStoreConfig{
			Type: OnlineRemoteConfigType,
			Path: HttpScheme + onlinePath,
		}
		if appliedServices.OnlineStore != nil && appliedServices.OnlineStore.TLS.IsTLS() {
			clientRepoConfig.OnlineStore.Cert = GetTlsPath(OnlineFeastType) + appliedServices.OnlineStore.TLS.SecretKeyNames.TlsCrt
			clientRepoConfig.OnlineStore.Path = HttpsScheme + onlinePath
		}
	}
	if len(status.ServiceHostnames.Registry) > 0 {
		clientRepoConfig.Registry = RegistryConfig{
			RegistryType: RegistryRemoteConfigType,
			Path:         status.ServiceHostnames.Registry,
		}
		if localRegistryTls(featureStore) {
			clientRepoConfig.Registry.Cert = GetTlsPath(RegistryFeastType) + appliedServices.Registry.Local.TLS.SecretKeyNames.TlsCrt
		} else if remoteRegistryTls(featureStore) {
			clientRepoConfig.Registry.Cert = GetTlsPath(RegistryFeastType) + appliedServices.Registry.Remote.TLS.CertName
		}
	}

	if status.Applied.AuthzConfig.KubernetesAuthz == nil {
		clientRepoConfig.AuthzConfig = AuthzConfig{
			Type: NoAuthAuthType,
		}
	} else {
		if status.Applied.AuthzConfig.KubernetesAuthz != nil {
			clientRepoConfig.AuthzConfig = AuthzConfig{
				Type: KubernetesAuthType,
			}
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

func (feast *FeastServices) extractConfigFromSecret(secretRef string, secretKeyName string) (map[string]interface{}, error) {
	secret, err := feast.getSecret(secretRef)
	if err != nil {
		return nil, err
	}
	parameters := map[string]interface{}{}

	val, exists := secret.Data[secretKeyName]
	if !exists {
		return nil, fmt.Errorf("secret key %s doesn't exist in secret %s", secretKeyName, secretRef)
	}

	err = yaml.Unmarshal(val, &parameters)
	if err != nil {
		return nil, fmt.Errorf("secret %s contains invalid value", secretKeyName)
	}

	_, exists = parameters["type"]
	if exists {
		return nil, fmt.Errorf("secret key %s in secret %s contains invalid tag named type", secretKeyName, secretRef)
	}

	_, exists = parameters["registry_type"]
	if exists {
		return nil, fmt.Errorf("secret key %s in secret %s contains invalid tag named registry_type", secretKeyName, secretRef)
	}

	return parameters, nil
}

func mergeStructWithDBParametersMap(parametersMap *map[string]interface{}, s interface{}) error {
	for key, val := range *parametersMap {
		hasAttribute, err := hasAttrib(s, key, val)
		if err != nil {
			return err
		}

		if hasAttribute {
			delete(*parametersMap, key)
		}
	}

	return nil
}
