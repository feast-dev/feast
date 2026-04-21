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
	"os"
	"path"
	"strings"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"gopkg.in/yaml.v3"
)

const oidcIssuerUrlEnvVar = "OIDC_ISSUER_URL"

// GetServiceFeatureStoreYamlBase64 returns a base64 encoded feature_store.yaml config for the feast service
func (feast *FeastServices) GetServiceFeatureStoreYamlBase64() (string, error) {
	fsYaml, err := feast.getServiceFeatureStoreYaml()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(fsYaml), nil
}

func (feast *FeastServices) getServiceFeatureStoreYaml() ([]byte, error) {
	repoConfig, err := feast.getServiceRepoConfig()
	if err != nil {
		return nil, err
	}
	return yaml.Marshal(repoConfig)
}

func (feast *FeastServices) getServiceRepoConfig() (RepoConfig, error) {
	odhCaBundleExists := feast.GetCustomCertificatesBundle().IsDefined
	return getServiceRepoConfig(feast.Handler.FeatureStore, feast.extractConfigFromSecret, feast.extractConfigFromConfigMap, odhCaBundleExists)
}

func getServiceRepoConfig(
	featureStore *feastdevv1.FeatureStore,
	secretExtractionFunc func(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error),
	configMapExtractionFunc func(configMapRef string, configMapKey string) (map[string]interface{}, error),
	odhCaBundleExists bool) (RepoConfig, error) {
	repoConfig, err := getBaseServiceRepoConfig(featureStore, secretExtractionFunc, odhCaBundleExists)
	if err != nil {
		return repoConfig, err
	}

	appliedSpec := featureStore.Status.Applied
	if appliedSpec.Services != nil {
		services := appliedSpec.Services
		if services.OfflineStore != nil {
			err := setRepoConfigOffline(services, secretExtractionFunc, &repoConfig)
			if err != nil {
				return repoConfig, err
			}
		}
		if services.OnlineStore != nil {
			err := setRepoConfigOnline(services, secretExtractionFunc, &repoConfig)
			if err != nil {
				return repoConfig, err
			}
		}
		if IsLocalRegistry(featureStore) {
			err := setRepoConfigRegistry(services, secretExtractionFunc, &repoConfig)
			if err != nil {
				return repoConfig, err
			}
		}
	}

	if appliedSpec.BatchEngine != nil {
		err := setRepoConfigBatchEngine(appliedSpec.BatchEngine, configMapExtractionFunc, &repoConfig)
		if err != nil {
			return repoConfig, err
		}
	}

	if appliedSpec.Dqm != nil {
		setRepoConfigDqm(appliedSpec.Dqm, &repoConfig)
	}

	return repoConfig, nil
}

func getBaseServiceRepoConfig(
	featureStore *feastdevv1.FeatureStore,
	secretExtractionFunc func(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error),
	odhCaBundleExists bool) (RepoConfig, error) {

	repoConfig := defaultRepoConfig(featureStore)
	clientRepoConfig := getClientRepoConfig(featureStore, nil)
	if isRemoteRegistry(featureStore) {
		repoConfig.Registry = clientRepoConfig.Registry
	}
	appliedSpec := featureStore.Status.Applied
	if appliedSpec.AuthzConfig != nil && appliedSpec.AuthzConfig.OidcAuthz != nil {
		repoConfig.AuthzConfig = AuthzConfig{Type: OidcAuthType}
		oidcAuthz := appliedSpec.AuthzConfig.OidcAuthz
		oidcParameters := map[string]interface{}{}

		var secretProperties map[string]interface{}
		if oidcAuthz.SecretRef != nil {
			var err error
			secretProperties, err = secretExtractionFunc("", oidcAuthz.SecretRef.Name, oidcAuthz.SecretKeyName)
			if err != nil {
				return repoConfig, err
			}
			for _, prop := range OidcOptionalSecretProperties {
				if val, exists := secretProperties[string(prop)]; exists {
					oidcParameters[string(prop)] = val
				}
			}
		}

		discoveryUrl, err := resolveAuthDiscoveryUrl(oidcAuthz, secretProperties)
		if err != nil {
			return repoConfig, err
		}
		oidcParameters[string(OidcAuthDiscoveryUrl)] = discoveryUrl

		if oidcAuthz.VerifySSL != nil {
			oidcParameters[string(OidcVerifySsl)] = *oidcAuthz.VerifySSL
		}
		if caCertPath := resolveOidcCACertPath(oidcAuthz, odhCaBundleExists); caCertPath != "" {
			oidcParameters[string(OidcCaCertPath)] = caCertPath
		}
		repoConfig.AuthzConfig.OidcParameters = oidcParameters
	} else {
		repoConfig.AuthzConfig = clientRepoConfig.AuthzConfig
	}

	return repoConfig, nil
}

// resolveAuthDiscoveryUrl determines the OIDC discovery URL from the first available source.
// Priority: CR issuerUrl > Secret auth_discovery_url > OIDC_ISSUER_URL env var.
func resolveAuthDiscoveryUrl(oidcAuthz *feastdevv1.OidcAuthz, secretProperties map[string]interface{}) (string, error) {
	if oidcAuthz.IssuerUrl != "" {
		return issuerToDiscoveryUrl(oidcAuthz.IssuerUrl), nil
	}

	if val, ok := secretProperties[string(OidcAuthDiscoveryUrl)]; ok {
		if s, ok := val.(string); ok && s != "" {
			return s, nil
		}
	}

	if envIssuer := os.Getenv(oidcIssuerUrlEnvVar); envIssuer != "" {
		return issuerToDiscoveryUrl(envIssuer), nil
	}

	return "", fmt.Errorf("no OIDC discovery URL configured: set issuerUrl on the OidcAuthz CR, "+
		"include auth_discovery_url in the referenced Secret, or ensure the %s environment variable is set on the operator pod", oidcIssuerUrlEnvVar)
}

func issuerToDiscoveryUrl(issuerUrl string) string {
	return strings.TrimRight(issuerUrl, "/") + "/.well-known/openid-configuration"
}

// resolveOidcCACertPath determines the CA cert file path for OIDC provider TLS verification.
// Priority: explicit CRD caCertConfigMap > ODH auto-detected bundle > empty (system CA fallback).
func resolveOidcCACertPath(oidcAuthz *feastdevv1.OidcAuthz, odhCaBundleExists bool) string {
	if oidcAuthz.CACertConfigMap != nil {
		return tlsPathOidcCA
	}
	if odhCaBundleExists {
		return tlsPathOdhCABundle
	}
	return ""
}

func setRepoConfigRegistry(services *feastdevv1.FeatureStoreServices, secretExtractionFunc func(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error), repoConfig *RepoConfig) error {
	registryPersistence := services.Registry.Local.Persistence

	if registryPersistence != nil {
		filePersistence := registryPersistence.FilePersistence
		dbPersistence := registryPersistence.DBPersistence

		if filePersistence != nil {
			repoConfig.Registry.RegistryType = RegistryFileConfigType
			repoConfig.Registry.Path = getActualPath(filePersistence.Path, filePersistence.PvcConfig)
			repoConfig.Registry.S3AdditionalKwargs = filePersistence.S3AdditionalKwargs
			if filePersistence.CacheTTLSeconds != nil {
				repoConfig.Registry.CacheTTLSeconds = filePersistence.CacheTTLSeconds
			}
			if filePersistence.CacheMode != nil {
				repoConfig.Registry.CacheMode = filePersistence.CacheMode
			}

		} else if dbPersistence != nil && len(dbPersistence.Type) > 0 {
			repoConfig.Registry.Path = ""
			repoConfig.Registry.RegistryType = RegistryConfigType(dbPersistence.Type)
			secretKeyName := dbPersistence.SecretKeyName
			if len(secretKeyName) == 0 {
				secretKeyName = string(repoConfig.Registry.RegistryType)
			}
			parametersMap, err := secretExtractionFunc(dbPersistence.Type, dbPersistence.SecretRef.Name, secretKeyName)
			if err != nil {
				return err
			}

			// Extract typed cache settings from DB parameters to avoid inline map conflicts
			if ttlVal, ok := parametersMap["cache_ttl_seconds"]; ok {
				switch v := ttlVal.(type) {
				case int:
					ttl := int32(v)
					repoConfig.Registry.CacheTTLSeconds = &ttl
				case int32:
					ttl := v
					repoConfig.Registry.CacheTTLSeconds = &ttl
				case int64:
					ttl := int32(v)
					repoConfig.Registry.CacheTTLSeconds = &ttl
				case float64:
					ttl := int32(v)
					repoConfig.Registry.CacheTTLSeconds = &ttl
				}
				delete(parametersMap, "cache_ttl_seconds")
			}
			if modeVal, ok := parametersMap["cache_mode"]; ok {
				if modeStr, ok := modeVal.(string); ok {
					repoConfig.Registry.CacheMode = &modeStr
				}
				delete(parametersMap, "cache_mode")
			}

			err = mergeStructWithDBParametersMap(&parametersMap, &repoConfig.Registry)
			if err != nil {
				return err
			}

			repoConfig.Registry.DBParameters = parametersMap
		}
	}
	return nil
}

func setRepoConfigOnline(services *feastdevv1.FeatureStoreServices, secretExtractionFunc func(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error), repoConfig *RepoConfig) error {
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

			parametersMap, err := secretExtractionFunc(dbPersistence.Type, dbPersistence.SecretRef.Name, secretKeyName)
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

func setRepoConfigOffline(services *feastdevv1.FeatureStoreServices, secretExtractionFunc func(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error), repoConfig *RepoConfig) error {
	repoConfig.OfflineStore = defaultOfflineStoreConfig
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

			parametersMap, err := secretExtractionFunc(dbPersistence.Type, dbPersistence.SecretRef.Name, secretKeyName)
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
	return nil
}

func setRepoConfigBatchEngine(
	batchEngineConfig *feastdevv1.BatchEngineConfig,
	configMapExtractionFunc func(configMapRef string, configMapKey string) (map[string]interface{}, error),
	repoConfig *RepoConfig) error {
	if batchEngineConfig.ConfigMapRef == nil {
		return nil
	}
	configMapKey := batchEngineConfig.ConfigMapKey
	if configMapKey == "" {
		configMapKey = "config"
	}
	config, err := configMapExtractionFunc(batchEngineConfig.ConfigMapRef.Name, configMapKey)
	if err != nil {
		return err
	}
	// Extract type from config
	engineType, ok := config["type"].(string)
	if !ok {
		return fmt.Errorf("batch engine config must contain 'type' field")
	}
	delete(config, "type")
	repoConfig.BatchEngine = &ComputeEngineConfig{
		Type:       engineType,
		Parameters: config,
	}
	return nil
}

func setRepoConfigDqm(dqmConfig *feastdevv1.DqmConfig, repoConfig *RepoConfig) {
	if dqmConfig.Distribution == nil || dqmConfig.Distribution.Initial == nil || dqmConfig.Distribution.Initial.Enabled == nil {
		return
	}
	repoConfig.FeatureServer = &FeatureServerYamlConfig{
		Dqm: &DqmYamlConfig{
			Distribution: &DqmDistributionYamlConfig{
				Initial: &DqmInitialDistributionYamlConfig{
					Enabled: *dqmConfig.Distribution.Initial.Enabled,
				},
			},
		},
	}
}

func (feast *FeastServices) getClientFeatureStoreYaml() ([]byte, error) {
	clientRepo := getClientRepoConfig(feast.Handler.FeatureStore, feast)
	return yaml.Marshal(clientRepo)
}

func getClientRepoConfig(
	featureStore *feastdevv1.FeatureStore,
	feast *FeastServices) RepoConfig {
	status := featureStore.Status
	appliedServices := status.Applied.Services
	clientRepoConfig := getRepoConfig(featureStore)
	if len(status.ServiceHostnames.OfflineStore) > 0 {
		clientRepoConfig.OfflineStore = OfflineStoreConfig{
			Type: OfflineRemoteConfigType,
			Host: strings.Split(status.ServiceHostnames.OfflineStore, ":")[0],
			Port: HttpPort,
		}
		if appliedServices.OfflineStore != nil &&
			appliedServices.OfflineStore.Server != nil && appliedServices.OfflineStore.Server.TLS.IsTLS() {
			clientRepoConfig.OfflineStore.Cert = getCertificatePath(feast, OfflineFeastType, appliedServices.OfflineStore.Server.TLS.SecretKeyNames.TlsCrt)
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
		if appliedServices.OnlineStore != nil &&
			appliedServices.OnlineStore.Server != nil && appliedServices.OnlineStore.Server.TLS.IsTLS() {
			clientRepoConfig.OnlineStore.Cert = getCertificatePath(feast, OnlineFeastType, appliedServices.OnlineStore.Server.TLS.SecretKeyNames.TlsCrt)
			clientRepoConfig.OnlineStore.Path = HttpsScheme + onlinePath
		}
	}
	if len(status.ServiceHostnames.Registry) > 0 {
		clientRepoConfig.Registry = RegistryConfig{
			RegistryType: RegistryRemoteConfigType,
			Path:         status.ServiceHostnames.Registry,
		}
		if localRegistryTls(featureStore) {
			clientRepoConfig.Registry.Cert = getCertificatePath(feast, RegistryFeastType, appliedServices.Registry.Local.Server.TLS.SecretKeyNames.TlsCrt)
		} else if remoteRegistryTls(featureStore) {
			clientRepoConfig.Registry.Cert = getCertificatePath(feast, RegistryFeastType, appliedServices.Registry.Remote.TLS.CertName)
		}
	}

	return clientRepoConfig
}

func getRepoConfig(featureStore *feastdevv1.FeatureStore) RepoConfig {
	status := featureStore.Status
	repoConfig := initRepoConfig(status.Applied.FeastProject)
	if status.Applied.AuthzConfig != nil {
		if status.Applied.AuthzConfig.KubernetesAuthz != nil {
			repoConfig.AuthzConfig = AuthzConfig{
				Type: KubernetesAuthType,
			}
		} else if status.Applied.AuthzConfig.OidcAuthz != nil {
			repoConfig.AuthzConfig = AuthzConfig{
				Type: OidcAuthType,
			}
			oidcClientProperties := map[string]interface{}{}
			if status.Applied.AuthzConfig.OidcAuthz.TokenEnvVar != nil {
				oidcClientProperties[string(OidcTokenEnvVar)] = *status.Applied.AuthzConfig.OidcAuthz.TokenEnvVar
			}
			if len(oidcClientProperties) > 0 {
				repoConfig.AuthzConfig.OidcParameters = oidcClientProperties
			}
		}
	}
	return repoConfig
}

func getActualPath(filePath string, pvcConfig *feastdevv1.PvcConfig) string {
	if pvcConfig == nil {
		return filePath
	}
	return path.Join(pvcConfig.MountPath, filePath)
}

func (feast *FeastServices) extractConfigFromSecret(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error) {
	secret, err := feast.getSecret(secretRef)
	if err != nil {
		return nil, err
	}
	parameters := map[string]interface{}{}

	if secretKeyName != "" {
		val, exists := secret.Data[secretKeyName]
		if !exists {
			return nil, fmt.Errorf("secret key %s doesn't exist in secret %s", secretKeyName, secretRef)
		}

		err = yaml.Unmarshal(val, &parameters)
		if err != nil {
			return nil, fmt.Errorf("secret %s contains invalid value", secretKeyName)
		}

		typeVal, typeExists := parameters["type"]
		if typeExists && storeType != typeVal {
			return nil, fmt.Errorf("secret key %s in secret %s contains tag named type with value %s", secretKeyName, secretRef, typeVal)
		}

		typeVal, typeExists = parameters["registry_type"]
		if typeExists && storeType != typeVal {
			return nil, fmt.Errorf("secret key %s in secret %s contains tag named registry_type with value %s", secretKeyName, secretRef, typeVal)
		}
	} else {
		for k, v := range secret.Data {
			var val interface{}
			err := yaml.Unmarshal(v, &val)
			if err != nil {
				return nil, fmt.Errorf("secret %s contains invalid value %v", k, v)
			}
			parameters[k] = val
		}
	}

	return parameters, nil
}

func (feast *FeastServices) extractConfigFromConfigMap(configMapRef string, configMapKey string) (map[string]interface{}, error) {
	configMap, err := feast.getConfigMap(configMapRef)
	if err != nil {
		return nil, err
	}
	if configMapKey == "" {
		configMapKey = "config"
	}
	val, exists := configMap.Data[configMapKey]
	if !exists {
		return nil, fmt.Errorf("configmap key %s doesn't exist in configmap %s", configMapKey, configMapRef)
	}
	var config map[string]interface{}
	err = yaml.Unmarshal([]byte(val), &config)
	if err != nil {
		return nil, fmt.Errorf("configmap %s contains invalid YAML in key %s", configMapRef, configMapKey)
	}
	return config, nil
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

func (feast *FeastServices) GetDefaultRepoConfig() RepoConfig {
	return defaultRepoConfig(feast.Handler.FeatureStore)
}

func defaultRepoConfig(featureStore *feastdevv1.FeatureStore) RepoConfig {
	repoConfig := initRepoConfig(featureStore.Status.Applied.FeastProject)
	repoConfig.OnlineStore = defaultOnlineStoreConfig(featureStore)
	repoConfig.Registry = defaultRegistryConfig(featureStore)
	return repoConfig
}

func (feast *FeastServices) GetInitRepoConfig() RepoConfig {
	return initRepoConfig(feast.Handler.FeatureStore.Status.Applied.FeastProject)
}

func initRepoConfig(feastProject string) RepoConfig {
	return RepoConfig{
		Project:                       feastProject,
		Provider:                      LocalProviderType,
		EntityKeySerializationVersion: feastdevv1.SerializationVersion,
		AuthzConfig:                   defaultAuthzConfig,
	}
}

func defaultOnlineStoreConfig(featureStore *feastdevv1.FeatureStore) OnlineStoreConfig {
	return OnlineStoreConfig{
		Type: OnlineSqliteConfigType,
		Path: defaultOnlineStorePath(featureStore),
	}
}

func defaultRegistryConfig(featureStore *feastdevv1.FeatureStore) RegistryConfig {
	return RegistryConfig{
		RegistryType: RegistryFileConfigType,
		Path:         defaultRegistryPath(featureStore),
	}
}

var defaultOfflineStoreConfig = OfflineStoreConfig{
	Type: OfflineFilePersistenceDaskConfigType,
}

var defaultAuthzConfig = AuthzConfig{
	Type: NoAuthAuthType,
}

// getCertificatePath returns the appropriate certificate path based on whether a custom CA bundle is available
func getCertificatePath(feast *FeastServices, feastType FeastServiceType, certFileName string) string {
	// Check if custom CA bundle is available
	if feast != nil {
		customCaBundle := feast.GetCustomCertificatesBundle()
		if customCaBundle.IsDefined {
			// Use custom CA bundle path when available (for RHOAI/ODH deployments)
			return tlsPathCustomCABundle
		}
	}
	// Fall back to individual service certificate path
	return GetTlsPath(feastType) + certFileName
}
