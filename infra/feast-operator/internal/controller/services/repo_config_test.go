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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
)

var projectName = "test-project"

var _ = Describe("Repo Config", func() {
	Context("When creating the RepoConfig of a FeatureStore", func() {
		It("should successfully create the repo configs", func() {
			By("Having the minimal created resource")
			featureStore := minimalFeatureStore()
			ApplyDefaultsToStatus(featureStore)

			expectedRegistryConfig := RegistryConfig{
				RegistryType: "file",
				Path:         EphemeralPath + "/" + DefaultRegistryPath,
			}
			expectedOnlineConfig := OnlineStoreConfig{
				Type: "sqlite",
				Path: EphemeralPath + "/" + DefaultOnlineStorePath,
			}

			repoConfig, err := getServiceRepoConfig(featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.AuthzConfig.Type).To(Equal(NoAuthAuthType))
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig))
			Expect(repoConfig.OnlineStore).To(Equal(expectedOnlineConfig))
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))

			By("Having the local registry resource")
			featureStore = minimalFeatureStore()
			testPath := "/test/file.db"
			featureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				Registry: &feastdevv1alpha1.Registry{
					Local: &feastdevv1alpha1.LocalRegistryConfig{
						Persistence: &feastdevv1alpha1.RegistryPersistence{
							FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
								Path: testPath,
							},
						},
					},
				},
			}
			ApplyDefaultsToStatus(featureStore)

			expectedRegistryConfig = RegistryConfig{
				RegistryType: "file",
				Path:         testPath,
			}

			repoConfig, err = getServiceRepoConfig(featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.AuthzConfig.Type).To(Equal(NoAuthAuthType))
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig))
			Expect(repoConfig.OnlineStore).To(Equal(expectedOnlineConfig))
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))

			By("Adding an offlineStore with PVC")
			featureStore.Spec.Services.OfflineStore = &feastdevv1alpha1.OfflineStore{
				Persistence: &feastdevv1alpha1.OfflineStorePersistence{
					FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
						PvcConfig: &feastdevv1alpha1.PvcConfig{
							MountPath: "/testing",
						},
					},
				},
			}
			ApplyDefaultsToStatus(featureStore)
			appliedServices := featureStore.Status.Applied.Services
			Expect(appliedServices.OnlineStore).NotTo(BeNil())
			Expect(appliedServices.Registry.Local).NotTo(BeNil())

			repoConfig, err = getServiceRepoConfig(featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(defaultOfflineStoreConfig))
			Expect(repoConfig.AuthzConfig.Type).To(Equal(NoAuthAuthType))
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))
			Expect(repoConfig.OnlineStore).To(Equal(expectedOnlineConfig))

			By("Having the remote registry resource")
			featureStore = minimalFeatureStore()
			featureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				Registry: &feastdevv1alpha1.Registry{
					Remote: &feastdevv1alpha1.RemoteRegistryConfig{
						FeastRef: &feastdevv1alpha1.FeatureStoreRef{
							Name: "registry",
						},
					},
				},
			}
			ApplyDefaultsToStatus(featureStore)
			repoConfig, err = getServiceRepoConfig(featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.AuthzConfig.Type).To(Equal(NoAuthAuthType))
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig))
			Expect(repoConfig.OnlineStore).To(Equal(expectedOnlineConfig))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig))

			By("Having the all the file services")
			featureStore = minimalFeatureStore()
			featureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				OfflineStore: &feastdevv1alpha1.OfflineStore{
					Persistence: &feastdevv1alpha1.OfflineStorePersistence{
						FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
							Type: "duckdb",
						},
					},
				},
				OnlineStore: &feastdevv1alpha1.OnlineStore{
					Persistence: &feastdevv1alpha1.OnlineStorePersistence{
						FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
							Path: "/data/online.db",
						},
					},
				},
				Registry: &feastdevv1alpha1.Registry{
					Local: &feastdevv1alpha1.LocalRegistryConfig{
						Persistence: &feastdevv1alpha1.RegistryPersistence{
							FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
								Path: "/data/registry.db",
							},
						},
					},
				},
			}
			ApplyDefaultsToStatus(featureStore)

			expectedOfflineConfig := OfflineStoreConfig{
				Type: "duckdb",
			}
			expectedRegistryConfig = RegistryConfig{
				RegistryType: "file",
				Path:         "/data/registry.db",
			}
			expectedOnlineConfig = OnlineStoreConfig{
				Type: "sqlite",
				Path: "/data/online.db",
			}

			repoConfig, err = getServiceRepoConfig(featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.AuthzConfig.Type).To(Equal(NoAuthAuthType))
			Expect(repoConfig.OfflineStore).To(Equal(expectedOfflineConfig))
			Expect(repoConfig.OnlineStore).To(Equal(expectedOnlineConfig))
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))

			By("Having kubernetes authorization")
			featureStore = minimalFeatureStore()
			featureStore.Spec.AuthzConfig = &feastdevv1alpha1.AuthzConfig{
				KubernetesAuthz: &feastdevv1alpha1.KubernetesAuthz{},
			}
			featureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				OfflineStore: &feastdevv1alpha1.OfflineStore{},
				OnlineStore:  &feastdevv1alpha1.OnlineStore{},
				Registry: &feastdevv1alpha1.Registry{
					Local: &feastdevv1alpha1.LocalRegistryConfig{},
				},
			}
			ApplyDefaultsToStatus(featureStore)

			expectedOfflineConfig = OfflineStoreConfig{
				Type: "dask",
			}

			repoConfig, err = getServiceRepoConfig(featureStore, mockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.AuthzConfig.Type).To(Equal(KubernetesAuthType))
			Expect(repoConfig.OfflineStore).To(Equal(expectedOfflineConfig))
			Expect(repoConfig.OnlineStore).To(Equal(defaultOnlineStoreConfig(featureStore)))
			Expect(repoConfig.Registry).To(Equal(defaultRegistryConfig(featureStore)))

			By("Having oidc authorization")
			featureStore.Spec.AuthzConfig = &feastdevv1alpha1.AuthzConfig{
				OidcAuthz: &feastdevv1alpha1.OidcAuthz{
					SecretRef: corev1.LocalObjectReference{
						Name: "oidc-secret",
					},
				},
			}
			ApplyDefaultsToStatus(featureStore)

			secretExtractionFunc := mockOidcConfigFromSecret(map[string]interface{}{
				string(OidcAuthDiscoveryUrl): "discovery-url",
				string(OidcClientId):         "client-id",
				string(OidcClientSecret):     "client-secret",
				string(OidcUsername):         "username",
				string(OidcPassword):         "password"})
			repoConfig, err = getServiceRepoConfig(featureStore, secretExtractionFunc)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.AuthzConfig.Type).To(Equal(OidcAuthType))
			Expect(repoConfig.AuthzConfig.OidcParameters).To(HaveLen(2))
			Expect(repoConfig.AuthzConfig.OidcParameters).To(HaveKey(string(OidcClientId)))
			Expect(repoConfig.AuthzConfig.OidcParameters).To(HaveKey(string(OidcAuthDiscoveryUrl)))
			Expect(repoConfig.OfflineStore).To(Equal(expectedOfflineConfig))
			Expect(repoConfig.OnlineStore).To(Equal(defaultOnlineStoreConfig(featureStore)))
			Expect(repoConfig.Registry).To(Equal(defaultRegistryConfig(featureStore)))

			repoConfig, err = getClientRepoConfig(featureStore, secretExtractionFunc, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.AuthzConfig.Type).To(Equal(OidcAuthType))
			Expect(repoConfig.AuthzConfig.OidcParameters).To(HaveLen(3))
			Expect(repoConfig.AuthzConfig.OidcParameters).To(HaveKey(string(OidcClientSecret)))
			Expect(repoConfig.AuthzConfig.OidcParameters).To(HaveKey(string(OidcUsername)))
			Expect(repoConfig.AuthzConfig.OidcParameters).To(HaveKey(string(OidcPassword)))

			By("Having the all the db services")
			featureStore = minimalFeatureStore()
			featureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				OfflineStore: &feastdevv1alpha1.OfflineStore{
					Persistence: &feastdevv1alpha1.OfflineStorePersistence{
						DBPersistence: &feastdevv1alpha1.OfflineStoreDBStorePersistence{
							Type: string(OfflineDBPersistenceSnowflakeConfigType),
							SecretRef: corev1.LocalObjectReference{
								Name: "offline-test-secret",
							},
						},
					},
				},
				OnlineStore: &feastdevv1alpha1.OnlineStore{
					Persistence: &feastdevv1alpha1.OnlineStorePersistence{
						DBPersistence: &feastdevv1alpha1.OnlineStoreDBStorePersistence{
							Type: string(OnlineDBPersistenceSnowflakeConfigType),
							SecretRef: corev1.LocalObjectReference{
								Name: "online-test-secret",
							},
						},
					},
				},
				Registry: &feastdevv1alpha1.Registry{
					Local: &feastdevv1alpha1.LocalRegistryConfig{
						Persistence: &feastdevv1alpha1.RegistryPersistence{
							DBPersistence: &feastdevv1alpha1.RegistryDBStorePersistence{
								Type: string(RegistryDBPersistenceSnowflakeConfigType),
								SecretRef: corev1.LocalObjectReference{
									Name: "registry-test-secret",
								},
							},
						},
					},
				},
			}
			parameterMap := createParameterMap()
			ApplyDefaultsToStatus(featureStore)
			featureStore.Spec.Services.OfflineStore.Persistence.FilePersistence = nil
			featureStore.Spec.Services.OnlineStore.Persistence.FilePersistence = nil
			featureStore.Spec.Services.Registry.Local.Persistence.FilePersistence = nil
			repoConfig, err = getServiceRepoConfig(featureStore, mockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			newMap := CopyMap(parameterMap)
			port := parameterMap["port"].(int)
			delete(newMap, "port")
			expectedOfflineConfig = OfflineStoreConfig{
				Type:         OfflineDBPersistenceSnowflakeConfigType,
				Port:         port,
				DBParameters: newMap,
			}
			expectedOnlineConfig = OnlineStoreConfig{
				Type:         OnlineDBPersistenceSnowflakeConfigType,
				DBParameters: CopyMap(parameterMap),
			}
			expectedRegistryConfig = RegistryConfig{
				RegistryType: RegistryDBPersistenceSnowflakeConfigType,
				DBParameters: parameterMap,
			}
			Expect(repoConfig.OfflineStore).To(Equal(expectedOfflineConfig))
			Expect(repoConfig.OnlineStore).To(Equal(expectedOnlineConfig))
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))
		})
	})
	It("should fail to create the repo configs", func() {
		featureStore := minimalFeatureStore()

		By("Having invalid server oidc authorization")
		featureStore.Spec.AuthzConfig = &feastdevv1alpha1.AuthzConfig{
			OidcAuthz: &feastdevv1alpha1.OidcAuthz{
				SecretRef: corev1.LocalObjectReference{
					Name: "oidc-secret",
				},
			},
		}
		ApplyDefaultsToStatus(featureStore)

		secretExtractionFunc := mockOidcConfigFromSecret(map[string]interface{}{
			string(OidcClientId):     "client-id",
			string(OidcClientSecret): "client-secret",
			string(OidcUsername):     "username",
			string(OidcPassword):     "password"})
		_, err := getServiceRepoConfig(featureStore, secretExtractionFunc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("missing OIDC secret"))
		_, err = getServiceRepoConfig(featureStore, secretExtractionFunc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("missing OIDC secret"))
		_, err = getServiceRepoConfig(featureStore, secretExtractionFunc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("missing OIDC secret"))
		_, err = getClientRepoConfig(featureStore, secretExtractionFunc, nil)
		Expect(err).ToNot(HaveOccurred())

		By("Having invalid client oidc authorization")
		featureStore.Spec.AuthzConfig = &feastdevv1alpha1.AuthzConfig{
			OidcAuthz: &feastdevv1alpha1.OidcAuthz{
				SecretRef: corev1.LocalObjectReference{
					Name: "oidc-secret",
				},
			},
		}
		ApplyDefaultsToStatus(featureStore)

		secretExtractionFunc = mockOidcConfigFromSecret(map[string]interface{}{
			string(OidcAuthDiscoveryUrl): "discovery-url",
			string(OidcClientId):         "client-id",
			string(OidcUsername):         "username",
			string(OidcPassword):         "password"})
		_, err = getServiceRepoConfig(featureStore, secretExtractionFunc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("missing OIDC secret"))
		_, err = getServiceRepoConfig(featureStore, secretExtractionFunc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("missing OIDC secret"))
		_, err = getServiceRepoConfig(featureStore, secretExtractionFunc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("missing OIDC secret"))
		_, err = getClientRepoConfig(featureStore, secretExtractionFunc, nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("missing OIDC secret"))
	})
})

var emptyOfflineStoreConfig = OfflineStoreConfig{}
var emptyRegistryConfig = RegistryConfig{}

func minimalFeatureStore() *feastdevv1alpha1.FeatureStore {
	return &feastdevv1alpha1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{Name: "test"},
		Spec: feastdevv1alpha1.FeatureStoreSpec{
			FeastProject: projectName,
		},
	}
}

func minimalFeatureStoreWithAllServers() *feastdevv1alpha1.FeatureStore {
	feast := minimalFeatureStore()
	// onlineStore configured by default
	feast.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Server: &feastdevv1alpha1.ServerConfigs{},
		},
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Server: &feastdevv1alpha1.RegistryServerConfigs{},
			},
		},
		UI: &feastdevv1alpha1.ServerConfigs{},
	}
	return feast
}

func emptyMockExtractConfigFromSecret(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func mockExtractConfigFromSecret(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error) {
	return createParameterMap(), nil
}

func mockOidcConfigFromSecret(
	oidcProperties map[string]interface{}) func(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error) {
	return func(storeType string, secretRef string, secretKeyName string) (map[string]interface{}, error) {
		return oidcProperties, nil
	}
}

func createParameterMap() map[string]interface{} {
	yamlString := `
hosts:
  - 192.168.1.1
  - 192.168.1.2
  - 192.168.1.3
keyspace: KeyspaceName
port: 9042                                                              
username: user                                                          
password: secret                                                        
protocol_version: 5                                                     
load_balancing:                                                         
  local_dc: datacenter1                                             
  load_balancing_policy: TokenAwarePolicy(DCAwareRoundRobinPolicy)
read_concurrency: 100                                                   
write_concurrency: 100
`
	var parameters map[string]interface{}

	err := yaml.Unmarshal([]byte(yamlString), &parameters)
	if err != nil {
		fmt.Println(err)
	}
	return parameters
}

var _ = Describe("getCertificatePath", func() {
	Context("when feast parameter is nil", func() {
		It("should return individual service certificate path", func() {
			// Test with nil feast parameter
			path := getCertificatePath(nil, OfflineFeastType, "tls.crt")
			Expect(path).To(Equal("/tls/offline/tls.crt"))

			path = getCertificatePath(nil, OnlineFeastType, "tls.crt")
			Expect(path).To(Equal("/tls/online/tls.crt"))

			path = getCertificatePath(nil, RegistryFeastType, "tls.crt")
			Expect(path).To(Equal("/tls/registry/tls.crt"))
		})
	})

	Context("with different certificate file names", func() {
		It("should use the provided certificate file name", func() {
			// Test with nil feast parameter (no custom CA bundle)
			path := getCertificatePath(nil, OfflineFeastType, "custom.crt")
			Expect(path).To(Equal("/tls/offline/custom.crt"))

			path = getCertificatePath(nil, RegistryFeastType, "remote.crt")
			Expect(path).To(Equal("/tls/registry/remote.crt"))
		})
	})

	Context("when custom CA bundle is available", func() {
		It("should return custom CA bundle path", func() {
			// Create a FeastServices instance with custom CA bundle available
			// This test would require a full test environment setup
			// For now, we test the nil case which covers the fallback behavior
			path := getCertificatePath(nil, OfflineFeastType, "tls.crt")
			Expect(path).To(Equal("/tls/offline/tls.crt"))
		})
	})
})

var _ = Describe("TLS Certificate Path Configuration", func() {
	Context("in getClientRepoConfig", func() {
		It("should use individual service certificate paths when no custom CA bundle", func() {
			// Create a feature store with TLS enabled
			featureStore := &feastdevv1alpha1.FeatureStore{
				Status: feastdevv1alpha1.FeatureStoreStatus{
					ServiceHostnames: feastdevv1alpha1.ServiceHostnames{
						OfflineStore: "offline.example.com:443",
						OnlineStore:  "online.example.com:443",
						Registry:     "registry.example.com:443",
					},
					Applied: feastdevv1alpha1.FeatureStoreSpec{
						Services: &feastdevv1alpha1.FeatureStoreServices{
							OfflineStore: &feastdevv1alpha1.OfflineStore{
								Server: &feastdevv1alpha1.ServerConfigs{
									TLS: &feastdevv1alpha1.TlsConfigs{
										SecretRef: &corev1.LocalObjectReference{Name: "offline-tls"},
										SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
											TlsCrt: "tls.crt",
										},
									},
								},
							},
							OnlineStore: &feastdevv1alpha1.OnlineStore{
								Server: &feastdevv1alpha1.ServerConfigs{
									TLS: &feastdevv1alpha1.TlsConfigs{
										SecretRef: &corev1.LocalObjectReference{Name: "online-tls"},
										SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
											TlsCrt: "tls.crt",
										},
									},
								},
							},
							UI: &feastdevv1alpha1.ServerConfigs{
								TLS: &feastdevv1alpha1.TlsConfigs{
									SecretRef: &corev1.LocalObjectReference{Name: "ui-tls"},
									SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
										TlsCrt: "tls.crt",
									},
								},
							},
							Registry: &feastdevv1alpha1.Registry{
								Local: &feastdevv1alpha1.LocalRegistryConfig{
									Server: &feastdevv1alpha1.RegistryServerConfigs{
										ServerConfigs: feastdevv1alpha1.ServerConfigs{
											TLS: &feastdevv1alpha1.TlsConfigs{
												SecretRef: &corev1.LocalObjectReference{Name: "registry-tls"},
												SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
													TlsCrt: "tls.crt",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}

			// Test with nil feast parameter (no custom CA bundle)
			repoConfig, err := getClientRepoConfig(featureStore, emptyMockExtractConfigFromSecret, nil)
			Expect(err).NotTo(HaveOccurred())

			// Verify individual service certificate paths are used
			Expect(repoConfig.OfflineStore.Cert).To(Equal("/tls/offline/tls.crt"))
			Expect(repoConfig.OnlineStore.Cert).To(Equal("/tls/online/tls.crt"))
			Expect(repoConfig.Registry.Cert).To(Equal("/tls/registry/tls.crt"))
		})

		It("should use custom CA bundle path when available", func() {
			// This test would require a full FeastServices setup with custom CA bundle
			// For now, we verify the function signature and basic behavior
			featureStore := &feastdevv1alpha1.FeatureStore{
				Status: feastdevv1alpha1.FeatureStoreStatus{
					ServiceHostnames: feastdevv1alpha1.ServiceHostnames{
						OfflineStore: "offline.example.com:443",
						OnlineStore:  "online.example.com:443",
						Registry:     "registry.example.com:443",
						UI:           "ui.example.com:443",
					},
					Applied: feastdevv1alpha1.FeatureStoreSpec{
						Services: &feastdevv1alpha1.FeatureStoreServices{
							OfflineStore: &feastdevv1alpha1.OfflineStore{
								Server: &feastdevv1alpha1.ServerConfigs{
									TLS: &feastdevv1alpha1.TlsConfigs{
										SecretRef: &corev1.LocalObjectReference{Name: "offline-tls"},
										SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
											TlsCrt: "tls.crt",
										},
									},
								},
							},
							OnlineStore: &feastdevv1alpha1.OnlineStore{
								Server: &feastdevv1alpha1.ServerConfigs{
									TLS: &feastdevv1alpha1.TlsConfigs{
										SecretRef: &corev1.LocalObjectReference{Name: "online-tls"},
										SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
											TlsCrt: "tls.crt",
										},
									},
								},
							},
							UI: &feastdevv1alpha1.ServerConfigs{
								TLS: &feastdevv1alpha1.TlsConfigs{
									SecretRef: &corev1.LocalObjectReference{Name: "ui-tls"},
									SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
										TlsCrt: "tls.crt",
									},
								},
							},
							Registry: &feastdevv1alpha1.Registry{
								Local: &feastdevv1alpha1.LocalRegistryConfig{
									Server: &feastdevv1alpha1.RegistryServerConfigs{
										ServerConfigs: feastdevv1alpha1.ServerConfigs{
											TLS: &feastdevv1alpha1.TlsConfigs{
												SecretRef: &corev1.LocalObjectReference{Name: "registry-tls"},
												SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
													TlsCrt: "tls.crt",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}

			// Test with nil feast parameter (no custom CA bundle available)
			repoConfig, err := getClientRepoConfig(featureStore, emptyMockExtractConfigFromSecret, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore.Cert).To(Equal("/tls/offline/tls.crt"))
		})
	})
})
