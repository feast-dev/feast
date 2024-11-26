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

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
)

var projectName = "test-project"

var _ = Describe("Repo Config", func() {
	Context("When creating the RepoConfig of a FeatureStore", func() {

		It("should successfully create the repo configs", func() {
			By("Having the minimal created resource")
			featureStore := minimalFeatureStore()
			ApplyDefaultsToStatus(featureStore)
			var repoConfig RepoConfig
			repoConfig, err := getServiceRepoConfig(OfflineFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(OnlineFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(RegistryFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			expectedRegistryConfig := RegistryConfig{
				RegistryType: "file",
				Path:         DefaultRegistryEphemeralPath,
			}
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))

			By("Having the local registry resource")
			featureStore = minimalFeatureStore()
			featureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				Registry: &feastdevv1alpha1.Registry{
					Local: &feastdevv1alpha1.LocalRegistryConfig{
						Persistence: &feastdevv1alpha1.RegistryPersistence{
							FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
								Path: "file.db",
							},
						},
					},
				},
			}
			ApplyDefaultsToStatus(featureStore)
			repoConfig, err = getServiceRepoConfig(OfflineFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(OnlineFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(RegistryFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			expectedRegistryConfig = RegistryConfig{
				RegistryType: "file",
				Path:         "file.db",
			}
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))

			By("Having the remote registry resource")
			featureStore = minimalFeatureStore()
			featureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				Registry: &feastdevv1alpha1.Registry{
					Remote: &feastdevv1alpha1.RemoteRegistryConfig{
						FeastRef: &feastdevv1alpha1.FeatureStoreRef{
							Name:      "registry",
							Namespace: "remoteNS",
						},
					},
				},
			}
			ApplyDefaultsToStatus(featureStore)
			repoConfig, err = getServiceRepoConfig(OfflineFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(OnlineFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(RegistryFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

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
			repoConfig, err = getServiceRepoConfig(OfflineFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			expectedOfflineConfig := OfflineStoreConfig{
				Type: "duckdb",
			}
			Expect(repoConfig.OfflineStore).To(Equal(expectedOfflineConfig))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(OnlineFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			expectedOnlineConfig := OnlineStoreConfig{
				Type: "sqlite",
				Path: "/data/online.db",
			}
			Expect(repoConfig.OnlineStore).To(Equal(expectedOnlineConfig))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(RegistryFeastType, featureStore, emptyMockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			expectedRegistryConfig = RegistryConfig{
				RegistryType: "file",
				Path:         "/data/registry.db",
			}
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))

			By("Having the all the db services")
			featureStore = minimalFeatureStore()
			featureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				OfflineStore: &feastdevv1alpha1.OfflineStore{
					Persistence: &feastdevv1alpha1.OfflineStorePersistence{
						DBPersistence: &feastdevv1alpha1.OfflineStoreDBStorePersistence{
							Type: string(OfflineDBPersistenceSnowflakeConfigType),
							SecretRef: &corev1.LocalObjectReference{
								Name: "offline-test-secret",
							},
						},
					},
				},
				OnlineStore: &feastdevv1alpha1.OnlineStore{
					Persistence: &feastdevv1alpha1.OnlineStorePersistence{
						DBPersistence: &feastdevv1alpha1.OnlineStoreDBStorePersistence{
							Type: string(OnlineDBPersistenceSnowflakeConfigType),
							SecretRef: &corev1.LocalObjectReference{
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
								SecretRef: &corev1.LocalObjectReference{
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
			repoConfig, err = getServiceRepoConfig(OfflineFeastType, featureStore, mockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			newMap := CopyMap(parameterMap)
			port := parameterMap["port"].(int)
			delete(newMap, "port")
			expectedOfflineConfig = OfflineStoreConfig{
				Type:         OfflineDBPersistenceSnowflakeConfigType,
				Port:         port,
				DBParameters: newMap,
			}
			Expect(repoConfig.OfflineStore).To(Equal(expectedOfflineConfig))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(OnlineFeastType, featureStore, mockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			newMap = CopyMap(parameterMap)
			expectedOnlineConfig = OnlineStoreConfig{
				Type:         OnlineDBPersistenceSnowflakeConfigType,
				DBParameters: newMap,
			}
			Expect(repoConfig.OnlineStore).To(Equal(expectedOnlineConfig))
			Expect(repoConfig.Registry).To(Equal(emptyRegistryConfig()))

			repoConfig, err = getServiceRepoConfig(RegistryFeastType, featureStore, mockExtractConfigFromSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore).To(Equal(emptyOfflineStoreConfig()))
			Expect(repoConfig.OnlineStore).To(Equal(emptyOnlineStoreConfig()))
			expectedRegistryConfig = RegistryConfig{
				RegistryType: RegistryDBPersistenceSnowflakeConfigType,
				DBParameters: parameterMap,
			}
			Expect(repoConfig.Registry).To(Equal(expectedRegistryConfig))
		})
	})
})

func emptyOnlineStoreConfig() OnlineStoreConfig {
	return OnlineStoreConfig{}
}

func emptyOfflineStoreConfig() OfflineStoreConfig {
	return OfflineStoreConfig{}
}

func emptyRegistryConfig() RegistryConfig {
	return RegistryConfig{}
}

func minimalFeatureStore() *feastdevv1alpha1.FeatureStore {
	return &feastdevv1alpha1.FeatureStore{
		Spec: feastdevv1alpha1.FeatureStoreSpec{
			FeastProject: projectName,
		},
	}
}

func emptyMockExtractConfigFromSecret(secretRef string, secretKeyName string) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func mockExtractConfigFromSecret(secretRef string, secretKeyName string) (map[string]interface{}, error) {
	return createParameterMap(), nil
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
