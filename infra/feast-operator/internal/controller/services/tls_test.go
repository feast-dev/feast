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
	"context"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// test tls functions directly
var _ = Describe("TLS Config", func() {
	Context("When reconciling a FeatureStore", func() {
		scheme := runtime.NewScheme()
		utilruntime.Must(clientgoscheme.AddToScheme(scheme))
		utilruntime.Must(feastdevv1.AddToScheme(scheme))

		secretKeyNames := feastdevv1.SecretKeyNames{
			TlsCrt: "tls.crt",
			TlsKey: "tls.key",
		}

		It("should set default TLS configs", func() {
			By("Having the created resource")

			// registry server w/o tls
			feast := FeastServices{
				Handler: handler.FeastHandler{
					Client:       k8sClient,
					Scheme:       scheme,
					Context:      context.TODO(),
					FeatureStore: minimalFeatureStore(),
				},
			}
			feast.Handler.FeatureStore.Spec.Services = &feastdevv1.FeatureStoreServices{
				Registry: &feastdevv1.Registry{
					Local: &feastdevv1.LocalRegistryConfig{
						Server: &feastdevv1.RegistryServerConfigs{
							ServerConfigs: feastdevv1.ServerConfigs{},
						},
					},
				},
			}
			err := feast.ApplyDefaults()
			Expect(err).ToNot(HaveOccurred())

			tls := feast.getTlsConfigs(RegistryFeastType)
			Expect(tls).To(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			Expect(getPortStr(tls)).To(Equal("80"))

			Expect(feast.remoteRegistryTls()).To(BeFalse())
			Expect(feast.localRegistryTls()).To(BeFalse())
			Expect(feast.isOpenShiftTls(OfflineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(OnlineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(UIFeastType)).To(BeFalse())

			openshiftTls, err := feast.checkOpenshiftTls()
			Expect(err).ToNot(HaveOccurred())
			Expect(openshiftTls).To(BeFalse())

			// registry service w/ openshift tls
			testSetIsOpenShift()
			feast.Handler.FeatureStore = minimalFeatureStore()
			feast.Handler.FeatureStore.Spec.Services = &feastdevv1.FeatureStoreServices{
				Registry: &feastdevv1.Registry{
					Local: &feastdevv1.LocalRegistryConfig{
						Server: &feastdevv1.RegistryServerConfigs{
							ServerConfigs: feastdevv1.ServerConfigs{},
						},
					},
				},
			}
			err = feast.ApplyDefaults()
			Expect(err).ToNot(HaveOccurred())

			tls = feast.getTlsConfigs(OfflineFeastType)
			Expect(tls).To(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			tls = feast.getTlsConfigs(OnlineFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeTrue())
			tls = feast.getTlsConfigs(UIFeastType)
			Expect(tls).To(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			tls = feast.getTlsConfigs(RegistryFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeTrue())
			Expect(tls.SecretKeyNames).To(Equal(secretKeyNames))
			Expect(getPortStr(tls)).To(Equal("443"))
			Expect(GetTlsPath(RegistryFeastType)).To(Equal("/tls/registry/"))

			Expect(feast.remoteRegistryTls()).To(BeFalse())
			Expect(feast.localRegistryTls()).To(BeTrue())
			Expect(feast.isOpenShiftTls(OfflineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(OnlineFeastType)).To(BeTrue())
			Expect(feast.isOpenShiftTls(UIFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeTrue())

			openshiftTls, err = feast.checkOpenshiftTls()
			Expect(err).ToNot(HaveOccurred())
			Expect(openshiftTls).To(BeTrue())

			// all services w/ openshift tls
			feast.Handler.FeatureStore = minimalFeatureStoreWithAllServers()
			err = feast.ApplyDefaults()
			Expect(err).ToNot(HaveOccurred())

			repoConfig, err := getClientRepoConfig(feast.Handler.FeatureStore, emptyMockExtractConfigFromSecret, &feast)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore.Port).To(Equal(HttpsPort))
			Expect(repoConfig.OfflineStore.Scheme).To(Equal(HttpsScheme))
			Expect(repoConfig.OfflineStore.Cert).To(ContainSubstring(string(OfflineFeastType)))
			Expect(repoConfig.OnlineStore.Cert).To(ContainSubstring(string(OnlineFeastType)))
			Expect(repoConfig.Registry.Cert).To(ContainSubstring(string(RegistryFeastType)))

			tls = feast.getTlsConfigs(OfflineFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeTrue())
			Expect(tls.SecretRef).NotTo(BeNil())
			Expect(tls.SecretRef.Name).To(Equal("feast-test-offline-tls"))
			tls = feast.getTlsConfigs(OnlineFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeTrue())
			Expect(tls.SecretRef).NotTo(BeNil())
			Expect(tls.SecretRef.Name).To(Equal("feast-test-online-tls"))
			tls = feast.getTlsConfigs(RegistryFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.SecretRef).NotTo(BeNil())
			Expect(tls.SecretRef.Name).To(Equal("feast-test-registry-tls"))
			Expect(tls.SecretKeyNames).To(Equal(secretKeyNames))
			Expect(tls.IsTLS()).To(BeTrue())
			tls = feast.getTlsConfigs(UIFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.SecretRef).NotTo(BeNil())
			Expect(tls.SecretRef.Name).To(Equal("feast-test-ui-tls"))
			Expect(tls.SecretKeyNames).To(Equal(secretKeyNames))
			Expect(tls.IsTLS()).To(BeTrue())

			Expect(feast.remoteRegistryTls()).To(BeFalse())
			Expect(feast.localRegistryTls()).To(BeTrue())
			Expect(feast.isOpenShiftTls(OfflineFeastType)).To(BeTrue())
			Expect(feast.isOpenShiftTls(OnlineFeastType)).To(BeTrue())
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeTrue())
			Expect(feast.isOpenShiftTls(UIFeastType)).To(BeTrue())
			openshiftTls, err = feast.checkOpenshiftTls()
			Expect(err).ToNot(HaveOccurred())
			Expect(openshiftTls).To(BeTrue())

			// check k8s deployment objects
			feastDeploy := feast.initFeastDeploy(OnlineFeastType)
			err = feast.setDeployment(feastDeploy, OnlineFeastType)
			Expect(err).ToNot(HaveOccurred())
			Expect(feastDeploy.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(feastDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			onlineContainer := GetOnlineContainer(*feastDeploy)
			Expect(onlineContainer).NotTo(BeNil())
			Expect(onlineContainer.Command).To(ContainElements(ContainSubstring("--key")))

			// registry service w/ tls and in an openshift cluster
			feast.Handler.FeatureStore = minimalFeatureStore()
			feast.Handler.FeatureStore.Spec.Services = &feastdevv1.FeatureStoreServices{
				OnlineStore: &feastdevv1.OnlineStore{
					Server: &feastdevv1.ServerConfigs{
						TLS: &feastdevv1.TlsConfigs{},
					},
				},
				UI: &feastdevv1.ServerConfigs{
					TLS: &feastdevv1.TlsConfigs{},
				},
				Registry: &feastdevv1.Registry{
					Local: &feastdevv1.LocalRegistryConfig{
						Server: &feastdevv1.RegistryServerConfigs{
							ServerConfigs: feastdevv1.ServerConfigs{
								TLS: &feastdevv1.TlsConfigs{
									SecretRef: &corev1.LocalObjectReference{},
									SecretKeyNames: feastdevv1.SecretKeyNames{
										TlsCrt: "test.crt",
									},
								},
							},
						},
					},
				},
			}
			err = feast.ApplyDefaults()
			Expect(err).ToNot(HaveOccurred())

			tls = feast.getTlsConfigs(OfflineFeastType)
			Expect(tls).To(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			tls = feast.getTlsConfigs(OnlineFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			tls = feast.getTlsConfigs(UIFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			tls = feast.getTlsConfigs(RegistryFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeTrue())
			Expect(tls.SecretKeyNames).NotTo(Equal(secretKeyNames))
			Expect(getPortStr(tls)).To(Equal("443"))
			Expect(GetTlsPath(RegistryFeastType)).To(Equal("/tls/registry/"))
			Expect(feast.remoteRegistryTls()).To(BeFalse())
			Expect(feast.localRegistryTls()).To(BeTrue())
			Expect(feast.isOpenShiftTls(OfflineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(OnlineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(UIFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeFalse())
			openshiftTls, err = feast.checkOpenshiftTls()
			Expect(err).ToNot(HaveOccurred())
			Expect(openshiftTls).To(BeFalse())

			// all services w/ tls and in an openshift cluster
			feast.Handler.FeatureStore = minimalFeatureStoreWithAllServers()
			disable := true
			feast.Handler.FeatureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					TLS: &feastdevv1.TlsConfigs{
						Disable: &disable,
					},
				},
			}
			feast.Handler.FeatureStore.Spec.Services.UI.TLS = &feastdevv1.TlsConfigs{
				Disable: &disable,
			}
			feast.Handler.FeatureStore.Spec.Services.Registry = &feastdevv1.Registry{
				Local: &feastdevv1.LocalRegistryConfig{
					Server: &feastdevv1.RegistryServerConfigs{
						ServerConfigs: feastdevv1.ServerConfigs{
							TLS: &feastdevv1.TlsConfigs{
								Disable: &disable,
							},
						},
					},
				},
			}
			err = feast.ApplyDefaults()
			Expect(err).ToNot(HaveOccurred())

			repoConfig, err = getClientRepoConfig(feast.Handler.FeatureStore, emptyMockExtractConfigFromSecret, &feast)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig.OfflineStore.Port).To(Equal(HttpsPort))
			Expect(repoConfig.OfflineStore.Scheme).To(Equal(HttpsScheme))
			Expect(repoConfig.OfflineStore.Cert).To(ContainSubstring(string(OfflineFeastType)))
			Expect(repoConfig.OnlineStore.Cert).NotTo(ContainSubstring(string(OnlineFeastType)))
			Expect(repoConfig.Registry.Cert).NotTo(ContainSubstring(string(RegistryFeastType)))

			tls = feast.getTlsConfigs(OfflineFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeTrue())
			Expect(tls.SecretKeyNames).To(Equal(secretKeyNames))
			tls = feast.getTlsConfigs(OnlineFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			Expect(tls.SecretKeyNames).NotTo(Equal(secretKeyNames))
			tls = feast.getTlsConfigs(UIFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			Expect(tls.SecretKeyNames).NotTo(Equal(secretKeyNames))
			tls = feast.getTlsConfigs(RegistryFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			Expect(tls.SecretKeyNames).NotTo(Equal(secretKeyNames))
			Expect(getPortStr(tls)).To(Equal("80"))
			Expect(GetTlsPath(RegistryFeastType)).To(Equal("/tls/registry/"))

			Expect(feast.remoteRegistryTls()).To(BeFalse())
			Expect(feast.localRegistryTls()).To(BeFalse())
			Expect(feast.isOpenShiftTls(OfflineFeastType)).To(BeTrue())
			Expect(feast.isOpenShiftTls(OnlineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(UIFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeFalse())
			openshiftTls, err = feast.checkOpenshiftTls()
			Expect(err).ToNot(HaveOccurred())
			Expect(openshiftTls).To(BeTrue())

			// check k8s service objects
			offlineSvc := feast.initFeastSvc(OfflineFeastType)
			Expect(offlineSvc.Annotations).To(BeEmpty())
			err = feast.setService(offlineSvc, OfflineFeastType, false)
			Expect(err).ToNot(HaveOccurred())
			Expect(offlineSvc.Annotations).NotTo(BeEmpty())
			Expect(offlineSvc.Spec.Ports[0].Name).To(Equal(HttpsScheme))

			onlineSvc := feast.initFeastSvc(OnlineFeastType)
			err = feast.setService(onlineSvc, OnlineFeastType, false)
			Expect(err).ToNot(HaveOccurred())
			Expect(onlineSvc.Annotations).To(BeEmpty())
			Expect(onlineSvc.Spec.Ports[0].Name).To(Equal(HttpScheme))

			uiSvc := feast.initFeastSvc(UIFeastType)
			err = feast.setService(uiSvc, UIFeastType, false)
			Expect(err).ToNot(HaveOccurred())
			Expect(uiSvc.Annotations).To(BeEmpty())
			Expect(uiSvc.Spec.Ports[0].Name).To(Equal(HttpScheme))

			// check k8s deployment objects
			offlineDeploy := feast.initFeastDeploy(OfflineFeastType)
			err = feast.setDeployment(offlineDeploy, OfflineFeastType)
			Expect(err).ToNot(HaveOccurred())
			offlineContainer := GetOfflineContainer(*offlineDeploy)
			Expect(offlineContainer).NotTo(BeNil())
			Expect(offlineContainer.Command).To(ContainElements(ContainSubstring("--key")))

			onlineDeploy := feast.initFeastDeploy(OnlineFeastType)
			err = feast.setDeployment(onlineDeploy, OnlineFeastType)
			Expect(err).ToNot(HaveOccurred())
			onlineContainer = GetOnlineContainer(*onlineDeploy)
			Expect(onlineContainer).NotTo(BeNil())
			Expect(onlineContainer.Command).NotTo(ContainElements(ContainSubstring("--key")))

			registryDeploy := feast.initFeastDeploy(RegistryFeastType)
			err = feast.setDeployment(registryDeploy, RegistryFeastType)
			Expect(err).ToNot(HaveOccurred())
			registryContainer := GetRegistryContainer(*registryDeploy)
			Expect(registryContainer).NotTo(BeNil())
			Expect(registryContainer.Command).NotTo(ContainElements(ContainSubstring("--key")))

			uiDeploy := feast.initFeastDeploy(UIFeastType)
			err = feast.setDeployment(uiDeploy, UIFeastType)
			Expect(err).ToNot(HaveOccurred())
			uiContainer := GetUIContainer(*uiDeploy)
			Expect(uiContainer).NotTo(BeNil())
			Expect(uiContainer.Command).NotTo(ContainElements(ContainSubstring("--key")))

			// Test REST registry server TLS configuration
			feast.Handler.FeatureStore = minimalFeatureStore()
			restEnabled := true
			grpcEnabled := false
			feast.Handler.FeatureStore.Spec.Services = &feastdevv1.FeatureStoreServices{
				Registry: &feastdevv1.Registry{
					Local: &feastdevv1.LocalRegistryConfig{
						Server: &feastdevv1.RegistryServerConfigs{
							ServerConfigs: feastdevv1.ServerConfigs{},
							RestAPI:       &restEnabled,
							GRPC:          &grpcEnabled,
						},
					},
				},
			}
			testSetIsOpenShift()
			err = feast.ApplyDefaults()
			Expect(err).ToNot(HaveOccurred())

			tls = feast.getTlsConfigs(RegistryFeastType)
			Expect(tls).NotTo(BeNil())
			Expect(tls.IsTLS()).To(BeTrue())
			Expect(tls.SecretRef).NotTo(BeNil())
			Expect(tls.SecretRef.Name).To(Equal("feast-test-registry-rest-tls"))
			Expect(tls.SecretKeyNames).To(Equal(secretKeyNames))
			Expect(getPortStr(tls)).To(Equal("443"))
			Expect(GetTlsPath(RegistryFeastType)).To(Equal("/tls/registry/"))

			registryRestSvc := feast.initFeastRestSvc(RegistryFeastType)
			err = feast.setService(registryRestSvc, RegistryFeastType, true)
			Expect(err).ToNot(HaveOccurred())
			Expect(registryRestSvc.Annotations).NotTo(BeEmpty())
			Expect(registryRestSvc.Spec.Ports[0].Name).To(Equal(HttpsScheme))

			feastDeploy = feast.initFeastDeploy(RegistryFeastType)
			err = feast.setDeployment(feastDeploy, RegistryFeastType)
			Expect(err).ToNot(HaveOccurred())
			registryContainer = GetRegistryContainer(*feastDeploy)
			Expect(registryContainer).NotTo(BeNil())
			Expect(registryContainer.Command).To(ContainElements(ContainSubstring("--key")))
		})
	})
})
