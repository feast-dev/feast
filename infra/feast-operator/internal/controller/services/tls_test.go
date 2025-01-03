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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
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
		utilruntime.Must(feastdevv1alpha1.AddToScheme(scheme))

		secretKeyNames := feastdevv1alpha1.SecretKeyNames{
			TlsCrt: "tls.crt",
			TlsKey: "tls.key",
		}

		It("should set default TLS configs", func() {
			By("Having the created resource")

			// registry service w/o tls
			feast := FeastServices{
				Handler: handler.FeastHandler{
					FeatureStore: minimalFeatureStore(),
					Scheme:       scheme,
				},
			}
			err := feast.ApplyDefaults()
			Expect(err).To(BeNil())

			tls := feast.getTlsConfigs(RegistryFeastType)
			Expect(tls).To(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			Expect(getPortStr(tls)).To(Equal("80"))

			Expect(feast.remoteRegistryTls()).To(BeFalse())
			Expect(feast.localRegistryTls()).To(BeFalse())
			Expect(feast.isOpenShiftTls(OfflineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(OnlineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeFalse())
			openshiftTls, err := feast.checkOpenshiftTls()
			Expect(err).To(BeNil())
			Expect(openshiftTls).To(BeFalse())

			// registry service w/ openshift tls
			testSetIsOpenShift()
			feast.Handler.FeatureStore = minimalFeatureStore()
			err = feast.ApplyDefaults()
			Expect(err).To(BeNil())

			tls = feast.getTlsConfigs(OfflineFeastType)
			Expect(tls).To(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			tls = feast.getTlsConfigs(OnlineFeastType)
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
			Expect(feast.isOpenShiftTls(OnlineFeastType)).To(BeFalse())
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeTrue())
			openshiftTls, err = feast.checkOpenshiftTls()
			Expect(err).To(BeNil())
			Expect(openshiftTls).To(BeTrue())

			// all services w/ openshift tls
			feast.Handler.FeatureStore = minimalFeatureStoreWithAllServices()
			err = feast.ApplyDefaults()
			Expect(err).To(BeNil())

			repoConfig, err := getClientRepoConfig(feast.Handler.FeatureStore, emptyMockExtractConfigFromSecret)
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

			Expect(feast.remoteRegistryTls()).To(BeFalse())
			Expect(feast.localRegistryTls()).To(BeTrue())
			Expect(feast.isOpenShiftTls(OfflineFeastType)).To(BeTrue())
			Expect(feast.isOpenShiftTls(OnlineFeastType)).To(BeTrue())
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeTrue())
			openshiftTls, err = feast.checkOpenshiftTls()
			Expect(err).To(BeNil())
			Expect(openshiftTls).To(BeTrue())

			// check k8s deployment objects
			feastDeploy := feast.initFeastDeploy()
			err = feast.setDeployment(feastDeploy)
			Expect(err).To(BeNil())
			Expect(feastDeploy.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(feastDeploy.Spec.Template.Spec.Containers).To(HaveLen(3))
			Expect(feastDeploy.Spec.Template.Spec.Containers[0].Command).To(ContainElements(ContainSubstring("--key")))
			Expect(feastDeploy.Spec.Template.Spec.Containers[1].Command).To(ContainElements(ContainSubstring("--key")))
			Expect(feastDeploy.Spec.Template.Spec.Containers[2].Command).To(ContainElements(ContainSubstring("--key")))
			Expect(feastDeploy.Spec.Template.Spec.Volumes).To(HaveLen(4))

			// registry service w/ tls and in an openshift cluster
			feast.Handler.FeatureStore = minimalFeatureStore()
			feast.Handler.FeatureStore.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
				OnlineStore: &feastdevv1alpha1.OnlineStore{
					TLS: &feastdevv1alpha1.TlsConfigs{},
				},
				Registry: &feastdevv1alpha1.Registry{
					Local: &feastdevv1alpha1.LocalRegistryConfig{
						TLS: &feastdevv1alpha1.TlsConfigs{
							SecretRef: &corev1.LocalObjectReference{},
							SecretKeyNames: feastdevv1alpha1.SecretKeyNames{
								TlsCrt: "test.crt",
							},
						},
					},
				},
			}
			err = feast.ApplyDefaults()
			Expect(err).To(BeNil())

			tls = feast.getTlsConfigs(OfflineFeastType)
			Expect(tls).To(BeNil())
			Expect(tls.IsTLS()).To(BeFalse())
			tls = feast.getTlsConfigs(OnlineFeastType)
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
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeFalse())
			openshiftTls, err = feast.checkOpenshiftTls()
			Expect(err).To(BeNil())
			Expect(openshiftTls).To(BeFalse())

			// all services w/ tls and in an openshift cluster
			feast.Handler.FeatureStore = minimalFeatureStoreWithAllServices()
			disable := true
			feast.Handler.FeatureStore.Spec.Services.OnlineStore.TLS = &feastdevv1alpha1.TlsConfigs{
				Disable: &disable,
			}
			feast.Handler.FeatureStore.Spec.Services.Registry = &feastdevv1alpha1.Registry{
				Local: &feastdevv1alpha1.LocalRegistryConfig{
					TLS: &feastdevv1alpha1.TlsConfigs{
						Disable: &disable,
					},
				},
			}
			err = feast.ApplyDefaults()
			Expect(err).To(BeNil())

			repoConfig, err = getClientRepoConfig(feast.Handler.FeatureStore, emptyMockExtractConfigFromSecret)
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
			Expect(feast.isOpenShiftTls(RegistryFeastType)).To(BeFalse())
			openshiftTls, err = feast.checkOpenshiftTls()
			Expect(err).To(BeNil())
			Expect(openshiftTls).To(BeTrue())

			// check k8s service objects
			offlineSvc := feast.initFeastSvc(OfflineFeastType)
			Expect(offlineSvc.Annotations).To(BeEmpty())
			err = feast.setService(offlineSvc, OfflineFeastType)
			Expect(err).To(BeNil())
			Expect(offlineSvc.Annotations).NotTo(BeEmpty())
			Expect(offlineSvc.Spec.Ports[0].Name).To(Equal(HttpsScheme))

			onlineSvc := feast.initFeastSvc(OnlineFeastType)
			err = feast.setService(onlineSvc, OnlineFeastType)
			Expect(err).To(BeNil())
			Expect(onlineSvc.Annotations).To(BeEmpty())
			Expect(onlineSvc.Spec.Ports[0].Name).To(Equal(HttpScheme))

			// check k8s deployment objects
			feastDeploy = feast.initFeastDeploy()
			err = feast.setDeployment(feastDeploy)
			Expect(err).To(BeNil())
			Expect(feastDeploy.Spec.Template.Spec.Containers).To(HaveLen(3))
			Expect(GetOfflineContainer(feastDeploy.Spec.Template.Spec.Containers)).NotTo(BeNil())
			Expect(feastDeploy.Spec.Template.Spec.Volumes).To(HaveLen(2))

			Expect(GetRegistryContainer(feastDeploy.Spec.Template.Spec.Containers).Command).NotTo(ContainElements(ContainSubstring("--key")))
			Expect(GetRegistryContainer(feastDeploy.Spec.Template.Spec.Containers).VolumeMounts).To(HaveLen(1))
			Expect(GetOfflineContainer(feastDeploy.Spec.Template.Spec.Containers).Command).To(ContainElements(ContainSubstring("--key")))
			Expect(GetOfflineContainer(feastDeploy.Spec.Template.Spec.Containers).VolumeMounts).To(HaveLen(2))
			Expect(GetOnlineContainer(feastDeploy.Spec.Template.Spec.Containers).Command).NotTo(ContainElements(ContainSubstring("--key")))
			Expect(GetOnlineContainer(feastDeploy.Spec.Template.Spec.Containers).VolumeMounts).To(HaveLen(1))
		})
	})
})
