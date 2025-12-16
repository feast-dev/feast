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

package controller

import (
	"context"
	"encoding/base64"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("FeatureStore Controller - Feast service TLS", func() {
	Context("When reconciling a FeatureStore resource", func() {
		const resourceName = "test-tls"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1.FeatureStore{}
		localRef := corev1.LocalObjectReference{Name: "test"}
		tlsConfigs := &feastdevv1.TlsConfigs{
			SecretRef: &localRef,
		}
		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := &feastdevv1.FeatureStore{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: feastdevv1.FeatureStoreSpec{
						FeastProject: feastProject,
						Services: &feastdevv1.FeatureStoreServices{
							OnlineStore: &feastdevv1.OnlineStore{
								Server: &feastdevv1.ServerConfigs{
									TLS: tlsConfigs,
								},
							},
							OfflineStore: &feastdevv1.OfflineStore{
								Server: &feastdevv1.ServerConfigs{
									TLS: tlsConfigs,
								},
							},
							Registry: &feastdevv1.Registry{
								Local: &feastdevv1.LocalRegistryConfig{
									Server: &feastdevv1.RegistryServerConfigs{
										ServerConfigs: feastdevv1.ServerConfigs{
											TLS: tlsConfigs,
										},
									},
								},
							},
							UI: &feastdevv1.ServerConfigs{
								TLS: tlsConfigs,
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})
		AfterEach(func() {
			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance FeatureStore")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource := &feastdevv1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast := services.FeastServices{
				Handler: handler.FeastHandler{
					Client:       controllerReconciler.Client,
					Context:      ctx,
					Scheme:       controllerReconciler.Scheme,
					FeatureStore: resource,
				},
			}

			Expect(resource.Status).NotTo(BeNil())
			Expect(resource.Status.FeastVersion).To(Equal(feastversion.FeastVersion))
			Expect(resource.Status.ClientConfigMap).To(Equal(feast.GetFeastServiceName(services.ClientFeastType)))
			Expect(resource.Status.Applied.FeastProject).To(Equal(resource.Spec.FeastProject))
			Expect(resource.Status.Applied.Services).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())

			Expect(resource.Status.ServiceHostnames.OfflineStore).To(Equal(feast.GetFeastServiceName(services.OfflineFeastType) + "." + resource.Namespace + domainTls))
			Expect(resource.Status.ServiceHostnames.OnlineStore).To(Equal(feast.GetFeastServiceName(services.OnlineFeastType) + "." + resource.Namespace + domainTls))
			Expect(resource.Status.ServiceHostnames.Registry).To(Equal(feast.GetFeastServiceName(services.RegistryFeastType) + "." + resource.Namespace + domainTls))

			Expect(resource.Status.Conditions).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.ReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).To(Equal(feastdevv1.DeploymentNotAvailableReason))
			Expect(cond.Type).To(Equal(feastdevv1.ReadyType))
			Expect(cond.Message).To(Equal(feastdevv1.DeploymentNotAvailableMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.RegistryReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1.RegistryReadyType))
			Expect(cond.Message).To(Equal(feastdevv1.RegistryReadyMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.ClientReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1.ClientReadyType))
			Expect(cond.Message).To(Equal(feastdevv1.ClientReadyMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.OfflineStoreReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1.OfflineStoreReadyType))
			Expect(cond.Message).To(Equal(feastdevv1.OfflineStoreReadyMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.OnlineStoreReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1.OnlineStoreReadyType))
			Expect(cond.Message).To(Equal(feastdevv1.OnlineStoreReadyMessage))

			Expect(resource.Status.Phase).To(Equal(feastdevv1.PendingPhase))

			// check deployment
			deploy := &appsv1.Deployment{}
			objMeta := feast.GetObjectMeta()
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Replicas).To(Equal(int32Ptr(1)))
			Expect(controllerutil.HasControllerReference(deploy)).To(BeTrue())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(4))
			svc := &corev1.Service{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(svc)).To(BeTrue())
			Expect(svc.Spec.Ports[0].TargetPort).To(Equal(intstr.FromInt(int(services.FeastServiceConstants[services.RegistryFeastType].TargetHttpsPort))))
		})

		It("should properly encode a feature_store.yaml config", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource := &feastdevv1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast := services.FeastServices{
				Handler: handler.FeastHandler{
					Client:       controllerReconciler.Client,
					Context:      ctx,
					Scheme:       controllerReconciler.Scheme,
					FeatureStore: resource,
				},
			}

			req, err := labels.NewRequirement(services.NameLabelKey, selection.Equals, []string{resource.Name})
			Expect(err).NotTo(HaveOccurred())
			labelSelector := labels.NewSelector().Add(*req)
			listOpts := &client.ListOptions{Namespace: resource.Namespace, LabelSelector: labelSelector}
			deployList := appsv1.DeploymentList{}
			err = k8sClient.List(ctx, &deployList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(deployList.Items).To(HaveLen(1))

			svcList := corev1.ServiceList{}
			err = k8sClient.List(ctx, &svcList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(svcList.Items).To(HaveLen(4))

			cmList := corev1.ConfigMapList{}
			err = k8sClient.List(ctx, &cmList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmList.Items).To(HaveLen(1))

			// check deployment
			deploy := &appsv1.Deployment{}
			objMeta := feast.GetObjectMeta()
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(4))
			registryContainer := services.GetRegistryContainer(*deploy)
			Expect(registryContainer.Env).To(HaveLen(1))
			env := getFeatureStoreYamlEnvVar(registryContainer.Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err := feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err := base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			testConfig := feast.GetDefaultRepoConfig()
			testConfig.OfflineStore = services.OfflineStoreConfig{
				Type: services.OfflineFilePersistenceDaskConfigType,
			}
			Expect(repoConfig).To(Equal(&testConfig))

			// check offline config
			offlineContainer := services.GetOfflineContainer(*deploy)
			Expect(offlineContainer.Env).To(HaveLen(1))
			env = getFeatureStoreYamlEnvVar(offlineContainer.Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfigOffline := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOffline)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfigOffline).To(Equal(&testConfig))

			// check online config
			onlineContainer := services.GetOnlineContainer(*deploy)
			Expect(onlineContainer.Env).To(HaveLen(1))
			env = getFeatureStoreYamlEnvVar(onlineContainer.Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfigOnline := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOnline)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfigOnline).To(Equal(&testConfig))
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(1))

			// check client config
			cm := &corev1.ConfigMap{}
			name := feast.GetFeastServiceName(services.ClientFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: resource.Namespace,
			},
				cm)
			Expect(err).NotTo(HaveOccurred())
			repoConfigClient := &services.RepoConfig{}
			err = yaml.Unmarshal([]byte(cm.Data[services.FeatureStoreYamlCmKey]), repoConfigClient)
			Expect(err).NotTo(HaveOccurred())
			offlineRemote := services.OfflineStoreConfig{
				Host:   fmt.Sprintf("feast-%s-offline.default.svc.cluster.local", resourceName),
				Type:   services.OfflineRemoteConfigType,
				Port:   services.HttpsPort,
				Scheme: services.HttpsScheme,
				Cert:   services.GetTlsPath(services.OfflineFeastType) + "tls.crt",
			}
			regRemote := services.RegistryConfig{
				RegistryType: services.RegistryRemoteConfigType,
				Path:         fmt.Sprintf("feast-%s-registry.default.svc.cluster.local:443", resourceName),
				Cert:         services.GetTlsPath(services.RegistryFeastType) + "tls.crt",
			}
			clientConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1.SerializationVersion,
				OfflineStore:                  offlineRemote,
				OnlineStore: services.OnlineStoreConfig{
					Path: fmt.Sprintf("https://feast-%s-online.default.svc.cluster.local:443", resourceName),
					Type: services.OnlineRemoteConfigType,
					Cert: services.GetTlsPath(services.OnlineFeastType) + "tls.crt",
				},
				Registry:    regRemote,
				AuthzConfig: noAuthzConfig(),
			}
			Expect(repoConfigClient).To(Equal(clientConfig))

			// change tls and reconcile
			resourceNew := resource.DeepCopy()
			disable := true
			remoteRegHost := "test.other-ns:443"
			resourceNew.Spec = feastdevv1.FeatureStoreSpec{
				FeastProject: feastProject,
				Services: &feastdevv1.FeatureStoreServices{
					OnlineStore: &feastdevv1.OnlineStore{
						Server: &feastdevv1.ServerConfigs{
							TLS: &feastdevv1.TlsConfigs{
								Disable: &disable,
							},
						},
					},
					OfflineStore: &feastdevv1.OfflineStore{
						Server: &feastdevv1.ServerConfigs{
							TLS: tlsConfigs,
						},
					},
					Registry: &feastdevv1.Registry{
						Remote: &feastdevv1.RemoteRegistryConfig{
							Hostname: &remoteRegHost,
							TLS: &feastdevv1.TlsRemoteRegistryConfigs{
								ConfigMapRef: localRef,
								CertName:     "remote.crt",
							},
						},
					},
				},
			}
			err = k8sClient.Update(ctx, resourceNew)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource = &feastdevv1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast.Handler.FeatureStore = resource

			// check registry
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(2))

			// check offline config
			offlineContainer = services.GetOfflineContainer(*deploy)
			env = getFeatureStoreYamlEnvVar(offlineContainer.Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfigOffline = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOffline)
			Expect(err).NotTo(HaveOccurred())
			regRemote = services.RegistryConfig{
				RegistryType: services.RegistryRemoteConfigType,
				Path:         remoteRegHost,
				Cert:         services.GetTlsPath(services.RegistryFeastType) + "remote.crt",
			}
			testConfig.Registry = regRemote
			Expect(repoConfigOffline).To(Equal(&testConfig))

			// check online config
			onlineContainer = services.GetOnlineContainer(*deploy)
			env = getFeatureStoreYamlEnvVar(onlineContainer.Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())

			repoConfigOnline = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOnline)
			Expect(err).NotTo(HaveOccurred())
			testConfig.Registry = regRemote
			Expect(repoConfigOnline).To(Equal(&testConfig))
		})
	})
})

var _ = Describe("Test mountCustomCABundle functionality", func() {
	const resourceName = "test-cabundle"
	const feastProject = "test_cabundle"
	const configMapName = "odh-trusted-ca-bundle"
	const caBundleAnnotation = "config.openshift.io/inject-trusted-cabundle"
	const tlsPathCustomCABundle = "/etc/pki/tls/custom-certs/ca-bundle.crt"

	ctx := context.Background()
	nsName := types.NamespacedName{
		Name:      resourceName,
		Namespace: "default",
	}

	fs := &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: nsName.Namespace,
		},
		Spec: feastdevv1.FeatureStoreSpec{
			FeastProject: feastProject,
			Services: &feastdevv1.FeatureStoreServices{
				Registry:     &feastdevv1.Registry{Local: &feastdevv1.LocalRegistryConfig{Server: &feastdevv1.RegistryServerConfigs{ServerConfigs: feastdevv1.ServerConfigs{}}}},
				OnlineStore:  &feastdevv1.OnlineStore{Server: &feastdevv1.ServerConfigs{}},
				OfflineStore: &feastdevv1.OfflineStore{Server: &feastdevv1.ServerConfigs{}},
				UI:           &feastdevv1.ServerConfigs{},
			},
		},
	}

	AfterEach(func() {
		By("cleaning up FeatureStore and ConfigMap")
		_ = k8sClient.Delete(ctx, &feastdevv1.FeatureStore{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: nsName.Namespace}})
		_ = k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: nsName.Namespace}})
	})

	It("should mount CA bundle volume and mounts in containers when ConfigMap exists", func() {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      configMapName,
				Namespace: nsName.Namespace,
				Labels: map[string]string{
					caBundleAnnotation: "true",
				},
			},
		}
		Expect(k8sClient.Create(ctx, cm.DeepCopy())).To(Succeed())
		Expect(k8sClient.Create(ctx, fs.DeepCopy())).To(Succeed())

		controllerReconciler := &FeatureStoreReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}

		_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: nsName,
		})
		Expect(err).NotTo(HaveOccurred())

		resource := &feastdevv1.FeatureStore{}
		err = k8sClient.Get(ctx, nsName, resource)
		Expect(err).NotTo(HaveOccurred())

		feast := services.FeastServices{
			Handler: handler.FeastHandler{
				Client:       controllerReconciler.Client,
				Context:      ctx,
				Scheme:       controllerReconciler.Scheme,
				FeatureStore: resource,
			},
		}

		deploy := &appsv1.Deployment{}
		objMeta := feast.GetObjectMeta()
		err = k8sClient.Get(ctx, types.NamespacedName{
			Name:      objMeta.Name,
			Namespace: objMeta.Namespace,
		}, deploy)
		Expect(err).NotTo(HaveOccurred())

		Expect(deploy.Spec.Template.Spec.Volumes).To(ContainElement(HaveField("Name", configMapName)))
		for _, container := range deploy.Spec.Template.Spec.Containers {
			Expect(container.VolumeMounts).To(ContainElement(SatisfyAll(
				HaveField("Name", configMapName),
				HaveField("MountPath", tlsPathCustomCABundle),
			)))
		}
	})

	It("should not mount CA bundle volume or container mounts when ConfigMap is absent", func() {
		Expect(k8sClient.Create(ctx, fs.DeepCopy())).To(Succeed())

		controllerReconciler := &FeatureStoreReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}

		_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: nsName,
		})
		Expect(err).NotTo(HaveOccurred())

		resource := &feastdevv1.FeatureStore{}
		err = k8sClient.Get(ctx, nsName, resource)
		Expect(err).NotTo(HaveOccurred())

		feast := services.FeastServices{
			Handler: handler.FeastHandler{
				Client:       controllerReconciler.Client,
				Context:      ctx,
				Scheme:       controllerReconciler.Scheme,
				FeatureStore: resource,
			},
		}

		deploy := &appsv1.Deployment{}
		objMeta := feast.GetObjectMeta()
		err = k8sClient.Get(ctx, types.NamespacedName{
			Name:      objMeta.Name,
			Namespace: objMeta.Namespace,
		}, deploy)
		Expect(err).NotTo(HaveOccurred())

		Expect(deploy.Spec.Template.Spec.Volumes).NotTo(ContainElement(HaveField("Name", configMapName)))
		for _, container := range deploy.Spec.Template.Spec.Containers {
			Expect(container.VolumeMounts).NotTo(ContainElement(HaveField("Name", configMapName)))
		}
	})
})
