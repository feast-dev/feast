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

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func ptr[T any](v T) *T {
	return &v
}

func (feast *FeastServices) refreshFeatureStore(ctx context.Context, key types.NamespacedName) {
	fs := &feastdevv1alpha1.FeatureStore{}
	Expect(k8sClient.Get(ctx, key, fs)).To(Succeed())
	feast.Handler.FeatureStore = fs
}

func applySpecToStatus(fs *feastdevv1alpha1.FeatureStore) {
	fs.Status.Applied.Services = fs.Spec.Services.DeepCopy()
	fs.Status.Applied.FeastProject = fs.Spec.FeastProject
	Expect(k8sClient.Status().Update(context.Background(), fs)).To(Succeed())
}

var _ = Describe("Registry Service", func() {
	var (
		featureStore       *feastdevv1alpha1.FeatureStore
		feast              *FeastServices
		typeNamespacedName types.NamespacedName
		ctx                context.Context
	)

	var setFeatureStoreServerConfig = func(grpcEnabled, restEnabled bool) {
		featureStore.Spec.Services.Registry.Local.Server.GRPC = ptr(grpcEnabled)
		featureStore.Spec.Services.Registry.Local.Server.RestAPI = ptr(restEnabled)
		Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
		Expect(feast.ApplyDefaults()).To(Succeed())
		applySpecToStatus(featureStore)
		feast.refreshFeatureStore(ctx, typeNamespacedName)
	}

	BeforeEach(func() {
		ctx = context.Background()
		typeNamespacedName = types.NamespacedName{
			Name:      "testfeaturestore",
			Namespace: "default",
		}

		featureStore = &feastdevv1alpha1.FeatureStore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      typeNamespacedName.Name,
				Namespace: typeNamespacedName.Namespace,
			},
			Spec: feastdevv1alpha1.FeatureStoreSpec{
				FeastProject: "testproject",
				Services: &feastdevv1alpha1.FeatureStoreServices{
					Registry: &feastdevv1alpha1.Registry{
						Local: &feastdevv1alpha1.LocalRegistryConfig{
							Server: &feastdevv1alpha1.RegistryServerConfigs{
								ServerConfigs: feastdevv1alpha1.ServerConfigs{
									ContainerConfigs: feastdevv1alpha1.ContainerConfigs{
										DefaultCtrConfigs: feastdevv1alpha1.DefaultCtrConfigs{
											Image: ptr("test-image"),
										},
									},
								},
								GRPC:    ptr(true),
								RestAPI: ptr(false),
							},
						},
					},
				},
			},
		}

		Expect(k8sClient.Create(ctx, featureStore)).To(Succeed())
		applySpecToStatus(featureStore)

		feast = &FeastServices{
			Handler: handler.FeastHandler{
				Client:       k8sClient,
				Context:      ctx,
				Scheme:       k8sClient.Scheme(),
				FeatureStore: featureStore,
			},
		}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, featureStore)).To(Succeed())
	})

	Describe("Probe Handler Configuration", func() {
		It("should configure TCP socket probe when gRPC is enabled", func() {
			setFeatureStoreServerConfig(true, false)
			probeHandler := feast.getProbeHandler(RegistryFeastType, featureStore.Spec.Services.Registry.Local.Server.TLS)
			Expect(probeHandler.TCPSocket).NotTo(BeNil())
			Expect(probeHandler.TCPSocket.Port).To(Equal(intstr.FromInt(int(FeastServiceConstants[RegistryFeastType].TargetHttpPort))))
		})

		It("should configure HTTP GET probe when REST is enabled", func() {
			setFeatureStoreServerConfig(false, true)
			probeHandler := feast.getProbeHandler(RegistryFeastType, featureStore.Spec.Services.Registry.Local.Server.TLS)
			Expect(probeHandler.HTTPGet).NotTo(BeNil())
			Expect(probeHandler.HTTPGet.Port).To(Equal(intstr.FromInt(int(FeastServiceConstants[RegistryFeastType].TargetRestHttpPort))))
		})
	})

	Describe("Registry Server Configuration", func() {
		It("should enable both gRPC and REST", func() {
			setFeatureStoreServerConfig(true, true)
			Expect(feast.isRegistryGrpcEnabled()).To(BeTrue())
			Expect(feast.isRegistryRestEnabled()).To(BeTrue())
		})

		It("should create both gRPC and REST services", func() {
			setFeatureStoreServerConfig(true, true)
			Expect(feast.deployFeastServiceByType(RegistryFeastType)).To(Succeed())
			Expect(feast.initFeastSvc(RegistryFeastType)).NotTo(BeNil())
			Expect(feast.initFeastRestSvc(RegistryFeastType)).NotTo(BeNil())
		})

		It("should enable only gRPC", func() {
			setFeatureStoreServerConfig(true, false)
			Expect(feast.isRegistryGrpcEnabled()).To(BeTrue())
			Expect(feast.isRegistryRestEnabled()).To(BeFalse())
		})

		It("should create only gRPC service and not REST service", func() {
			setFeatureStoreServerConfig(true, false)
			Expect(feast.deployFeastServiceByType(RegistryFeastType)).To(Succeed())
			Expect(feast.initFeastSvc(RegistryFeastType)).NotTo(BeNil())
		})
	})

	Describe("Container Ports Configuration", func() {
		It("should configure correct gRPC container ports", func() {
			setFeatureStoreServerConfig(true, false)
			Expect(feast.deployFeastServiceByType(RegistryFeastType)).To(Succeed())
			deployment := feast.initFeastDeploy()
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment)).To(Succeed())

			ports := deployment.Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(1))
			Expect(ports[0].ContainerPort).To(Equal(FeastServiceConstants[RegistryFeastType].TargetHttpPort))
			Expect(ports[0].Name).To(Equal(string(RegistryFeastType)))
		})

		It("should configure correct REST container ports", func() {
			setFeatureStoreServerConfig(false, true)
			Expect(feast.deployFeastServiceByType(RegistryFeastType)).To(Succeed())
			deployment := feast.initFeastDeploy()
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment)).To(Succeed())

			ports := deployment.Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(1))
			Expect(ports[0].ContainerPort).To(Equal(FeastServiceConstants[RegistryFeastType].TargetRestHttpPort))
			Expect(ports[0].Name).To(Equal(string(RegistryFeastType) + "-rest"))

			Expect(deployment.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deployment.Spec.Template.Spec.Containers[0].Ports).To(HaveLen(1))
			Expect(deployment.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort).To(Equal(FeastServiceConstants[RegistryFeastType].TargetRestHttpPort))
			Expect(deployment.Spec.Template.Spec.Containers[0].Ports[0].Name).To(Equal(string(RegistryFeastType) + "-rest"))
		})

		It("should configure correct ports for both services", func() {
			setFeatureStoreServerConfig(true, true)
			Expect(feast.deployFeastServiceByType(RegistryFeastType)).To(Succeed())

			deployment := feast.initFeastDeploy()
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment)).To(Succeed())

			ports := deployment.Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(2))
			Expect(ports[0].ContainerPort).To(Equal(FeastServiceConstants[RegistryFeastType].TargetHttpPort))
			Expect(ports[0].Name).To(Equal(string(RegistryFeastType)))
			Expect(ports[1].ContainerPort).To(Equal(FeastServiceConstants[RegistryFeastType].TargetRestHttpPort))
			Expect(ports[1].Name).To(Equal(string(RegistryFeastType) + "-rest"))
		})
	})

	Describe("NodeSelector Configuration", func() {
		It("should apply NodeSelector to pod spec when configured", func() {
			// Set NodeSelector for registry service
			nodeSelector := map[string]string{
				"kubernetes.io/os": "linux",
				"node-type":        "compute",
			}
			featureStore.Spec.Services.Registry.Local.Server.ContainerConfigs.OptionalCtrConfigs.NodeSelector = &nodeSelector
			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			// Create deployment and verify NodeSelector is applied
			deployment := feast.initFeastDeploy()
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment)).To(Succeed())

			// Verify NodeSelector is applied to pod spec
			expectedNodeSelector := map[string]string{
				"kubernetes.io/os": "linux",
				"node-type":        "compute",
			}
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(expectedNodeSelector))
		})

		It("should merge NodeSelectors from multiple services", func() {
			// Set NodeSelector for registry service
			registryNodeSelector := map[string]string{
				"kubernetes.io/os": "linux",
				"node-type":        "compute",
			}
			featureStore.Spec.Services.Registry.Local.Server.ContainerConfigs.OptionalCtrConfigs.NodeSelector = &registryNodeSelector

			// Set NodeSelector for online store service
			onlineNodeSelector := map[string]string{
				"node-type": "online",
				"zone":      "us-west-1a",
			}
			featureStore.Spec.Services.OnlineStore = &feastdevv1alpha1.OnlineStore{
				Server: &feastdevv1alpha1.ServerConfigs{
					ContainerConfigs: feastdevv1alpha1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1alpha1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
						OptionalCtrConfigs: feastdevv1alpha1.OptionalCtrConfigs{
							NodeSelector: &onlineNodeSelector,
						},
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			// Create deployment and verify merged NodeSelector is applied
			deployment := feast.initFeastDeploy()
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment)).To(Succeed())

			// Verify NodeSelector merges all service selectors (online overrides registry for node-type)
			expectedNodeSelector := map[string]string{
				"kubernetes.io/os": "linux",
				"node-type":        "online",
				"zone":             "us-west-1a",
			}
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(expectedNodeSelector))
		})

		It("should merge operator NodeSelector with existing selectors (ops team scenario)", func() {
			// Simulate existing node selector from ops team
			existingNodeSelector := map[string]string{
				"team":        "ml",
				"environment": "prod",
			}

			// Set NodeSelector for UI service
			uiNodeSelector := map[string]string{
				"node-type": "ui",
			}
			featureStore.Spec.Services.UI = &feastdevv1alpha1.ServerConfigs{
				ContainerConfigs: feastdevv1alpha1.ContainerConfigs{
					DefaultCtrConfigs: feastdevv1alpha1.DefaultCtrConfigs{
						Image: ptr("test-image"),
					},
					OptionalCtrConfigs: feastdevv1alpha1.OptionalCtrConfigs{
						NodeSelector: &uiNodeSelector,
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			// Create deployment and simulate existing node selector
			deployment := feast.initFeastDeploy()
			deployment.Spec.Template.Spec.NodeSelector = existingNodeSelector
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment)).To(Succeed())

			// Verify NodeSelector merges existing and operator selectors
			expectedNodeSelector := map[string]string{
				"team":        "ml",
				"environment": "prod",
				"node-type":   "ui",
			}
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(expectedNodeSelector))
		})

		It("should apply UI service NodeSelector when UI has highest precedence", func() {
			// Set NodeSelector for online service
			onlineNodeSelector := map[string]string{
				"node-type": "online",
			}
			featureStore.Spec.Services.OnlineStore = &feastdevv1alpha1.OnlineStore{
				Server: &feastdevv1alpha1.ServerConfigs{
					ContainerConfigs: feastdevv1alpha1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1alpha1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
						OptionalCtrConfigs: feastdevv1alpha1.OptionalCtrConfigs{
							NodeSelector: &onlineNodeSelector,
						},
					},
				},
			}

			// Set NodeSelector for UI service (should win)
			uiNodeSelector := map[string]string{
				"node-type": "ui",
				"zone":      "us-east-1",
			}
			featureStore.Spec.Services.UI = &feastdevv1alpha1.ServerConfigs{
				ContainerConfigs: feastdevv1alpha1.ContainerConfigs{
					DefaultCtrConfigs: feastdevv1alpha1.DefaultCtrConfigs{
						Image: ptr("test-image"),
					},
					OptionalCtrConfigs: feastdevv1alpha1.OptionalCtrConfigs{
						NodeSelector: &uiNodeSelector,
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			// Create deployment and verify UI service selector is applied
			deployment := feast.initFeastDeploy()
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment)).To(Succeed())

			// Verify NodeSelector is applied with UI service's selector (UI wins)
			expectedNodeSelector := map[string]string{
				"node-type": "ui",
				"zone":      "us-east-1",
			}
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(expectedNodeSelector))
		})

		It("should handle empty NodeSelector gracefully", func() {
			// Set empty NodeSelector
			emptyNodeSelector := map[string]string{}
			featureStore.Spec.Services.Registry.Local.Server.ContainerConfigs.OptionalCtrConfigs.NodeSelector = &emptyNodeSelector
			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			// Create deployment and verify no NodeSelector is applied (empty selector)
			deployment := feast.initFeastDeploy()
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment)).To(Succeed())

			// Verify no NodeSelector is applied (empty selector)
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(BeEmpty())
		})
	})
})
