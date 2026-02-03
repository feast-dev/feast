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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func ptr[T any](v T) *T {
	return &v
}

func findPvcVolume(volumes []corev1.Volume) *corev1.Volume {
	for i := range volumes {
		if volumes[i].PersistentVolumeClaim != nil {
			return &volumes[i]
		}
	}
	return nil
}

func (feast *FeastServices) refreshFeatureStore(ctx context.Context, key types.NamespacedName) {
	fs := &feastdevv1.FeatureStore{}
	Expect(k8sClient.Get(ctx, key, fs)).To(Succeed())
	feast.Handler.FeatureStore = fs
}

func applySpecToStatus(fs *feastdevv1.FeatureStore) {
	if fs.Spec.Services != nil {
		fs.Status.Applied.Services = fs.Spec.Services.DeepCopy()
	}
	fs.Status.Applied.FeastProject = fs.Spec.FeastProject
	Expect(k8sClient.Status().Update(context.Background(), fs)).To(Succeed())
}

var _ = Describe("Registry Service", func() {
	var (
		featureStore       *feastdevv1.FeatureStore
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
		// Avoid cross-test contamination from tls tests toggling the global OpenShift flag.
		isOpenShift = false
		ctx = context.Background()
		typeNamespacedName = types.NamespacedName{
			Name:      "testfeaturestore",
			Namespace: "default",
		}

		featureStore = &feastdevv1.FeatureStore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      typeNamespacedName.Name,
				Namespace: typeNamespacedName.Namespace,
			},
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: "testproject",
				Services: &feastdevv1.FeatureStoreServices{
					Registry: &feastdevv1.Registry{
						Local: &feastdevv1.LocalRegistryConfig{
							Server: &feastdevv1.RegistryServerConfigs{
								ServerConfigs: feastdevv1.ServerConfigs{
									ContainerConfigs: feastdevv1.ContainerConfigs{
										DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
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
			deployment := feast.initFeastDeploy(RegistryFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, RegistryFeastType)).To(Succeed())

			ports := deployment.Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(1))
			Expect(ports[0].ContainerPort).To(Equal(getTargetPort(RegistryFeastType, feast.getTlsConfigs(RegistryFeastType))))
			Expect(ports[0].Name).To(Equal(string(RegistryFeastType)))
		})

		It("should configure correct REST container ports", func() {
			setFeatureStoreServerConfig(false, true)
			Expect(feast.deployFeastServiceByType(RegistryFeastType)).To(Succeed())
			deployment := feast.initFeastDeploy(RegistryFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, RegistryFeastType)).To(Succeed())

			ports := deployment.Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(1))
			Expect(ports[0].ContainerPort).To(Equal(getTargetRestPort(RegistryFeastType, feast.getTlsConfigs(RegistryFeastType))))
			Expect(ports[0].Name).To(Equal(string(RegistryFeastType) + "-rest"))

			Expect(deployment.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deployment.Spec.Template.Spec.Containers[0].Ports).To(HaveLen(1))
			Expect(deployment.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort).To(Equal(getTargetRestPort(RegistryFeastType, feast.getTlsConfigs(RegistryFeastType))))
			Expect(deployment.Spec.Template.Spec.Containers[0].Ports[0].Name).To(Equal(string(RegistryFeastType) + "-rest"))
		})

		It("should configure correct ports for both services", func() {
			setFeatureStoreServerConfig(true, true)
			Expect(feast.deployFeastServiceByType(RegistryFeastType)).To(Succeed())

			deployment := feast.initFeastDeploy(RegistryFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, RegistryFeastType)).To(Succeed())

			ports := deployment.Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(2))
			Expect(ports[0].ContainerPort).To(Equal(getTargetPort(RegistryFeastType, feast.getTlsConfigs(RegistryFeastType))))
			Expect(ports[0].Name).To(Equal(string(RegistryFeastType)))
			Expect(ports[1].ContainerPort).To(Equal(getTargetRestPort(RegistryFeastType, feast.getTlsConfigs(RegistryFeastType))))
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
			deployment := feast.initFeastDeploy(RegistryFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, RegistryFeastType)).To(Succeed())

			// Verify NodeSelector is applied to pod spec
			expectedNodeSelector := map[string]string{
				"kubernetes.io/os": "linux",
				"node-type":        "compute",
			}
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(expectedNodeSelector))
		})

		It("should apply online NodeSelector when multiple services configured", func() {
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
			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
						OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
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
			deployment := feast.initFeastDeploy(OnlineFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, OnlineFeastType)).To(Succeed())

			// Verify NodeSelector uses online service selector only
			expectedNodeSelector := map[string]string{
				"node-type": "online",
				"zone":      "us-west-1a",
			}
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(expectedNodeSelector))
		})

		It("should merge operator NodeSelector with existing selectors (mutating webhook scenario)", func() {
			// Set NodeSelector for UI service
			uiNodeSelector := map[string]string{
				"node-type": "ui",
			}
			featureStore.Spec.Services.UI = &feastdevv1.ServerConfigs{
				ContainerConfigs: feastdevv1.ContainerConfigs{
					DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
						Image: ptr("test-image"),
					},
					OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
						NodeSelector: &uiNodeSelector,
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			// Create deployment first
			deployment := feast.initFeastDeploy(UIFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, UIFeastType)).To(Succeed())

			// Simulate a mutating webhook or admission controller adding node selectors
			// This would happen after the operator creates the pod spec but before scheduling
			existingNodeSelector := map[string]string{
				"team":        "ml",
				"environment": "prod",
			}
			deployment.Spec.Template.Spec.NodeSelector = existingNodeSelector

			// Apply the node selector logic again to test merging
			// This simulates the operator reconciling and re-applying node selectors
			feast.applyNodeSelector(&deployment.Spec.Template.Spec, UIFeastType)

			// Verify NodeSelector merges existing and operator selectors
			expectedNodeSelector := map[string]string{
				"team":        "ml",
				"environment": "prod",
				"node-type":   "ui",
			}
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(expectedNodeSelector))
		})

		It("should apply UI NodeSelector for UI deployment", func() {
			// Set NodeSelector for online service
			onlineNodeSelector := map[string]string{
				"node-type": "online",
			}
			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
						OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
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
			featureStore.Spec.Services.UI = &feastdevv1.ServerConfigs{
				ContainerConfigs: feastdevv1.ContainerConfigs{
					DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
						Image: ptr("test-image"),
					},
					OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
						NodeSelector: &uiNodeSelector,
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			// Create deployment and verify UI service selector is applied
			deployment := feast.initFeastDeploy(UIFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, UIFeastType)).To(Succeed())

			// Verify NodeSelector is applied with UI service's selector
			expectedNodeSelector := map[string]string{
				"node-type": "ui",
				"zone":      "us-east-1",
			}
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(expectedNodeSelector))
		})

		It("should enable metrics on the online service when configured", func() {
			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					Metrics: ptr(true),
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			Expect(feast.deployFeastServiceByType(OnlineFeastType)).To(Succeed())

			deployment := feast.initFeastDeploy(OnlineFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, OnlineFeastType)).To(Succeed())

			onlineContainer := GetOnlineContainer(*deployment)
			Expect(onlineContainer).NotTo(BeNil())
			Expect(onlineContainer.Command).To(Equal(feast.getContainerCommand(OnlineFeastType)))
			Expect(onlineContainer.Ports).To(ContainElement(corev1.ContainerPort{
				Name:          "metrics",
				ContainerPort: MetricsPort,
				Protocol:      corev1.ProtocolTCP,
			}))
			metricsPortCount := 0
			for _, port := range onlineContainer.Ports {
				if port.Name == "metrics" {
					metricsPortCount++
				}
			}
			Expect(metricsPortCount).To(Equal(1))

			svc := feast.initFeastSvc(OnlineFeastType)
			Expect(svc).NotTo(BeNil())
			Expect(feast.setService(svc, OnlineFeastType, false)).To(Succeed())
			Expect(svc.Spec.Ports).To(ContainElement(corev1.ServicePort{
				Name:       "metrics",
				Port:       MetricsPort,
				Protocol:   corev1.ProtocolTCP,
				TargetPort: intstr.FromInt(int(MetricsPort)),
			}))
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
			deployment := feast.initFeastDeploy(RegistryFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, RegistryFeastType)).To(Succeed())

			// Verify no NodeSelector is applied (empty selector)
			Expect(deployment.Spec.Template.Spec.NodeSelector).To(BeEmpty())
		})
	})

	Describe("Online Store Server Removal", func() {
		It("should delete online deployment when server config is removed", func() {
			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			// create online deployment
			Expect(feast.deployFeastServiceByType(OnlineFeastType)).To(Succeed())
			Expect(apimeta.IsStatusConditionTrue(feast.Handler.FeatureStore.Status.Conditions, feastdevv1.OnlineStoreReadyType)).To(BeTrue())

			onlineDeploy := feast.initFeastDeploy(OnlineFeastType)
			Expect(onlineDeploy).NotTo(BeNil())
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      onlineDeploy.Name,
				Namespace: onlineDeploy.Namespace,
			}, onlineDeploy)).To(Succeed())

			// remove server config (simulate grpc-only)
			featureStore.Spec.Services.OnlineStore.Server = nil
			featureStore.Spec.Services.OnlineStore.Grpc = &feastdevv1.GrpcServerConfigs{}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			Expect(feast.deployFeastServiceByType(OnlineFeastType)).To(Succeed())
			Expect(apimeta.FindStatusCondition(feast.Handler.FeatureStore.Status.Conditions, feastdevv1.OnlineStoreReadyType)).To(BeNil())

			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      onlineDeploy.Name,
				Namespace: onlineDeploy.Namespace,
			}, onlineDeploy)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("Online gRPC PVC", func() {
		It("should mount the same PVC as the online deployment when file persistence is enabled", func() {
			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
				},
				Grpc: &feastdevv1.GrpcServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
				},
				Persistence: &feastdevv1.OnlineStorePersistence{
					FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
						Path: "online.db",
						PvcConfig: &feastdevv1.PvcConfig{
							Create: &feastdevv1.PvcCreate{
								AccessModes: DefaultPVCAccessModes,
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: resource.MustParse(DefaultOnlineStorageRequest),
									},
								},
							},
							MountPath: "/data",
						},
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			onlineDeploy := feast.initFeastDeploy(OnlineFeastType)
			Expect(onlineDeploy).NotTo(BeNil())
			Expect(feast.setDeployment(onlineDeploy, OnlineFeastType)).To(Succeed())

			grpcDeploy := feast.initFeastDeploy(OnlineGrpcFeastType)
			Expect(grpcDeploy).NotTo(BeNil())
			Expect(feast.setDeployment(grpcDeploy, OnlineGrpcFeastType)).To(Succeed())

			onlinePvcVolume := findPvcVolume(onlineDeploy.Spec.Template.Spec.Volumes)
			grpcPvcVolume := findPvcVolume(grpcDeploy.Spec.Template.Spec.Volumes)

			Expect(onlinePvcVolume).NotTo(BeNil())
			Expect(grpcPvcVolume).NotTo(BeNil())

			expectedPvcName := feast.GetFeastServiceName(OnlineFeastType)
			Expect(onlinePvcVolume.Name).To(Equal(expectedPvcName))
			Expect(onlinePvcVolume.PersistentVolumeClaim.ClaimName).To(Equal(expectedPvcName))
			Expect(grpcPvcVolume.Name).To(Equal(expectedPvcName))
			Expect(grpcPvcVolume.PersistentVolumeClaim.ClaimName).To(Equal(expectedPvcName))
		})
	})

	Describe("Online gRPC PVC cleanup", func() {
		It("should delete the shared online PVC when the gRPC service is disabled", func() {
			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Grpc: &feastdevv1.GrpcServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
				},
				Persistence: &feastdevv1.OnlineStorePersistence{
					FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
						Path: "online.db",
						PvcConfig: &feastdevv1.PvcConfig{
							Create: &feastdevv1.PvcCreate{
								AccessModes: DefaultPVCAccessModes,
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: resource.MustParse(DefaultOnlineStorageRequest),
									},
								},
							},
							MountPath: "/data",
						},
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			Expect(feast.deployFeastServiceByType(OnlineGrpcFeastType)).To(Succeed())

			pvcName := feast.GetFeastServiceName(OnlineFeastType)
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      pvcName,
				Namespace: featureStore.Namespace,
			}, pvc)).To(Succeed())

			featureStore.Spec.Services.OnlineStore = nil
			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			Expect(feast.reconcileOnlineGrpc()).To(Succeed())

			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      pvcName,
				Namespace: featureStore.Namespace,
			}, pvc)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("Online gRPC TLS", func() {
		It("should mount TLS secret and configure command flags", func() {
			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Grpc: &feastdevv1.GrpcServerConfigs{
					TLS: &feastdevv1.TlsConfigs{
						SecretRef: &corev1.LocalObjectReference{
							Name: "grpc-tls",
						},
						SecretKeyNames: feastdevv1.SecretKeyNames{
							TlsKey: "tls.key",
							TlsCrt: "tls.crt",
						},
					},
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			deployment := feast.initFeastDeploy(OnlineGrpcFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, OnlineGrpcFeastType)).To(Succeed())

			_, container := getContainerByType(OnlineGrpcFeastType, deployment.Spec.Template.Spec)
			Expect(container).NotTo(BeNil())
			Expect(container.Command).To(ContainElements(
				"--key", "/tls/online-grpc/tls.key",
				"--cert", "/tls/online-grpc/tls.crt",
			))

			hasMount := false
			for _, mount := range container.VolumeMounts {
				if mount.Name == "online-grpc-tls" &&
					mount.MountPath == "/tls/online-grpc/" &&
					mount.ReadOnly {
					hasMount = true
					break
				}
			}
			Expect(hasMount).To(BeTrue())

			hasVolume := false
			for _, volume := range deployment.Spec.Template.Spec.Volumes {
				if volume.Name == "online-grpc-tls" &&
					volume.Secret != nil &&
					volume.Secret.SecretName == "grpc-tls" {
					hasVolume = true
					break
				}
			}
			Expect(hasVolume).To(BeTrue())
		})
	})

	Describe("WorkerConfigs Configuration", func() {
		It("should apply WorkerConfigs to the online store command", func() {
			// Set WorkerConfigs for online store
			workers := int32(4)
			workerConnections := int32(2000)
			maxRequests := int32(500)
			maxRequestsJitter := int32(100)
			keepAliveTimeout := int32(60)
			registryTTLSeconds := int32(120)

			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
					WorkerConfigs: &feastdevv1.WorkerConfigs{
						Workers:            &workers,
						WorkerConnections:  &workerConnections,
						MaxRequests:        &maxRequests,
						MaxRequestsJitter:  &maxRequestsJitter,
						KeepAliveTimeout:   &keepAliveTimeout,
						RegistryTTLSeconds: &registryTTLSeconds,
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			Expect(feast.deployFeastServiceByType(OnlineFeastType)).To(Succeed())

			deployment := feast.initFeastDeploy(OnlineFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, OnlineFeastType)).To(Succeed())

			onlineContainer := GetOnlineContainer(*deployment)
			Expect(onlineContainer).NotTo(BeNil())

			// Verify all worker config options are present in the command
			Expect(onlineContainer.Command).To(ContainElement("--workers"))
			Expect(onlineContainer.Command).To(ContainElement("4"))
			Expect(onlineContainer.Command).To(ContainElement("--worker-connections"))
			Expect(onlineContainer.Command).To(ContainElement("2000"))
			Expect(onlineContainer.Command).To(ContainElement("--max-requests"))
			Expect(onlineContainer.Command).To(ContainElement("500"))
			Expect(onlineContainer.Command).To(ContainElement("--max-requests-jitter"))
			Expect(onlineContainer.Command).To(ContainElement("100"))
			Expect(onlineContainer.Command).To(ContainElement("--keep-alive-timeout"))
			Expect(onlineContainer.Command).To(ContainElement("60"))
			Expect(onlineContainer.Command).To(ContainElement("--registry_ttl_sec"))
			Expect(onlineContainer.Command).To(ContainElement("120"))
		})

		It("should apply partial WorkerConfigs to the online store command", func() {
			// Set only some WorkerConfigs options
			workers := int32(-1) // Auto-calculate
			registryTTLSeconds := int32(300)

			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
					WorkerConfigs: &feastdevv1.WorkerConfigs{
						Workers:            &workers,
						RegistryTTLSeconds: &registryTTLSeconds,
					},
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			Expect(feast.deployFeastServiceByType(OnlineFeastType)).To(Succeed())

			deployment := feast.initFeastDeploy(OnlineFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, OnlineFeastType)).To(Succeed())

			onlineContainer := GetOnlineContainer(*deployment)
			Expect(onlineContainer).NotTo(BeNil())

			// Verify specified options are present
			Expect(onlineContainer.Command).To(ContainElement("--workers"))
			Expect(onlineContainer.Command).To(ContainElement("-1"))
			Expect(onlineContainer.Command).To(ContainElement("--registry_ttl_sec"))
			Expect(onlineContainer.Command).To(ContainElement("300"))

			// Verify unspecified options are not present
			Expect(onlineContainer.Command).NotTo(ContainElement("--worker-connections"))
			Expect(onlineContainer.Command).NotTo(ContainElement("--max-requests"))
			Expect(onlineContainer.Command).NotTo(ContainElement("--max-requests-jitter"))
			Expect(onlineContainer.Command).NotTo(ContainElement("--keep-alive-timeout"))
		})

		It("should not add worker config options when WorkerConfigs is nil", func() {
			featureStore.Spec.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
					// WorkerConfigs is not set (nil)
				},
			}

			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			Expect(feast.deployFeastServiceByType(OnlineFeastType)).To(Succeed())

			deployment := feast.initFeastDeploy(OnlineFeastType)
			Expect(deployment).NotTo(BeNil())
			Expect(feast.setDeployment(deployment, OnlineFeastType)).To(Succeed())

			onlineContainer := GetOnlineContainer(*deployment)
			Expect(onlineContainer).NotTo(BeNil())

			// Verify no worker config options are present (defaults are used by the CLI)
			Expect(onlineContainer.Command).NotTo(ContainElement("--workers"))
			Expect(onlineContainer.Command).NotTo(ContainElement("--worker-connections"))
			Expect(onlineContainer.Command).NotTo(ContainElement("--max-requests"))
			Expect(onlineContainer.Command).NotTo(ContainElement("--max-requests-jitter"))
			Expect(onlineContainer.Command).NotTo(ContainElement("--keep-alive-timeout"))
			Expect(onlineContainer.Command).NotTo(ContainElement("--registry_ttl_sec"))
		})
	})
})
