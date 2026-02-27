/*
Copyright 2026 Feast Community.

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
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Horizontal Scaling", func() {
	var (
		featureStore       *feastdevv1.FeatureStore
		feast              *FeastServices
		typeNamespacedName types.NamespacedName
		ctx                context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		typeNamespacedName = types.NamespacedName{
			Name:      "scaling-test-fs",
			Namespace: "default",
		}

		featureStore = &feastdevv1.FeatureStore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      typeNamespacedName.Name,
				Namespace: typeNamespacedName.Namespace,
			},
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: "scalingproject",
				Services: &feastdevv1.FeatureStoreServices{
					OnlineStore: &feastdevv1.OnlineStore{
						Server: &feastdevv1.ServerConfigs{
							ContainerConfigs: feastdevv1.ContainerConfigs{
								DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
									Image: ptr("test-image"),
								},
							},
						},
						Persistence: &feastdevv1.OnlineStorePersistence{
							DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{
								Type: "redis",
								SecretRef: corev1.LocalObjectReference{
									Name: "redis-secret",
								},
							},
						},
					},
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
								GRPC: ptr(true),
							},
							Persistence: &feastdevv1.RegistryPersistence{
								DBPersistence: &feastdevv1.RegistryDBStorePersistence{
									Type: "sql",
									SecretRef: corev1.LocalObjectReference{
										Name: "registry-secret",
									},
								},
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

	Describe("isScalingEnabled", func() {
		It("should return false when no scaling config is present", func() {
			Expect(isScalingEnabled(featureStore)).To(BeFalse())
		})

		It("should return false when replicas=1", func() {
			featureStore.Status.Applied.Replicas = ptr(int32(1))
			Expect(isScalingEnabled(featureStore)).To(BeFalse())
		})

		It("should return true when replicas > 1", func() {
			featureStore.Status.Applied.Replicas = ptr(int32(3))
			Expect(isScalingEnabled(featureStore)).To(BeTrue())
		})

		It("should return true when autoscaling is configured", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 5,
				},
			}
			Expect(isScalingEnabled(featureStore)).To(BeTrue())
		})
	})

	Describe("CEL admission validation rejects invalid scaling configurations", func() {
		dbOnlineStore := &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{
					Type:      "redis",
					SecretRef: corev1.LocalObjectReference{Name: "redis-secret"},
				},
			},
		}

		dbRegistry := &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Persistence: &feastdevv1.RegistryPersistence{
					DBPersistence: &feastdevv1.RegistryDBStorePersistence{
						Type:      "sql",
						SecretRef: corev1.LocalObjectReference{Name: "registry-secret"},
					},
				},
			},
		}

		It("should accept scaling with full DB persistence", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-valid-db", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: dbOnlineStore,
						Registry:    dbRegistry,
					},
				},
			}
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fs)).To(Succeed())
		})

		It("should reject scaling when online store is missing (implicit file default)", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-no-online", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						Registry: dbRegistry,
					},
				},
			}
			err := k8sClient.Create(ctx, fs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("online store"))
		})

		It("should reject scaling when online store uses file persistence", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-file-online", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: &feastdevv1.OnlineStore{
							Persistence: &feastdevv1.OnlineStorePersistence{
								FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
									Path: "/data/online.db",
								},
							},
						},
						Registry: dbRegistry,
					},
				},
			}
			err := k8sClient.Create(ctx, fs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("online store"))
		})

		It("should reject scaling when offline store uses file persistence", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-file-offline", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: dbOnlineStore,
						Registry:    dbRegistry,
						OfflineStore: &feastdevv1.OfflineStore{
							Persistence: &feastdevv1.OfflineStorePersistence{
								FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
									Type: "duckdb",
								},
							},
						},
					},
				},
			}
			err := k8sClient.Create(ctx, fs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("offline store"))
		})

		It("should reject scaling when no registry is configured (implicit file default)", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-no-registry", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: dbOnlineStore,
					},
				},
			}
			err := k8sClient.Create(ctx, fs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("registry"))
		})

		It("should reject scaling when registry uses file persistence", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-file-registry", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: dbOnlineStore,
						Registry: &feastdevv1.Registry{
							Local: &feastdevv1.LocalRegistryConfig{
								Persistence: &feastdevv1.RegistryPersistence{
									FilePersistence: &feastdevv1.RegistryFilePersistence{
										Path: "/data/registry.db",
									},
								},
							},
						},
					},
				},
			}
			err := k8sClient.Create(ctx, fs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("registry"))
		})

		It("should accept scaling with S3-backed registry", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-s3-registry", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: dbOnlineStore,
						Registry: &feastdevv1.Registry{
							Local: &feastdevv1.LocalRegistryConfig{
								Persistence: &feastdevv1.RegistryPersistence{
									FilePersistence: &feastdevv1.RegistryFilePersistence{
										Path: "s3://my-bucket/registry.db",
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fs)).To(Succeed())
		})

		It("should accept scaling with GS-backed registry", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-gs-registry", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: dbOnlineStore,
						Registry: &feastdevv1.Registry{
							Local: &feastdevv1.LocalRegistryConfig{
								Persistence: &feastdevv1.RegistryPersistence{
									FilePersistence: &feastdevv1.RegistryFilePersistence{
										Path: "gs://my-bucket/registry.db",
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fs)).To(Succeed())
		})

		It("should accept scaling with remote registry", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-remote-reg", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: dbOnlineStore,
						Registry: &feastdevv1.Registry{
							Remote: &feastdevv1.RemoteRegistryConfig{
								Hostname: ptr("registry.example.com:80"),
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fs)).To(Succeed())
		})

		It("should accept file persistence when replicas is 1", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-rep1-file", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(1)),
				},
			}
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fs)).To(Succeed())
		})

		It("should accept file persistence when no scaling is configured", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-no-scaling", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
				},
			}
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fs)).To(Succeed())
		})

		It("should reject autoscaling without DB online store", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-hpa-no-db", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Services: &feastdevv1.FeatureStoreServices{
						Scaling: &feastdevv1.ScalingConfig{
							Autoscaling: &feastdevv1.AutoscalingConfig{MaxReplicas: 5},
						},
						Registry: dbRegistry,
					},
				},
			}
			err := k8sClient.Create(ctx, fs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("online store"))
		})

		It("should reject scaling when online store has no persistence configured", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-online-nop", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: &feastdevv1.OnlineStore{},
						Registry:    dbRegistry,
					},
				},
			}
			err := k8sClient.Create(ctx, fs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("online store"))
		})

		It("should reject replicas and autoscaling set simultaneously", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-mutual-excl", Namespace: "default"},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "celtest",
					Replicas:     ptr(int32(3)),
					Services: &feastdevv1.FeatureStoreServices{
						Scaling: &feastdevv1.ScalingConfig{
							Autoscaling: &feastdevv1.AutoscalingConfig{MaxReplicas: 5},
						},
						OnlineStore: dbOnlineStore,
						Registry:    dbRegistry,
					},
				},
			}
			err := k8sClient.Create(ctx, fs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})
	})

	Describe("getDesiredReplicas", func() {
		It("should return 1 when no explicit replicas are configured (default)", func() {
			replicas := feast.getDesiredReplicas()
			Expect(replicas).NotTo(BeNil())
			Expect(*replicas).To(Equal(int32(1)))
		})

		It("should return static replicas when configured", func() {
			featureStore.Status.Applied.Replicas = ptr(int32(3))
			replicas := feast.getDesiredReplicas()
			Expect(replicas).NotTo(BeNil())
			Expect(*replicas).To(Equal(int32(3)))
		})

		It("should return nil when autoscaling is configured (HPA manages replicas)", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 5,
				},
			}
			Expect(feast.getDesiredReplicas()).To(BeNil())
		})
	})

	Describe("Deployment Strategy", func() {
		It("should default to Recreate when no scaling is configured", func() {
			Expect(feast.ApplyDefaults()).To(Succeed())
			strategy := feast.getDeploymentStrategy()
			Expect(strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
		})

		It("should default to RollingUpdate when scaling is enabled via replicas", func() {
			featureStore.Status.Applied.Replicas = ptr(int32(3))
			strategy := feast.getDeploymentStrategy()
			Expect(strategy.Type).To(Equal(appsv1.RollingUpdateDeploymentStrategyType))
		})

		It("should respect user-defined strategy even with scaling", func() {
			featureStore.Status.Applied.Replicas = ptr(int32(3))
			featureStore.Status.Applied.Services.DeploymentStrategy = &appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			}
			strategy := feast.getDeploymentStrategy()
			Expect(strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
		})
	})

	Describe("setDeployment with scaling", func() {
		setFilePersistence := func() {
			featureStore.Status.Applied.Services.OnlineStore = &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: ptr("test-image"),
						},
					},
				},
				Persistence: &feastdevv1.OnlineStorePersistence{
					FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
						Path: "/feast-data/online.db",
					},
				},
			}
			featureStore.Status.Applied.Services.Registry = &feastdevv1.Registry{
				Local: &feastdevv1.LocalRegistryConfig{
					Server: &feastdevv1.RegistryServerConfigs{
						ServerConfigs: feastdevv1.ServerConfigs{
							ContainerConfigs: feastdevv1.ContainerConfigs{
								DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
									Image: ptr("test-image"),
								},
							},
						},
						GRPC: ptr(true),
					},
					Persistence: &feastdevv1.RegistryPersistence{
						FilePersistence: &feastdevv1.RegistryFilePersistence{
							Path: "/feast-data/registry.db",
						},
					},
				},
			}
		}

		It("should set static replicas on the deployment", func() {
			setFilePersistence()
			featureStore.Status.Applied.Replicas = ptr(int32(3))

			deployment := feast.initFeastDeploy()
			Expect(feast.setDeployment(deployment)).To(Succeed())
			Expect(deployment.Spec.Replicas).NotTo(BeNil())
			Expect(*deployment.Spec.Replicas).To(Equal(int32(3)))
		})

		It("should preserve existing replicas when autoscaling is configured", func() {
			setFilePersistence()
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 5,
				},
			}

			deployment := feast.initFeastDeploy()
			existing := int32(4)
			deployment.Spec.Replicas = &existing
			Expect(feast.setDeployment(deployment)).To(Succeed())
			Expect(deployment.Spec.Replicas).NotTo(BeNil())
			Expect(*deployment.Spec.Replicas).To(Equal(int32(4)))
		})

		It("should set default replicas=1 when no explicit scaling is configured", func() {
			setFilePersistence()
			Expect(k8sClient.Status().Update(ctx, featureStore)).To(Succeed())
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			deployment := feast.initFeastDeploy()
			Expect(feast.setDeployment(deployment)).To(Succeed())
			Expect(deployment.Spec.Replicas).NotTo(BeNil())
			Expect(*deployment.Spec.Replicas).To(Equal(int32(1)))
		})
	})

	Describe("HPA Configuration", func() {
		It("should build an HPA apply config with default CPU metrics", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 10,
				},
			}

			hpa := feast.buildHPAApplyConfig()
			Expect(*hpa.Spec.MaxReplicas).To(Equal(int32(10)))
			Expect(*hpa.Spec.MinReplicas).To(Equal(int32(1)))
			Expect(hpa.Spec.Metrics).To(HaveLen(1))
			Expect(*hpa.Spec.Metrics[0].Resource.Name).To(Equal(corev1.ResourceCPU))
		})

		It("should build an HPA apply config with custom min replicas", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MinReplicas: ptr(int32(2)),
					MaxReplicas: 10,
				},
			}

			hpa := feast.buildHPAApplyConfig()
			Expect(*hpa.Spec.MinReplicas).To(Equal(int32(2)))
			Expect(*hpa.Spec.MaxReplicas).To(Equal(int32(10)))
		})

		It("should set correct scale target reference", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 5,
				},
			}

			hpa := feast.buildHPAApplyConfig()
			Expect(*hpa.Spec.ScaleTargetRef.APIVersion).To(Equal("apps/v1"))
			Expect(*hpa.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))
			Expect(*hpa.Spec.ScaleTargetRef.Name).To(Equal(GetFeastName(featureStore)))
		})

		It("should set TypeMeta and owner reference for SSA", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 5,
				},
			}

			hpa := feast.buildHPAApplyConfig()
			Expect(*hpa.Kind).To(Equal("HorizontalPodAutoscaler"))
			Expect(*hpa.APIVersion).To(Equal("autoscaling/v2"))
			Expect(hpa.OwnerReferences).To(HaveLen(1))
			Expect(*hpa.OwnerReferences[0].Name).To(Equal(featureStore.Name))
			Expect(*hpa.OwnerReferences[0].Controller).To(BeTrue())
		})

		It("should convert custom metrics via JSON round-trip", func() {
			customMetrics := []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: corev1.ResourceMemory,
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: ptr(int32(75)),
						},
					},
				},
			}
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 10,
					Metrics:     customMetrics,
				},
			}

			hpa := feast.buildHPAApplyConfig()
			Expect(hpa.Spec.Metrics).To(HaveLen(1))
			Expect(*hpa.Spec.Metrics[0].Resource.Name).To(Equal(corev1.ResourceMemory))
			Expect(*hpa.Spec.Metrics[0].Resource.Target.AverageUtilization).To(Equal(int32(75)))
		})
	})

	Describe("Scale sub-resource", func() {
		newDBFeatureStore := func(name string) *feastdevv1.FeatureStore {
			return &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "scaletest",
					Services: &feastdevv1.FeatureStoreServices{
						OnlineStore: &feastdevv1.OnlineStore{
							Persistence: &feastdevv1.OnlineStorePersistence{
								DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{
									Type:      "redis",
									SecretRef: corev1.LocalObjectReference{Name: "redis-secret"},
								},
							},
						},
						Registry: &feastdevv1.Registry{
							Local: &feastdevv1.LocalRegistryConfig{
								Persistence: &feastdevv1.RegistryPersistence{
									DBPersistence: &feastdevv1.RegistryDBStorePersistence{
										Type:      "sql",
										SecretRef: corev1.LocalObjectReference{Name: "registry-secret"},
									},
								},
							},
						},
					},
				},
			}
		}

		It("should allow scaling up via the scale sub-resource with DB persistence", func() {
			fs := newDBFeatureStore("scale-sub-valid")
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			defer func() { Expect(k8sClient.Delete(ctx, fs)).To(Succeed()) }()

			scale := &autoscalingv1.Scale{}
			Expect(k8sClient.SubResource("scale").Get(ctx, fs, scale)).To(Succeed())
			Expect(scale.Spec.Replicas).To(Equal(int32(1)))

			scale.Spec.Replicas = 3
			Expect(k8sClient.SubResource("scale").Update(ctx, fs, client.WithSubResourceBody(scale))).To(Succeed())

			updated := &feastdevv1.FeatureStore{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fs.Name, Namespace: fs.Namespace}, updated)).To(Succeed())
			Expect(updated.Spec.Replicas).NotTo(BeNil())
			Expect(*updated.Spec.Replicas).To(Equal(int32(3)))
		})

		It("should reject scaling up via the scale sub-resource without DB persistence", func() {
			fs := &feastdevv1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "scale-sub-reject",
					Namespace: "default",
				},
				Spec: feastdevv1.FeatureStoreSpec{
					FeastProject: "scaletest",
				},
			}
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			defer func() { Expect(k8sClient.Delete(ctx, fs)).To(Succeed()) }()

			scale := &autoscalingv1.Scale{}
			Expect(k8sClient.SubResource("scale").Get(ctx, fs, scale)).To(Succeed())

			scale.Spec.Replicas = 3
			err := k8sClient.SubResource("scale").Update(ctx, fs, client.WithSubResourceBody(scale))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("online store"))
		})

		It("should read the status replicas from the scale sub-resource", func() {
			fs := newDBFeatureStore("scale-sub-status")
			fs.Spec.Replicas = ptr(int32(2))
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			defer func() { Expect(k8sClient.Delete(ctx, fs)).To(Succeed()) }()

			fs.Status.Replicas = 2
			fs.Status.Applied.FeastProject = fs.Spec.FeastProject
			Expect(k8sClient.Status().Update(ctx, fs)).To(Succeed())

			scale := &autoscalingv1.Scale{}
			Expect(k8sClient.SubResource("scale").Get(ctx, fs, scale)).To(Succeed())
			Expect(scale.Status.Replicas).To(Equal(int32(2)))
			Expect(scale.Spec.Replicas).To(Equal(int32(2)))
		})
	})
})
