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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

		It("should return false when scaling config has replicas=1", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(1)),
			}
			Expect(isScalingEnabled(featureStore)).To(BeFalse())
		})

		It("should return true when scaling config has replicas > 1", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
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

	Describe("isFilePersistence", func() {
		It("should return false when all services use DB persistence", func() {
			Expect(isFilePersistence(featureStore)).To(BeFalse())
		})

		It("should return true when online store uses file persistence", func() {
			featureStore.Status.Applied.Services.OnlineStore.Persistence = &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path: "/data/online.db",
				},
			}
			Expect(isFilePersistence(featureStore)).To(BeTrue())
		})

		It("should return true when offline store uses file persistence", func() {
			featureStore.Status.Applied.Services.OfflineStore = &feastdevv1.OfflineStore{
				Persistence: &feastdevv1.OfflineStorePersistence{
					FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
						Type: "duckdb",
					},
				},
			}
			Expect(isFilePersistence(featureStore)).To(BeTrue())
		})

		It("should return true when registry uses local file persistence", func() {
			featureStore.Status.Applied.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
				FilePersistence: &feastdevv1.RegistryFilePersistence{
					Path: "registry.db",
				},
			}
			Expect(isFilePersistence(featureStore)).To(BeTrue())
		})

		It("should return false when registry uses S3 path", func() {
			featureStore.Status.Applied.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
				FilePersistence: &feastdevv1.RegistryFilePersistence{
					Path: "s3://my-bucket/registry.db",
				},
			}
			Expect(isFilePersistence(featureStore)).To(BeFalse())
		})

		It("should return false when registry uses GS path", func() {
			featureStore.Status.Applied.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
				FilePersistence: &feastdevv1.RegistryFilePersistence{
					Path: "gs://my-bucket/registry.db",
				},
			}
			Expect(isFilePersistence(featureStore)).To(BeFalse())
		})

		It("should return true when no registry is configured (implicit file-based default)", func() {
			featureStore.Status.Applied.Services.Registry = nil
			Expect(isFilePersistence(featureStore)).To(BeTrue())
		})

		It("should return true when local registry has no persistence configured", func() {
			featureStore.Status.Applied.Services.Registry = &feastdevv1.Registry{
				Local: &feastdevv1.LocalRegistryConfig{},
			}
			Expect(isFilePersistence(featureStore)).To(BeTrue())
		})

		It("should return true when local registry has empty persistence", func() {
			featureStore.Status.Applied.Services.Registry = &feastdevv1.Registry{
				Local: &feastdevv1.LocalRegistryConfig{
					Persistence: &feastdevv1.RegistryPersistence{},
				},
			}
			Expect(isFilePersistence(featureStore)).To(BeTrue())
		})

		It("should return false when remote registry is configured", func() {
			featureStore.Status.Applied.Services.Registry = &feastdevv1.Registry{
				Remote: &feastdevv1.RemoteRegistryConfig{
					Hostname: ptr("registry.example.com"),
				},
			}
			Expect(isFilePersistence(featureStore)).To(BeFalse())
		})
	})

	Describe("validateScaling", func() {
		It("should succeed when no scaling is configured", func() {
			Expect(feast.validateScaling()).To(Succeed())
		})

		It("should succeed with scaling and DB persistence", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			Expect(feast.validateScaling()).To(Succeed())
		})

		It("should pass with scaling and S3-backed registry", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			featureStore.Status.Applied.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
				FilePersistence: &feastdevv1.RegistryFilePersistence{
					Path: "s3://my-bucket/registry.db",
				},
			}
			Expect(feast.validateScaling()).To(Succeed())
		})

		It("should pass with scaling and GS-backed registry", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			featureStore.Status.Applied.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
				FilePersistence: &feastdevv1.RegistryFilePersistence{
					Path: "gs://my-bucket/registry.db",
				},
			}
			Expect(feast.validateScaling()).To(Succeed())
		})

		It("should reject scaling when no registry is configured (implicit file-based default)", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			featureStore.Status.Applied.Services.Registry = nil
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("DB-backed persistence"))
		})

		It("should succeed with scaling and remote registry", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			featureStore.Status.Applied.Services.Registry = &feastdevv1.Registry{
				Remote: &feastdevv1.RemoteRegistryConfig{
					Hostname: ptr("registry.example.com"),
				},
			}
			Expect(feast.validateScaling()).To(Succeed())
		})
	})

	Describe("validateScaling rejects file-based persistence with scaling", func() {
		It("should reject static replicas with file-based online store", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			featureStore.Status.Applied.Services.OnlineStore.Persistence = &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path: "/data/online.db",
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("DB-backed persistence"))
			Expect(err.Error()).To(ContainSubstring("File-based persistence"))
		})

		It("should reject autoscaling with file-based online store", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MinReplicas: ptr(int32(2)),
					MaxReplicas: 10,
				},
			}
			featureStore.Status.Applied.Services.OnlineStore.Persistence = &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path: "/data/online.db",
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("DB-backed persistence"))
		})

		It("should reject static replicas with file-based online store using PVC", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(2)),
			}
			featureStore.Status.Applied.Services.OnlineStore.Persistence = &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path: "online.db",
					PvcConfig: &feastdevv1.PvcConfig{
						Ref:       &corev1.LocalObjectReference{Name: "my-pvc"},
						MountPath: "/data",
					},
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("File-based persistence"))
		})

		It("should reject static replicas with file-based offline store (duckdb)", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			featureStore.Status.Applied.Services.OfflineStore = &feastdevv1.OfflineStore{
				Persistence: &feastdevv1.OfflineStorePersistence{
					FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
						Type: "duckdb",
					},
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("DB-backed persistence"))
		})

		It("should reject autoscaling with file-based offline store (dask)", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 5,
				},
			}
			featureStore.Status.Applied.Services.OfflineStore = &feastdevv1.OfflineStore{
				Persistence: &feastdevv1.OfflineStorePersistence{
					FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
						Type: "dask",
					},
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("DB-backed persistence"))
		})

		It("should reject static replicas with file-based registry (registry.db)", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			featureStore.Status.Applied.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
				FilePersistence: &feastdevv1.RegistryFilePersistence{
					Path: "registry.db",
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("DB-backed persistence"))
		})

		It("should reject autoscaling with file-based registry", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 5,
				},
			}
			featureStore.Status.Applied.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
				FilePersistence: &feastdevv1.RegistryFilePersistence{
					Path: "/data/registry.db",
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("DB-backed persistence"))
		})

		It("should reject scaling when only one service among many uses file persistence", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			// Online store uses DB persistence (from BeforeEach), but add file-based offline store
			featureStore.Status.Applied.Services.OfflineStore = &feastdevv1.OfflineStore{
				Persistence: &feastdevv1.OfflineStorePersistence{
					FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
						Type: "file",
					},
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("DB-backed persistence"))
		})

		It("should reject scaling when registry uses local file with PVC", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(2)),
			}
			featureStore.Status.Applied.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
				FilePersistence: &feastdevv1.RegistryFilePersistence{
					Path: "registry.db",
					PvcConfig: &feastdevv1.PvcConfig{
						Ref:       &corev1.LocalObjectReference{Name: "registry-pvc"},
						MountPath: "/data",
					},
				},
			}
			err := feast.validateScaling()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("File-based persistence"))
		})

		It("should allow file persistence when replicas is 1 (no actual scaling)", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(1)),
			}
			featureStore.Status.Applied.Services.OnlineStore.Persistence = &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path: "/data/online.db",
				},
			}
			Expect(feast.validateScaling()).To(Succeed())
		})

		It("should allow file persistence when scaling config is nil", func() {
			featureStore.Status.Applied.Services.OnlineStore.Persistence = &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path: "/data/online.db",
				},
			}
			Expect(feast.validateScaling()).To(Succeed())
		})
	})

	Describe("getDesiredReplicas", func() {
		It("should return nil when no scaling is configured", func() {
			Expect(feast.getDesiredReplicas()).To(BeNil())
		})

		It("should return static replicas when configured", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
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

		It("should default to RollingUpdate when scaling is enabled", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			strategy := feast.getDeploymentStrategy()
			Expect(strategy.Type).To(Equal(appsv1.RollingUpdateDeploymentStrategyType))
		})

		It("should respect user-defined strategy even with scaling", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			featureStore.Status.Applied.Services.DeploymentStrategy = &appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			}
			strategy := feast.getDeploymentStrategy()
			Expect(strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
		})
	})

	Describe("setDeployment with scaling", func() {
		// These tests use file-based (default) persistence to avoid needing real secrets
		// for the feature_store.yaml generation that happens inside setDeployment.
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
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Replicas: ptr(int32(3)),
			}
			Expect(k8sClient.Status().Update(ctx, featureStore)).To(Succeed())
			feast.refreshFeatureStore(ctx, typeNamespacedName)

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
			Expect(k8sClient.Status().Update(ctx, featureStore)).To(Succeed())
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			deployment := feast.initFeastDeploy()
			existing := int32(4)
			deployment.Spec.Replicas = &existing
			Expect(feast.setDeployment(deployment)).To(Succeed())
			Expect(deployment.Spec.Replicas).NotTo(BeNil())
			Expect(*deployment.Spec.Replicas).To(Equal(int32(4)))
		})

		It("should preserve existing replicas when no scaling is configured", func() {
			setFilePersistence()
			Expect(k8sClient.Status().Update(ctx, featureStore)).To(Succeed())
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			deployment := feast.initFeastDeploy()
			existing := int32(2)
			deployment.Spec.Replicas = &existing
			Expect(feast.setDeployment(deployment)).To(Succeed())
			Expect(deployment.Spec.Replicas).NotTo(BeNil())
			Expect(*deployment.Spec.Replicas).To(Equal(int32(2)))
		})
	})

	Describe("HPA Configuration", func() {
		It("should create an HPA with default CPU metrics", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 10,
				},
			}

			hpa := feast.initHPA()
			Expect(feast.setHPA(hpa)).To(Succeed())
			Expect(hpa.Spec.MaxReplicas).To(Equal(int32(10)))
			Expect(*hpa.Spec.MinReplicas).To(Equal(int32(1)))
			Expect(hpa.Spec.Metrics).To(HaveLen(1))
			Expect(hpa.Spec.Metrics[0].Resource.Name).To(Equal(corev1.ResourceCPU))
		})

		It("should create an HPA with custom min replicas", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MinReplicas: ptr(int32(2)),
					MaxReplicas: 10,
				},
			}

			hpa := feast.initHPA()
			Expect(feast.setHPA(hpa)).To(Succeed())
			Expect(*hpa.Spec.MinReplicas).To(Equal(int32(2)))
			Expect(hpa.Spec.MaxReplicas).To(Equal(int32(10)))
		})

		It("should set correct scale target reference", func() {
			featureStore.Status.Applied.Services.Scaling = &feastdevv1.ScalingConfig{
				Autoscaling: &feastdevv1.AutoscalingConfig{
					MaxReplicas: 5,
				},
			}

			hpa := feast.initHPA()
			Expect(feast.setHPA(hpa)).To(Succeed())
			Expect(hpa.Spec.ScaleTargetRef.APIVersion).To(Equal("apps/v1"))
			Expect(hpa.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))
			Expect(hpa.Spec.ScaleTargetRef.Name).To(Equal(GetFeastName(featureStore)))
		})
	})
})
