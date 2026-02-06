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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("FeatureStore Controller - Deployment Volumes and VolumeMounts", func() {
	Context("When deploying featureStore Spec we should have an option do Volumes and VolumeMounts", func() {
		const resourceName = "services-ephemeral"
		const offlineType = "duckdb"
		var pullPolicy = corev1.PullAlways

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1.FeatureStore{}
		onlineStorePath := "/data/online.db"
		registryPath := "/data/registry.db"

		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := createFeatureStoreVolumeResource(resourceName, image, pullPolicy)
				resource.Spec.Services.OfflineStore.Persistence = &feastdevv1.OfflineStorePersistence{
					FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
						Type: offlineType,
					},
				}
				resource.Spec.Services.OnlineStore.Persistence = &feastdevv1.OnlineStorePersistence{
					FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
						Path: onlineStorePath,
					},
				}
				resource.Spec.Services.Registry = &feastdevv1.Registry{
					Local: &feastdevv1.LocalRegistryConfig{
						Persistence: &feastdevv1.RegistryPersistence{
							FilePersistence: &feastdevv1.RegistryFilePersistence{
								Path: registryPath,
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

		It("should successfully reconcile the resource and volumes and volumeMounts should be available", func() {
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

			onlineDeploy := &appsv1.Deployment{}
			onlineMeta := feast.GetObjectMetaType(services.OnlineFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      onlineMeta.Name,
				Namespace: onlineMeta.Namespace,
			}, onlineDeploy)

			Expect(err).NotTo(HaveOccurred())

			// Extract the PodSpec from DeploymentSpec
			podSpec := onlineDeploy.Spec.Template.Spec

			// Validate Volumes
			// Validate Volumes - Check if our test volume exists among multiple
			Expect(podSpec.Volumes).To(ContainElement(WithTransform(func(v corev1.Volume) string {
				return v.Name
			}, Equal("test-volume"))), "Expected volume 'test-volume' to be present")

			// Ensure 'online' container has the test volume mount
			onlineContainer := services.GetOnlineContainer(*onlineDeploy)
			Expect(onlineContainer).ToNot(BeNil(), "Expected to find container 'online'")

			// Validate that 'online' container has the test-volume mount
			Expect(onlineContainer.VolumeMounts).To(ContainElement(WithTransform(func(vm corev1.VolumeMount) string {
				return vm.Name
			}, Equal("test-volume"))), "Expected 'online' container to have volume mount 'test-volume'")

			// Ensure other service deployments do NOT have the test volume mount
			offlineDeploy := &appsv1.Deployment{}
			offlineMeta := feast.GetObjectMetaType(services.OfflineFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      offlineMeta.Name,
				Namespace: offlineMeta.Namespace,
			}, offlineDeploy)
			Expect(err).NotTo(HaveOccurred())
			offlineContainer := services.GetOfflineContainer(*offlineDeploy)
			Expect(offlineContainer).NotTo(BeNil())
			Expect(offlineContainer.VolumeMounts).ToNot(ContainElement(WithTransform(func(vm corev1.VolumeMount) string {
				return vm.Name
			}, Equal("test-volume"))), "Unexpected volume mount 'test-volume' found in offline container")

			uiDeploy := &appsv1.Deployment{}
			uiMeta := feast.GetObjectMetaType(services.UIFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      uiMeta.Name,
				Namespace: uiMeta.Namespace,
			}, uiDeploy)
			Expect(err).NotTo(HaveOccurred())
			uiContainer := services.GetUIContainer(*uiDeploy)
			Expect(uiContainer).NotTo(BeNil())
			Expect(uiContainer.VolumeMounts).ToNot(ContainElement(WithTransform(func(vm corev1.VolumeMount) string {
				return vm.Name
			}, Equal("test-volume"))), "Unexpected volume mount 'test-volume' found in UI container")

		})
	})
})

func createFeatureStoreVolumeResource(resourceName string, image string, pullPolicy corev1.PullPolicy) *feastdevv1.FeatureStore {
	volume := corev1.Volume{
		Name: "test-volume",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
	volumeMount := corev1.VolumeMount{
		Name:      "test-volume",
		MountPath: "/data",
	}

	return &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: "default",
		},
		Spec: feastdevv1.FeatureStoreSpec{
			FeastProject: feastProject,
			Services: &feastdevv1.FeatureStoreServices{
				Volumes: []corev1.Volume{volume},
				OfflineStore: &feastdevv1.OfflineStore{
					Server: &feastdevv1.ServerConfigs{
						ContainerConfigs: feastdevv1.ContainerConfigs{},
					},
				},
				OnlineStore: &feastdevv1.OnlineStore{
					Server: &feastdevv1.ServerConfigs{
						VolumeMounts: []corev1.VolumeMount{volumeMount},
						ContainerConfigs: feastdevv1.ContainerConfigs{
							DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
								Image: &image,
							},
							OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
								ImagePullPolicy: &pullPolicy,
								Resources:       &corev1.ResourceRequirements{},
							},
						},
					},
				},
				UI: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
							Image: &image,
						},
						OptionalCtrConfigs: feastdevv1.OptionalCtrConfigs{
							ImagePullPolicy: &pullPolicy,
							Resources:       &corev1.ResourceRequirements{},
						},
					},
				},
			},
		},
	}
}
