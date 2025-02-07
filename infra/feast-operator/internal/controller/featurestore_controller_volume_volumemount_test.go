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

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
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
		featurestore := &feastdevv1alpha1.FeatureStore{}
		onlineStorePath := "/data/online.db"
		registryPath := "/data/registry.db"

		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := createFeatureStoreVolumeResource(resourceName, image, pullPolicy)
				resource.Spec.Services.OfflineStore.Persistence = &feastdevv1alpha1.OfflineStorePersistence{
					FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
						Type: offlineType,
					},
				}
				resource.Spec.Services.OnlineStore.Persistence = &feastdevv1alpha1.OnlineStorePersistence{
					FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
						Path: onlineStorePath,
					},
				}
				resource.Spec.Services.Registry = &feastdevv1alpha1.Registry{
					Local: &feastdevv1alpha1.LocalRegistryConfig{
						Persistence: &feastdevv1alpha1.RegistryPersistence{
							FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
								Path: registryPath,
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})
		AfterEach(func() {
			resource := &feastdevv1alpha1.FeatureStore{}
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

			resource := &feastdevv1alpha1.FeatureStore{}
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

			deploy := &appsv1.Deployment{}
			objMeta := feast.GetObjectMeta()
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)

			Expect(err).NotTo(HaveOccurred())

			// Extract the PodSpec from DeploymentSpec
			podSpec := deploy.Spec.Template.Spec

			// Validate Volumes
			// Validate Volumes - Check if our test volume exists among multiple
			Expect(podSpec.Volumes).To(ContainElement(WithTransform(func(v corev1.Volume) string {
				return v.Name
			}, Equal("test-volume"))), "Expected volume 'test-volume' to be present")

			// Ensure 'online' container has the test volume mount
			var onlineContainer *corev1.Container
			for i, container := range podSpec.Containers {
				if container.Name == "online" {
					onlineContainer = &podSpec.Containers[i]
					break
				}
			}
			Expect(onlineContainer).ToNot(BeNil(), "Expected to find container 'online'")

			// Validate that 'online' container has the test-volume mount
			Expect(onlineContainer.VolumeMounts).To(ContainElement(WithTransform(func(vm corev1.VolumeMount) string {
				return vm.Name
			}, Equal("test-volume"))), "Expected 'online' container to have volume mount 'test-volume'")

			// Ensure all other containers do NOT have the test volume mount
			for _, container := range podSpec.Containers {
				if container.Name != "online" {
					Expect(container.VolumeMounts).ToNot(ContainElement(WithTransform(func(vm corev1.VolumeMount) string {
						return vm.Name
					}, Equal("test-volume"))), "Unexpected volume mount 'test-volume' found in container "+container.Name)
				}
			}

		})
	})
})

func createFeatureStoreVolumeResource(resourceName string, image string, pullPolicy corev1.PullPolicy) *feastdevv1alpha1.FeatureStore {
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

	return &feastdevv1alpha1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: "default",
		},
		Spec: feastdevv1alpha1.FeatureStoreSpec{
			FeastProject: feastProject,
			Services: &feastdevv1alpha1.FeatureStoreServices{
				Volumes: []corev1.Volume{volume},
				OfflineStore: &feastdevv1alpha1.OfflineStore{
					Server: &feastdevv1alpha1.ServerConfigs{
						ContainerConfigs: feastdevv1alpha1.ContainerConfigs{},
					},
				},
				OnlineStore: &feastdevv1alpha1.OnlineStore{
					Server: &feastdevv1alpha1.ServerConfigs{
						VolumeMounts: []corev1.VolumeMount{volumeMount},
						ContainerConfigs: feastdevv1alpha1.ContainerConfigs{
							DefaultCtrConfigs: feastdevv1alpha1.DefaultCtrConfigs{
								Image: &image,
							},
							OptionalCtrConfigs: feastdevv1alpha1.OptionalCtrConfigs{
								ImagePullPolicy: &pullPolicy,
								Resources:       &corev1.ResourceRequirements{},
							},
						},
					},
				},
				UI: &feastdevv1alpha1.ServerConfigs{
					ContainerConfigs: feastdevv1alpha1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1alpha1.DefaultCtrConfigs{
							Image: &image,
						},
						OptionalCtrConfigs: feastdevv1alpha1.OptionalCtrConfigs{
							ImagePullPolicy: &pullPolicy,
							Resources:       &corev1.ResourceRequirements{},
						},
					},
				},
			},
		},
	}
}
