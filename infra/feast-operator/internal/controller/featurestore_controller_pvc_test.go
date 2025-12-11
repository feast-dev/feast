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
	"path"

	apiresource "k8s.io/apimachinery/pkg/api/resource"

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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("FeatureStore Controller-Ephemeral services", func() {
	Context("When deploying a resource with all ephemeral services", func() {
		const resourceName = "services-pvc"
		var pullPolicy = corev1.PullAlways
		var testEnvVarName = "testEnvVarName"
		var testEnvVarValue = "testEnvVarValue"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1.FeatureStore{}
		onlineStorePath := "online.db"
		registryPath := "registry.db"
		offlineType := "duckdb"

		offlineStoreMountPath := "/offline"
		onlineStoreMountPath := "/online"
		registryMountPath := "/registry"

		securityContext := &corev1.PodSecurityContext{
			FSGroup: int64Ptr(0),
		}

		accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce, corev1.ReadWriteMany}
		storageClassName := "test"

		onlineStoreMountedPath := path.Join(onlineStoreMountPath, onlineStorePath)
		registryMountedPath := path.Join(registryMountPath, registryPath)

		BeforeEach(func() {
			createEnvFromSecretAndConfigMap()

			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue},
					{Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}, withEnvFrom())
				resource.Spec.Services.UI = nil
				resource.Spec.Services.OfflineStore.Persistence = &feastdevv1.OfflineStorePersistence{
					FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
						Type: offlineType,
						PvcConfig: &feastdevv1.PvcConfig{
							Create: &feastdevv1.PvcCreate{
								AccessModes:      accessModes,
								StorageClassName: &storageClassName,
							},
							MountPath: offlineStoreMountPath,
						},
					},
				}
				resource.Spec.Services.OnlineStore.Persistence = &feastdevv1.OnlineStorePersistence{
					FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
						Path: onlineStorePath,
						PvcConfig: &feastdevv1.PvcConfig{
							Create:    &feastdevv1.PvcCreate{},
							MountPath: onlineStoreMountPath,
						},
					},
				}
				resource.Spec.Services.Registry.Local.Persistence = &feastdevv1.RegistryPersistence{
					FilePersistence: &feastdevv1.RegistryFilePersistence{
						Path: registryPath,
						PvcConfig: &feastdevv1.PvcConfig{
							Create:    &feastdevv1.PvcCreate{},
							MountPath: registryMountPath,
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

			deleteEnvFromSecretAndConfigMap()
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
			Expect(resource.Status.Applied.AuthzConfig).To(BeNil())
			Expect(resource.Status.Applied.Services).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.Type).To(Equal(offlineType))
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.AccessModes).To(Equal(accessModes))
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.StorageClassName).To(Equal(&storageClassName))
			expectedResources := corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: apiresource.MustParse("20Gi"),
				},
			}
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.Resources).To(Equal(expectedResources))
			Expect(resource.Status.Applied.Services.OfflineStore.Server.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Server.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Server.Image).To(Equal(&services.DefaultImage))
			Expect(resource.Status.Applied.Services.OnlineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.Path).To(Equal(onlineStorePath))
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.AccessModes).To(Equal(services.DefaultPVCAccessModes))
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.StorageClassName).To(BeNil())
			expectedResources = corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: apiresource.MustParse("5Gi"),
				},
			}
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.Resources).To(Equal(expectedResources))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.Env).To(Equal(&[]corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue}, {Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.EnvFrom).To(Equal(withEnvFrom()))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.ImagePullPolicy).To(Equal(&pullPolicy))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Server.Image).To(Equal(&image))
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.Path).To(Equal(registryPath))
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.AccessModes).To(Equal(services.DefaultPVCAccessModes))
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.StorageClassName).To(BeNil())
			expectedResources = corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: apiresource.MustParse("5Gi"),
				},
			}
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.Resources).To(Equal(expectedResources))
			Expect(resource.Status.Applied.Services.Registry.Local.Server.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Server.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Server.Image).To(Equal(&services.DefaultImage))

			Expect(resource.Status.ServiceHostnames.OfflineStore).To(Equal(feast.GetFeastServiceName(services.OfflineFeastType) + "." + resource.Namespace + domain))
			Expect(resource.Status.ServiceHostnames.OnlineStore).To(Equal(feast.GetFeastServiceName(services.OnlineFeastType) + "." + resource.Namespace + domain))
			Expect(resource.Status.ServiceHostnames.Registry).To(Equal(feast.GetFeastServiceName(services.RegistryFeastType) + "." + resource.Namespace + domain))

			Expect(resource.Status.Conditions).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.ReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).To(Equal(feastdevv1.DeploymentNotAvailableReason))
			Expect(cond.Type).To(Equal(feastdevv1.ReadyType))
			Expect(cond.Message).To(Equal(feastdevv1.DeploymentNotAvailableMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.AuthorizationReadyType)
			Expect(cond).To(BeNil())

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

			ephemeralName := "feast-data"
			ephemeralVolume := corev1.Volume{
				Name: ephemeralName,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			}
			ephemeralVolMount := corev1.VolumeMount{
				Name:      ephemeralName,
				MountPath: "/" + ephemeralName,
			}

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
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(3))
			Expect(deploy.Spec.Template.Spec.SecurityContext).NotTo(Equal(securityContext))
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(3))
			Expect(deploy.Spec.Template.Spec.Volumes).NotTo(ContainElement(ephemeralVolume))
			name := feast.GetFeastServiceName(services.RegistryFeastType)
			regVol := services.GetRegistryVolume(feast.Handler.FeatureStore, deploy.Spec.Template.Spec.Volumes)
			Expect(regVol.Name).To(Equal(name))
			Expect(regVol.PersistentVolumeClaim.ClaimName).To(Equal(name))

			offlineContainer := services.GetOfflineContainer(*deploy)
			Expect(offlineContainer.VolumeMounts).To(HaveLen(3))
			Expect(offlineContainer.VolumeMounts).NotTo(ContainElement(ephemeralVolMount))
			offlineVolMount := services.GetOfflineVolumeMount(feast.Handler.FeatureStore, offlineContainer.VolumeMounts)
			Expect(offlineVolMount.MountPath).To(Equal(offlineStoreMountPath))
			offlinePvcName := feast.GetFeastServiceName(services.OfflineFeastType)
			Expect(offlineVolMount.Name).To(Equal(offlinePvcName))

			assertEnvFrom(*offlineContainer)

			// check offline pvc
			pvc := &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      offlinePvcName,
				Namespace: resource.Namespace,
			},
				pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Spec.StorageClassName).To(Equal(&storageClassName))
			Expect(pvc.Spec.AccessModes).To(Equal(accessModes))
			Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal(services.DefaultOfflineStorageRequest))
			Expect(pvc.DeletionTimestamp).To(BeNil())

			// check online
			onlinePvcName := feast.GetFeastServiceName(services.OnlineFeastType)
			onlineVol := services.GetOnlineVolume(feast.Handler.FeatureStore, deploy.Spec.Template.Spec.Volumes)
			Expect(onlineVol.Name).To(Equal(onlinePvcName))
			Expect(onlineVol.PersistentVolumeClaim.ClaimName).To(Equal(onlinePvcName))
			onlineContainer := services.GetOnlineContainer(*deploy)
			Expect(onlineContainer.VolumeMounts).To(HaveLen(3))
			Expect(onlineContainer.VolumeMounts).NotTo(ContainElement(ephemeralVolMount))
			onlineVolMount := services.GetOnlineVolumeMount(feast.Handler.FeatureStore, onlineContainer.VolumeMounts)
			Expect(onlineVolMount.MountPath).To(Equal(onlineStoreMountPath))
			Expect(onlineVolMount.Name).To(Equal(onlinePvcName))

			assertEnvFrom(*onlineContainer)

			// check online pvc
			pvc = &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      onlinePvcName,
				Namespace: resource.Namespace,
			},
				pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Name).To(Equal(onlinePvcName))
			Expect(pvc.Spec.AccessModes).To(Equal(services.DefaultPVCAccessModes))
			Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal(services.DefaultOnlineStorageRequest))
			Expect(pvc.DeletionTimestamp).To(BeNil())

			// check registry
			registryPvcName := feast.GetFeastServiceName(services.RegistryFeastType)
			registryVol := services.GetRegistryVolume(feast.Handler.FeatureStore, deploy.Spec.Template.Spec.Volumes)
			Expect(registryVol.Name).To(Equal(registryPvcName))
			Expect(registryVol.PersistentVolumeClaim.ClaimName).To(Equal(registryPvcName))
			registryContainer := services.GetRegistryContainer(*deploy)
			Expect(registryContainer.VolumeMounts).To(HaveLen(3))
			Expect(registryContainer.VolumeMounts).NotTo(ContainElement(ephemeralVolMount))
			registryVolMount := services.GetRegistryVolumeMount(feast.Handler.FeatureStore, registryContainer.VolumeMounts)
			Expect(registryVolMount.MountPath).To(Equal(registryMountPath))
			Expect(registryVolMount.Name).To(Equal(registryPvcName))

			// check registry pvc
			pvc = &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      registryPvcName,
				Namespace: resource.Namespace,
			},
				pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Name).To(Equal(registryPvcName))
			Expect(pvc.Spec.AccessModes).To(Equal(services.DefaultPVCAccessModes))
			Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal(services.DefaultRegistryStorageRequest))
			Expect(pvc.DeletionTimestamp).To(BeNil())

			// remove online PVC and reconcile
			resourceNew := resource.DeepCopy()
			newOnlineStorePath := "/tmp/new_online.db"
			resourceNew.Spec.Services.OnlineStore.Persistence.FilePersistence.Path = newOnlineStorePath
			resourceNew.Spec.Services.OnlineStore.Persistence.FilePersistence.PvcConfig = nil
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
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig).To(BeNil())

			// check online deployment/container
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(3))
			Expect(deploy.Spec.Template.Spec.Volumes).To(ContainElement(ephemeralVolume))
			Expect(services.GetOnlineContainer(*deploy).VolumeMounts).To(HaveLen(3))
			Expect(services.GetOnlineContainer(*deploy).VolumeMounts).To(ContainElement(ephemeralVolMount))
			Expect(services.GetRegistryContainer(*deploy).VolumeMounts).To(ContainElement(ephemeralVolMount))
			Expect(services.GetOfflineContainer(*deploy).VolumeMounts).To(ContainElement(ephemeralVolMount))

			// check online pvc is deleted
			log.FromContext(feast.Handler.Context).Info("Checking deletion of", "PersistentVolumeClaim", deploy.Name)
			pvc = &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      onlinePvcName,
				Namespace: resource.Namespace,
			},
				pvc)
			if err != nil {
				Expect(errors.IsNotFound(err)).To(BeTrue())
			} else {
				Expect(pvc.DeletionTimestamp).NotTo(BeNil())
			}
		})

		It("should properly encode a feature_store.yaml config", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			resource.Spec.Services.SecurityContext = securityContext

			err = k8sClient.Update(ctx, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

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
			Expect(svcList.Items).To(HaveLen(3))

			cmList := corev1.ConfigMapList{}
			err = k8sClient.List(ctx, &cmList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmList.Items).To(HaveLen(1))

			feast := services.FeastServices{
				Handler: handler.FeastHandler{
					Client:       controllerReconciler.Client,
					Context:      ctx,
					Scheme:       controllerReconciler.Scheme,
					FeatureStore: resource,
				},
			}

			// check deployment
			deploy := &appsv1.Deployment{}
			objMeta := feast.GetObjectMeta()
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(3))
			Expect(deploy.Spec.Template.Spec.SecurityContext).To(Equal(securityContext))
			registryContainer := services.GetRegistryContainer(*deploy)
			Expect(registryContainer.Env).To(HaveLen(1))
			env := getFeatureStoreYamlEnvVar(registryContainer.Env)
			Expect(env).NotTo(BeNil())

			// check registry config
			fsYamlStr, err := feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err := base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			testConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1.SerializationVersion,
				Registry: services.RegistryConfig{
					RegistryType: services.RegistryFileConfigType,
					Path:         registryMountedPath,
				},
				OfflineStore: services.OfflineStoreConfig{
					Type: services.OfflineFilePersistenceDuckDbConfigType,
				},
				OnlineStore: services.OnlineStoreConfig{
					Path: onlineStoreMountedPath,
					Type: services.OnlineSqliteConfigType,
				},
				AuthzConfig: noAuthzConfig(),
			}
			Expect(repoConfig).To(Equal(testConfig))

			offlineContainer := services.GetOfflineContainer(*deploy)
			Expect(offlineContainer.Env).To(HaveLen(1))
			env = getFeatureStoreYamlEnvVar(offlineContainer.Env)
			Expect(env).NotTo(BeNil())

			// check offline config
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfigOffline := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOffline)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfigOffline).To(Equal(testConfig))

			// check online config
			onlineContainer := services.GetOnlineContainer(*deploy)
			Expect(onlineContainer.Env).To(HaveLen(3))
			Expect(onlineContainer.ImagePullPolicy).To(Equal(corev1.PullAlways))
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
			Expect(repoConfigOnline).To(Equal(testConfig))

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
				Host: fmt.Sprintf("feast-%s-offline.default.svc.cluster.local", resourceName),
				Type: services.OfflineRemoteConfigType,
				Port: services.HttpPort,
			}
			regRemote := services.RegistryConfig{
				RegistryType: services.RegistryRemoteConfigType,
				Path:         fmt.Sprintf("feast-%s-registry.default.svc.cluster.local:80", resourceName),
			}
			clientConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1.SerializationVersion,
				OfflineStore:                  offlineRemote,
				OnlineStore: services.OnlineStoreConfig{
					Path: fmt.Sprintf("http://feast-%s-online.default.svc.cluster.local:80", resourceName),
					Type: services.OnlineRemoteConfigType,
				},
				Registry:    regRemote,
				AuthzConfig: noAuthzConfig(),
			}
			Expect(repoConfigClient).To(Equal(clientConfig))

			// change paths and reconcile
			resourceNew := resource.DeepCopy()
			newOnlineStorePath := "new_online.db"
			newRegistryPath := "new_registry.db"

			newOnlineStoreMountedPath := path.Join(onlineStoreMountPath, newOnlineStorePath)
			newRegistryMountedPath := path.Join(registryMountPath, newRegistryPath)

			resourceNew.Spec.Services.OnlineStore.Persistence.FilePersistence.Path = newOnlineStorePath
			resourceNew.Spec.Services.Registry.Local.Persistence.FilePersistence.Path = newRegistryPath
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

			// check registry config
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			registryContainer = services.GetRegistryContainer(*deploy)
			env = getFeatureStoreYamlEnvVar(registryContainer.Env)
			Expect(env).NotTo(BeNil())
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			testConfig.OnlineStore.Path = newOnlineStoreMountedPath
			testConfig.Registry.Path = newRegistryMountedPath
			Expect(repoConfig).To(Equal(testConfig))

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
			Expect(repoConfigOffline).To(Equal(testConfig))

			// check online config
			onlineContainer = services.GetOfflineContainer(*deploy)
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
			testConfig.OnlineStore.Path = newOnlineStoreMountedPath
			Expect(repoConfigOnline).To(Equal(testConfig))
		})
	})
})
