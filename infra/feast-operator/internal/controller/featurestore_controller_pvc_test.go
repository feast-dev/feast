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
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
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
		featurestore := &feastdevv1alpha1.FeatureStore{}
		onlineStorePath := "online.db"
		registryPath := "registry.db"
		offlineType := "duckdb"

		offlineStoreMountPath := "/offline"
		onlineStoreMountPath := "/online"
		registryMountPath := "/registry"

		storageClassName := "default"

		onlineStoreMountedPath := path.Join(onlineStoreMountPath, onlineStorePath)
		registryMountedPath := path.Join(registryMountPath, registryPath)

		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue},
					{Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}})
				resource.Spec.Services.OfflineStore.Persistence = &feastdevv1alpha1.OfflineStorePersistence{
					FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
						Type: offlineType,
						PvcConfig: &feastdevv1alpha1.PvcConfig{
							Create: &feastdevv1alpha1.PvcCreate{
								StorageClassName: &storageClassName,
							},
							MountPath: offlineStoreMountPath,
						},
					},
				}
				resource.Spec.Services.OnlineStore.Persistence = &feastdevv1alpha1.OnlineStorePersistence{
					FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
						Path: onlineStorePath,
						PvcConfig: &feastdevv1alpha1.PvcConfig{
							Create:    &feastdevv1alpha1.PvcCreate{},
							MountPath: onlineStoreMountPath,
						},
					},
				}
				resource.Spec.Services.Registry = &feastdevv1alpha1.Registry{
					Local: &feastdevv1alpha1.LocalRegistryConfig{
						Persistence: &feastdevv1alpha1.RegistryPersistence{
							FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
								Path: registryPath,
								PvcConfig: &feastdevv1alpha1.PvcConfig{
									Create:    &feastdevv1alpha1.PvcCreate{},
									MountPath: registryMountPath,
								},
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

			resource := &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			feast := services.FeastServices{
				Client:       controllerReconciler.Client,
				Context:      ctx,
				Scheme:       controllerReconciler.Scheme,
				FeatureStore: resource,
			}
			Expect(resource.Status).NotTo(BeNil())
			Expect(resource.Status.FeastVersion).To(Equal(feastversion.FeastVersion))
			Expect(resource.Status.ClientConfigMap).To(Equal(feast.GetFeastServiceName(services.ClientFeastType)))
			Expect(resource.Status.Applied.FeastProject).To(Equal(resource.Spec.FeastProject))
			Expect(resource.Status.Applied.Services).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.Type).To(Equal(offlineType))
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.StorageClassName).To(Equal(&storageClassName))
			expectedResources := corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: apiresource.MustParse("20Gi"),
				},
			}
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.Resources).To(Equal(expectedResources))
			Expect(resource.Status.Applied.Services.OfflineStore.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Image).To(Equal(&services.DefaultImage))
			Expect(resource.Status.Applied.Services.OnlineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.Path).To(Equal(onlineStorePath))
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.StorageClassName).To(BeNil())
			expectedResources = corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: apiresource.MustParse("5Gi"),
				},
			}
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.Resources).To(Equal(expectedResources))
			Expect(resource.Status.Applied.Services.OnlineStore.Env).To(Equal(&[]corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue}, {Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}))
			Expect(resource.Status.Applied.Services.OnlineStore.ImagePullPolicy).To(Equal(&pullPolicy))
			Expect(resource.Status.Applied.Services.OnlineStore.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Image).To(Equal(&image))
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.Path).To(Equal(registryPath))
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.StorageClassName).To(BeNil())
			expectedResources = corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: apiresource.MustParse("5Gi"),
				},
			}
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.Resources).To(Equal(expectedResources))
			Expect(resource.Status.Applied.Services.Registry.Local.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Image).To(Equal(&services.DefaultImage))

			Expect(resource.Status.ServiceHostnames.OfflineStore).To(Equal(feast.GetFeastServiceName(services.OfflineFeastType) + "." + resource.Namespace + domain))
			Expect(resource.Status.ServiceHostnames.OnlineStore).To(Equal(feast.GetFeastServiceName(services.OnlineFeastType) + "." + resource.Namespace + domain))
			Expect(resource.Status.ServiceHostnames.Registry).To(Equal(feast.GetFeastServiceName(services.RegistryFeastType) + "." + resource.Namespace + domain))

			Expect(resource.Status.Conditions).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.ReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.ReadyMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.RegistryReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.RegistryReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.RegistryReadyMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ClientReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.ClientReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.ClientReadyMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.OfflineStoreReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.OfflineStoreReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.OfflineStoreReadyMessage))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.OnlineStoreReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.OnlineStoreReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.OnlineStoreReadyMessage))

			Expect(resource.Status.Phase).To(Equal(feastdevv1alpha1.ReadyPhase))

			// check offline deployment
			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OfflineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Replicas).To(Equal(&services.DefaultReplicas))
			Expect(controllerutil.HasControllerReference(deploy)).To(BeTrue())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Volumes[0].Name).To(Equal(deploy.Name))
			Expect(deploy.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal(deploy.Name))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath).To(Equal(offlineStoreMountPath))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name).To(Equal(deploy.Name))

			// check offline pvc
			pvc := &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      deploy.Name,
				Namespace: resource.Namespace,
			},
				pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Name).To(Equal(deploy.Name))
			Expect(pvc.Spec.StorageClassName).To(Equal(&storageClassName))
			Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal(services.DefaultOfflineStorageRequest))
			Expect(pvc.DeletionTimestamp).To(BeNil())

			// check online deployment
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OnlineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Replicas).To(Equal(&services.DefaultReplicas))
			Expect(controllerutil.HasControllerReference(deploy)).To(BeTrue())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Volumes[0].Name).To(Equal(deploy.Name))
			Expect(deploy.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal(deploy.Name))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath).To(Equal(onlineStoreMountPath))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name).To(Equal(deploy.Name))

			// check online pvc
			pvc = &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      deploy.Name,
				Namespace: resource.Namespace,
			},
				pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Name).To(Equal(deploy.Name))
			Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal(services.DefaultOnlineStorageRequest))
			Expect(pvc.DeletionTimestamp).To(BeNil())

			// check registry deployment
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Replicas).To(Equal(&services.DefaultReplicas))
			Expect(controllerutil.HasControllerReference(deploy)).To(BeTrue())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Volumes[0].Name).To(Equal(deploy.Name))
			Expect(deploy.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal(deploy.Name))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath).To(Equal(registryMountPath))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name).To(Equal(deploy.Name))

			// check registry pvc
			pvc = &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      deploy.Name,
				Namespace: resource.Namespace,
			},
				pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.Name).To(Equal(deploy.Name))
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

			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast.FeatureStore = resource
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig).To(BeNil())

			// check online deployment
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OnlineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(0))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts).To(HaveLen(0))

			// check online pvc is deleted
			log.FromContext(feast.Context).Info("Checking deletion of", "PersistentVolumeClaim", deploy.Name)
			pvc = &corev1.PersistentVolumeClaim{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      deploy.Name,
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

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource := &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			req, err := labels.NewRequirement(services.NameLabelKey, selection.Equals, []string{resource.Name})
			Expect(err).NotTo(HaveOccurred())
			labelSelector := labels.NewSelector().Add(*req)
			listOpts := &client.ListOptions{Namespace: resource.Namespace, LabelSelector: labelSelector}
			deployList := appsv1.DeploymentList{}
			err = k8sClient.List(ctx, &deployList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(deployList.Items).To(HaveLen(3))

			svcList := corev1.ServiceList{}
			err = k8sClient.List(ctx, &svcList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(svcList.Items).To(HaveLen(3))

			cmList := corev1.ConfigMapList{}
			err = k8sClient.List(ctx, &cmList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmList.Items).To(HaveLen(1))

			feast := services.FeastServices{
				Client:       controllerReconciler.Client,
				Context:      ctx,
				Scheme:       controllerReconciler.Scheme,
				FeatureStore: resource,
			}

			// check registry deployment
			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(1))
			env := getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			// check registry config
			fsYamlStr, err := feast.GetServiceFeatureStoreYamlBase64(services.RegistryFeastType)
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
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				Registry: services.RegistryConfig{
					RegistryType: services.RegistryFileConfigType,
					Path:         registryMountedPath,
				},
			}
			Expect(repoConfig).To(Equal(testConfig))

			// check offline deployment
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OfflineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(1))
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			// check offline config
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.OfflineFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfigOffline := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOffline)
			Expect(err).NotTo(HaveOccurred())
			regRemote := services.RegistryConfig{
				RegistryType: services.RegistryRemoteConfigType,
				Path:         fmt.Sprintf("feast-%s-registry.default.svc.cluster.local:80", resourceName),
			}
			offlineConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore: services.OfflineStoreConfig{
					Type: services.OfflineDuckDbConfigType,
				},
				Registry: regRemote,
			}
			Expect(repoConfigOffline).To(Equal(offlineConfig))

			// check online config
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OnlineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(3))
			Expect(deploy.Spec.Template.Spec.Containers[0].ImagePullPolicy).To(Equal(corev1.PullAlways))
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.OnlineFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfigOnline := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOnline)
			Expect(err).NotTo(HaveOccurred())
			offlineRemote := services.OfflineStoreConfig{
				Host: fmt.Sprintf("feast-%s-offline.default.svc.cluster.local", resourceName),
				Type: services.OfflineRemoteConfigType,
				Port: services.HttpPort,
			}
			onlineConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore:                  offlineRemote,
				OnlineStore: services.OnlineStoreConfig{
					Path: onlineStoreMountedPath,
					Type: services.OnlineSqliteConfigType,
				},
				Registry: regRemote,
			}
			Expect(repoConfigOnline).To(Equal(onlineConfig))
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(3))

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
			clientConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore:                  offlineRemote,
				OnlineStore: services.OnlineStoreConfig{
					Path: fmt.Sprintf("http://feast-%s-online.default.svc.cluster.local:80", resourceName),
					Type: services.OnlineRemoteConfigType,
				},
				Registry: regRemote,
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

			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast.FeatureStore = resource

			// check registry config
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.RegistryFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			testConfig.Registry.Path = newRegistryMountedPath
			Expect(repoConfig).To(Equal(testConfig))

			// check offline config
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OfflineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.OfflineFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfigOffline = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOffline)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfigOffline).To(Equal(offlineConfig))

			// check online config
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OnlineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.OnlineFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())

			repoConfigOnline = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOnline)
			Expect(err).NotTo(HaveOccurred())
			onlineConfig.OnlineStore.Path = newOnlineStoreMountedPath
			Expect(repoConfigOnline).To(Equal(onlineConfig))
		})
	})
})
