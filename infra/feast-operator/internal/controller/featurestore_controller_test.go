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
	"reflect"

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
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

const feastProject = "test_project"

var _ = Describe("FeatureStore Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1alpha1.FeatureStore{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := &feastdevv1alpha1.FeatureStore{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: feastdevv1alpha1.FeatureStoreSpec{FeastProject: feastProject},
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
			By("Reconciling the minimal created resource")
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
			Expect(deployList.Items).To(HaveLen(1))

			svcList := corev1.ServiceList{}
			err = k8sClient.List(ctx, &svcList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(svcList.Items).To(HaveLen(1))

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
			Expect(resource.Status).NotTo(BeNil())
			Expect(resource.Status.FeastVersion).To(Equal(feastversion.FeastVersion))
			Expect(resource.Status.ClientConfigMap).To(Equal(feast.GetFeastServiceName(services.ClientFeastType)))
			Expect(resource.Status.ServiceHostnames.OfflineStore).To(BeEmpty())
			Expect(resource.Status.ServiceHostnames.OnlineStore).To(BeEmpty())
			Expect(resource.Status.ServiceHostnames.Registry).To(Equal(feast.GetFeastServiceName(services.RegistryFeastType) + "." + resource.Namespace + ".svc.cluster.local:80"))
			Expect(resource.Status.Applied.FeastProject).To(Equal(resource.Spec.FeastProject))
			Expect(resource.Status.Applied.Services).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore).To(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Remote).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Image).To(Equal(&services.DefaultImage))

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

			Expect(resource.Status.Phase).To(Equal(feastdevv1alpha1.ReadyPhase))

			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Replicas).To(Equal(&services.DefaultReplicas))
			Expect(controllerutil.HasControllerReference(deploy)).To(BeTrue())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))

			svc := &corev1.Service{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(svc)).To(BeTrue())
			Expect(svc.Spec.Ports[0].TargetPort).To(Equal(intstr.FromInt(int(services.FeastServiceConstants[services.RegistryFeastType].TargetPort))))
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

			feast := services.FeastServices{
				Client:       controllerReconciler.Client,
				Context:      ctx,
				Scheme:       controllerReconciler.Scheme,
				FeatureStore: resource,
			}

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
					Path:         services.LocalRegistryPath,
				},
			}
			Expect(repoConfig).To(Equal(testConfig))

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
				Registry: services.RegistryConfig{
					RegistryType: services.RegistryRemoteConfigType,
					Path:         "feast-test-resource-registry.default.svc.cluster.local:80",
				},
			}
			Expect(repoConfigClient).To(Equal(clientConfig))

			// change feast project and reconcile
			resourceNew := resource.DeepCopy()
			resourceNew.Spec.FeastProject = "changed"
			err = k8sClient.Update(ctx, resourceNew)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(resource.Spec.FeastProject).To(Equal(resourceNew.Spec.FeastProject))
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())

			testConfig.Project = resourceNew.Spec.FeastProject
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(1))
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.RegistryFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig).To(Equal(testConfig))
		})

		It("should error on reconcile", func() {
			By("Trying to set the controller OwnerRef of a Deployment that already has a controller")
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

			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())

			err = controllerutil.RemoveControllerReference(resource, deploy, controllerReconciler.Scheme)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(deploy)).To(BeFalse())

			svc := &corev1.Service{}
			name := feast.GetFeastServiceName(services.RegistryFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: resource.Namespace,
			},
				svc)
			Expect(err).NotTo(HaveOccurred())
			err = controllerutil.SetControllerReference(svc, deploy, controllerReconciler.Scheme)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(deploy)).To(BeTrue())
			err = k8sClient.Update(ctx, deploy)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())

			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(resource.Status.Conditions).To(HaveLen(3))

			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Type).To(Equal(feastdevv1alpha1.ReadyType))
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.FailedReason))
			Expect(cond.Message).To(Equal("Error: Object " + resource.Namespace + "/" + name + " is already owned by another Service controller " + name))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.RegistryReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.RegistryFailedReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.RegistryReadyType))
			Expect(cond.Message).To(Equal("Error: Object " + resource.Namespace + "/" + name + " is already owned by another Service controller " + name))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ClientReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.ClientReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.ClientReadyMessage))

			Expect(resource.Status.Phase).To(Equal(feastdevv1alpha1.FailedPhase))
		})
	})

	Context("When reconciling a resource with all services enabled", func() {
		const resourceName = "services"
		image := "test:latest"
		var pullPolicy = corev1.PullAlways
		var testEnvVarName = "testEnvVarName"
		var testEnvVarValue = "testEnvVarValue"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1alpha1.FeatureStore{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue},
					{Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}})
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
			Expect(resource.Status.Applied.Services.OfflineStore.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Image).To(Equal(&services.DefaultImage))
			Expect(resource.Status.Applied.Services.OnlineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Env).To(Equal(&[]corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue}, {Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}))
			Expect(resource.Status.Applied.Services.OnlineStore.ImagePullPolicy).To(Equal(&pullPolicy))
			Expect(resource.Status.Applied.Services.OnlineStore.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Image).To(Equal(&image))
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Image).To(Equal(&services.DefaultImage))

			domain := ".svc.cluster.local:80"
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

			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Replicas).To(Equal(&services.DefaultReplicas))
			Expect(controllerutil.HasControllerReference(deploy)).To(BeTrue())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))

			svc := &corev1.Service{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(svc)).To(BeTrue())
			Expect(svc.Spec.Ports[0].TargetPort).To(Equal(intstr.FromInt(int(services.FeastServiceConstants[services.RegistryFeastType].TargetPort))))
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

			// check registry config
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
					Path:         services.LocalRegistryPath,
				},
			}
			Expect(repoConfig).To(Equal(testConfig))

			// check offline config
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
				Path:         "feast-services-registry.default.svc.cluster.local:80",
			}
			offlineConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore: services.OfflineStoreConfig{
					Type: services.OfflineDaskConfigType,
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
				Host: "feast-services-offline.default.svc.cluster.local",
				Type: services.OfflineRemoteConfigType,
				Port: services.HttpPort,
			}
			onlineConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore:                  offlineRemote,
				OnlineStore: services.OnlineStoreConfig{
					Path: services.LocalOnlinePath,
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
					Path: "http://feast-services-online.default.svc.cluster.local:80",
					Type: services.OnlineRemoteConfigType,
				},
				Registry: regRemote,
			}
			Expect(repoConfigClient).To(Equal(clientConfig))

			// change feast project and reconcile
			resourceNew := resource.DeepCopy()
			resourceNew.Spec.FeastProject = "changed"
			err = k8sClient.Update(ctx, resourceNew)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(resource.Spec.FeastProject).To(Equal(resourceNew.Spec.FeastProject))
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())

			testConfig.Project = resourceNew.Spec.FeastProject
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(1))
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.RegistryFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig).To(Equal(testConfig))
		})

		It("should properly set container env variables", func() {
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

			fsYamlStr := ""
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.OnlineFeastType)
			Expect(err).NotTo(HaveOccurred())

			// check online config
			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OnlineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(3))
			Expect(areEnvVarArraysEqual(deploy.Spec.Template.Spec.Containers[0].Env, []corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue}, {Name: services.FeatureStoreYamlEnvVar, Value: fsYamlStr}, {Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}})).To(BeTrue())
			Expect(deploy.Spec.Template.Spec.Containers[0].ImagePullPolicy).To(Equal(corev1.PullAlways))

			// change feast project and reconcile
			resourceNew := resource.DeepCopy()
			resourceNew.Spec.Services.OnlineStore.Env = &[]corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue + "1"}, {Name: services.FeatureStoreYamlEnvVar, Value: fsYamlStr}, {Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}}
			err = k8sClient.Update(ctx, resourceNew)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(areEnvVarArraysEqual(*resource.Status.Applied.Services.OnlineStore.Env, []corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue + "1"}, {Name: services.FeatureStoreYamlEnvVar, Value: fsYamlStr}, {Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}})).To(BeTrue())
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OnlineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())

			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(HaveLen(3))
			Expect(areEnvVarArraysEqual(deploy.Spec.Template.Spec.Containers[0].Env, []corev1.EnvVar{{Name: testEnvVarName, Value: testEnvVarValue + "1"}, {Name: services.FeatureStoreYamlEnvVar, Value: fsYamlStr}, {Name: "fieldRefName", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"}}}})).To(BeTrue())
		})

		It("Should delete k8s objects owned by the FeatureStore CR", func() {
			By("changing which feast services are configured in the CR")
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

			// disable the Online Store service
			resource.Spec.Services.OnlineStore = nil
			err = k8sClient.Update(ctx, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.List(ctx, &deployList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(deployList.Items).To(HaveLen(2))

			err = k8sClient.List(ctx, &svcList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(svcList.Items).To(HaveLen(2))

			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			// disable the Offline Store service as well
			resource.Spec.Services.OfflineStore = nil
			err = k8sClient.Update(ctx, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.List(ctx, &deployList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(deployList.Items).To(HaveLen(1))

			err = k8sClient.List(ctx, &svcList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(svcList.Items).To(HaveLen(1))
		})

		It("should handle remote registry references", func() {
			By("By properly configuring feast")

			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			referencedRegistry := &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, referencedRegistry)
			Expect(err).NotTo(HaveOccurred())

			name := "remote-registry-reference"
			resource := &feastdevv1alpha1.FeatureStore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: referencedRegistry.Namespace,
				},
				Spec: feastdevv1alpha1.FeatureStoreSpec{
					FeastProject: referencedRegistry.Spec.FeastProject,
					Services: &feastdevv1alpha1.FeatureStoreServices{
						OnlineStore:  &feastdevv1alpha1.OnlineStore{},
						OfflineStore: &feastdevv1alpha1.OfflineStore{},
						Registry: &feastdevv1alpha1.Registry{
							Remote: &feastdevv1alpha1.RemoteRegistryConfig{
								FeastRef: &feastdevv1alpha1.FeatureStoreRef{
									Name: name,
								},
							},
						},
					},
				},
			}
			resource.SetGroupVersionKind(feastdevv1alpha1.GroupVersion.WithKind("FeatureStore"))
			nsName := client.ObjectKeyFromObject(resource)
			err = k8sClient.Create(ctx, resource)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: nsName,
			})
			Expect(err).To(HaveOccurred())
			err = k8sClient.Get(ctx, nsName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.RegistryReadyType)).To(BeNil())
			Expect(apimeta.IsStatusConditionTrue(resource.Status.Conditions, feastdevv1alpha1.ReadyType)).To(BeFalse())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ReadyType)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Message).To(Equal("Error: FeatureStore '" + name + "' can't reference itself in `spec.services.registry.remote.feastRef`"))

			resource.Spec.Services.Registry.Remote.FeastRef.Name = "wrong"
			err = k8sClient.Update(ctx, resource)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: nsName,
			})
			Expect(err).To(HaveOccurred())
			err = k8sClient.Get(ctx, nsName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.RegistryReadyType)).To(BeNil())
			Expect(apimeta.IsStatusConditionTrue(resource.Status.Conditions, feastdevv1alpha1.ReadyType)).To(BeFalse())
			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ReadyType)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Message).To(Equal("Error: Referenced FeatureStore '" + resource.Spec.Services.Registry.Remote.FeastRef.Name + "' was not found"))

			resource.Spec.Services.Registry.Remote.FeastRef.Name = referencedRegistry.Name
			err = k8sClient.Update(ctx, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: nsName,
			})
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, nsName, resource)
			Expect(err).NotTo(HaveOccurred())

			Expect(apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.RegistryReadyType)).To(BeNil())
			Expect(apimeta.IsStatusConditionTrue(resource.Status.Conditions, feastdevv1alpha1.ReadyType)).To(BeTrue())
			Expect(apimeta.IsStatusConditionTrue(resource.Status.Conditions, feastdevv1alpha1.OnlineStoreReadyType)).To(BeTrue())
			Expect(apimeta.IsStatusConditionTrue(resource.Status.Conditions, feastdevv1alpha1.OfflineStoreReadyType)).To(BeTrue())
			Expect(resource.Status.Applied.Services.Registry.Remote.FeastRef.Namespace).To(Equal(resource.Namespace))
			Expect(resource.Status.ServiceHostnames.Registry).ToNot(BeEmpty())
			Expect(resource.Status.ServiceHostnames.Registry).To(Equal(referencedRegistry.Status.ServiceHostnames.Registry))
			feast := services.FeastServices{
				Client:       controllerReconciler.Client,
				Context:      ctx,
				Scheme:       controllerReconciler.Scheme,
				FeatureStore: resource,
			}

			// check client config
			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.ClientFeastType),
				Namespace: resource.Namespace,
			}, cm)
			Expect(err).NotTo(HaveOccurred())
			repoConfigClient := &services.RepoConfig{}
			err = yaml.Unmarshal([]byte(cm.Data[services.FeatureStoreYamlCmKey]), repoConfigClient)
			Expect(err).NotTo(HaveOccurred())
			clientConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore: services.OfflineStoreConfig{
					Host: "feast-" + resource.Name + "-offline.default.svc.cluster.local",
					Type: services.OfflineRemoteConfigType,
					Port: services.HttpPort,
				},
				OnlineStore: services.OnlineStoreConfig{
					Path: "http://feast-" + resource.Name + "-online.default.svc.cluster.local:80",
					Type: services.OnlineRemoteConfigType,
				},
				Registry: services.RegistryConfig{
					RegistryType: services.RegistryRemoteConfigType,
					Path:         "feast-" + referencedRegistry.Name + "-registry.default.svc.cluster.local:80",
				},
			}
			Expect(repoConfigClient).To(Equal(clientConfig))

			hostname := "test:80"
			referencedRegistry.Spec.Services.Registry = &feastdevv1alpha1.Registry{
				Remote: &feastdevv1alpha1.RemoteRegistryConfig{
					Hostname: &hostname,
				},
			}
			err = k8sClient.Update(ctx, referencedRegistry)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: nsName,
			})
			Expect(err).To(HaveOccurred())

			err = k8sClient.Get(ctx, nsName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(resource.Status.ServiceHostnames.Registry).To(BeEmpty())
			Expect(apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.RegistryReadyType)).To(BeNil())
			Expect(apimeta.IsStatusConditionTrue(resource.Status.Conditions, feastdevv1alpha1.ReadyType)).To(BeFalse())
			Expect(apimeta.IsStatusConditionTrue(resource.Status.Conditions, feastdevv1alpha1.OnlineStoreReadyType)).To(BeTrue())
			Expect(apimeta.IsStatusConditionTrue(resource.Status.Conditions, feastdevv1alpha1.OfflineStoreReadyType)).To(BeTrue())
			Expect(resource.Status.Applied.Services.Registry.Remote.FeastRef.Name).To(Equal(referencedRegistry.Name))
			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ReadyType)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Message).To(Equal("Error: Remote feast registry of referenced FeatureStore '" + referencedRegistry.Name + "' is not ready"))
		})

		It("should error on reconcile", func() {
			By("Trying to set the controller OwnerRef of a Deployment that already has a controller")
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

			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OfflineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())

			err = controllerutil.RemoveControllerReference(resource, deploy, controllerReconciler.Scheme)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(deploy)).To(BeFalse())

			svc := &corev1.Service{}
			name := feast.GetFeastServiceName(services.OfflineFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: resource.Namespace,
			},
				svc)
			Expect(err).NotTo(HaveOccurred())
			err = controllerutil.SetControllerReference(svc, deploy, controllerReconciler.Scheme)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(deploy)).To(BeTrue())
			err = k8sClient.Update(ctx, deploy)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())

			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(resource.Status.Conditions).To(HaveLen(5))

			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Type).To(Equal(feastdevv1alpha1.ReadyType))
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.FailedReason))
			Expect(cond.Message).To(Equal("Error: Object " + resource.Namespace + "/" + name + " is already owned by another Service controller " + name))

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
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.OfflineStoreFailedReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.OfflineStoreReadyType))
			Expect(cond.Message).To(Equal("Error: Object " + resource.Namespace + "/" + name + " is already owned by another Service controller " + name))

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.OnlineStoreReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.OnlineStoreReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.OnlineStoreReadyMessage))

			Expect(resource.Status.Phase).To(Equal(feastdevv1alpha1.FailedPhase))
		})

		It("should error on reconcile", func() {
			By("By failing to pass CRD schema validation")

			resource := &feastdevv1alpha1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			Expect(resource.Spec.Services.Registry).To(BeNil())

			resource.Spec.Services.Registry = &feastdevv1alpha1.Registry{}
			err = k8sClient.Update(ctx, resource)
			Expect(err).To(HaveOccurred())

			resource.Spec.Services.Registry = &feastdevv1alpha1.Registry{
				Local:  &feastdevv1alpha1.LocalRegistryConfig{},
				Remote: &feastdevv1alpha1.RemoteRegistryConfig{},
			}
			err = k8sClient.Update(ctx, resource)
			Expect(err).To(HaveOccurred())

			resource.Spec.Services.Registry = &feastdevv1alpha1.Registry{
				Remote: &feastdevv1alpha1.RemoteRegistryConfig{},
			}
			err = k8sClient.Update(ctx, resource)
			Expect(err).To(HaveOccurred())

			hostname := "test:80"
			resource.Spec.Services.Registry = &feastdevv1alpha1.Registry{
				Remote: &feastdevv1alpha1.RemoteRegistryConfig{
					Hostname: &hostname,
					FeastRef: &feastdevv1alpha1.FeatureStoreRef{
						Name: "test",
					},
				},
			}
			err = k8sClient.Update(ctx, resource)
			Expect(err).To(HaveOccurred())

			resource.Spec.Services.Registry = &feastdevv1alpha1.Registry{
				Remote: &feastdevv1alpha1.RemoteRegistryConfig{
					FeastRef: &feastdevv1alpha1.FeatureStoreRef{
						Name: "test",
					},
				},
			}
			err = k8sClient.Update(ctx, resource)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

func createFeatureStoreResource(resourceName string, image string, pullPolicy corev1.PullPolicy, envVars *[]corev1.EnvVar) *feastdevv1alpha1.FeatureStore {
	return &feastdevv1alpha1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: "default",
		},
		Spec: feastdevv1alpha1.FeatureStoreSpec{
			FeastProject: feastProject,
			Services: &feastdevv1alpha1.FeatureStoreServices{
				OfflineStore: &feastdevv1alpha1.OfflineStore{},
				OnlineStore: &feastdevv1alpha1.OnlineStore{
					ServiceConfigs: feastdevv1alpha1.ServiceConfigs{
						DefaultConfigs: feastdevv1alpha1.DefaultConfigs{
							Image: &image,
						},
						OptionalConfigs: feastdevv1alpha1.OptionalConfigs{
							Env:             envVars,
							ImagePullPolicy: &pullPolicy,
							Resources:       &corev1.ResourceRequirements{},
						},
					},
				},
			},
		},
	}
}

func getFeatureStoreYamlEnvVar(envs []corev1.EnvVar) *corev1.EnvVar {
	for _, e := range envs {
		if e.Name == services.FeatureStoreYamlEnvVar {
			return &e
		}
	}
	return nil
}

func areEnvVarArraysEqual(arr1 []corev1.EnvVar, arr2 []corev1.EnvVar) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	// Create a map to count occurrences of EnvVars in the first array.
	envMap := make(map[string]corev1.EnvVar)

	for _, env := range arr1 {
		envMap[env.Name] = env
	}

	// Check the second array against the map.
	for _, env := range arr2 {
		if _, exists := envMap[env.Name]; !exists || !reflect.DeepEqual(envMap[env.Name], env) {
			return false
		}
	}

	return true
}
