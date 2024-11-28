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
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/authz"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("FeatureStore Controller-Kubernetes authorization", func() {
	Context("When deploying a resource with all ephemeral services and Kubernetes authorization", func() {
		const resourceName = "kubernetes-authorization"
		var pullPolicy = corev1.PullAlways

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1alpha1.FeatureStore{}
		roles := []string{"reader", "writer"}

		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{})
				resource.Spec.AuthzConfig = &feastdevv1alpha1.AuthzConfig{KubernetesAuthz: &feastdevv1alpha1.KubernetesAuthz{
					Roles: roles,
				}}

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
			expectedAuthzConfig := &feastdevv1alpha1.AuthzConfig{
				KubernetesAuthz: &feastdevv1alpha1.KubernetesAuthz{
					Roles: roles,
				},
			}
			Expect(resource.Status.Applied.AuthzConfig).To(Equal(expectedAuthzConfig))
			Expect(resource.Status.Applied.Services).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.Type).To(Equal(string(services.OfflineFilePersistenceDaskConfigType)))
			Expect(resource.Status.Applied.Services.OfflineStore.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Image).To(Equal(&services.DefaultImage))
			Expect(resource.Status.Applied.Services.OnlineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.Path).To(Equal(services.DefaultOnlineStoreEphemeralPath))
			Expect(resource.Status.Applied.Services.OnlineStore.Env).To(Equal(&[]corev1.EnvVar{}))
			Expect(resource.Status.Applied.Services.OnlineStore.ImagePullPolicy).To(Equal(&pullPolicy))
			Expect(resource.Status.Applied.Services.OnlineStore.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Image).To(Equal(&image))
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.Path).To(Equal(services.DefaultRegistryEphemeralPath))
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

			cond = apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.AuthorizationReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.AuthorizationReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.KubernetesAuthzReadyMessage))

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
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(0))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts).To(HaveLen(0))

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
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(0))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts).To(HaveLen(0))

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
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(0))
			Expect(deploy.Spec.Template.Spec.Containers[0].VolumeMounts).To(HaveLen(0))

			// check configured Roles
			for _, roleName := range roles {
				role := &rbacv1.Role{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      roleName,
					Namespace: resource.Namespace,
				},
					role)
				Expect(err).NotTo(HaveOccurred())
				Expect(role.Rules).To(BeEmpty())
			}

			// check Feast Role
			feastRole := &rbacv1.Role{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      authz.GetFeastRoleName(resource),
				Namespace: resource.Namespace,
			},
				feastRole)
			Expect(err).NotTo(HaveOccurred())
			Expect(feastRole.Rules).ToNot(BeEmpty())
			Expect(feastRole.Rules).To(HaveLen(1))
			Expect(feastRole.Rules[0].APIGroups).To(HaveLen(1))
			Expect(feastRole.Rules[0].APIGroups[0]).To(Equal(rbacv1.GroupName))
			Expect(feastRole.Rules[0].Resources).To(HaveLen(2))
			Expect(feastRole.Rules[0].Resources).To(ContainElement("roles"))
			Expect(feastRole.Rules[0].Resources).To(ContainElement("rolebindings"))
			Expect(feastRole.Rules[0].Verbs).To(HaveLen(3))
			Expect(feastRole.Rules[0].Verbs).To(ContainElement("get"))
			Expect(feastRole.Rules[0].Verbs).To(ContainElement("list"))
			Expect(feastRole.Rules[0].Verbs).To(ContainElement("watch"))

			// check RoleBinding
			roleBinding := &rbacv1.RoleBinding{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      authz.GetFeastRoleName(resource),
				Namespace: resource.Namespace,
			},
				roleBinding)
			Expect(err).NotTo(HaveOccurred())

			// check ServiceAccounts
			expectedRoleRef := rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     feastRole.Name,
			}
			for _, serviceType := range []services.FeastServiceType{services.RegistryFeastType, services.OnlineFeastType, services.OfflineFeastType} {
				sa := &corev1.ServiceAccount{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      feast.GetFeastServiceName(serviceType),
					Namespace: resource.Namespace,
				},
					sa)
				Expect(err).NotTo(HaveOccurred())

				expectedSubject := rbacv1.Subject{
					Kind:      rbacv1.ServiceAccountKind,
					Name:      sa.Name,
					Namespace: sa.Namespace,
				}
				Expect(roleBinding.Subjects).To(ContainElement(expectedSubject))
				Expect(roleBinding.RoleRef).To(Equal(expectedRoleRef))
			}

			By("Updating the user roled and reconciling")
			resourceNew := resource.DeepCopy()
			rolesNew := roles[1:]
			resourceNew.Spec.AuthzConfig.KubernetesAuthz.Roles = rolesNew
			err = k8sClient.Update(ctx, resourceNew)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast.Handler.FeatureStore = resource

			// check new Roles
			for _, roleName := range rolesNew {
				role := &rbacv1.Role{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      roleName,
					Namespace: resource.Namespace,
				},
					role)
				Expect(err).NotTo(HaveOccurred())
				Expect(role.Rules).To(BeEmpty())
			}

			// check deleted Role
			role := &rbacv1.Role{}
			deletedRole := roles[0]
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      deletedRole,
				Namespace: resource.Namespace,
			},
				role)
			Expect(err).To(HaveOccurred())
			Expect(errors.IsNotFound(err)).To(BeTrue())

			By("Clearing the kubernetes authorization and reconciling")
			resourceNew = resource.DeepCopy()
			resourceNew.Spec.AuthzConfig = nil
			err = k8sClient.Update(ctx, resourceNew)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast.Handler.FeatureStore = resource

			// check no Roles
			for _, roleName := range roles {
				role := &rbacv1.Role{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      roleName,
					Namespace: resource.Namespace,
				},
					role)
				Expect(err).To(HaveOccurred())
				Expect(errors.IsNotFound(err)).To(BeTrue())
			}
			// check no RoleBinding
			roleBinding = &rbacv1.RoleBinding{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      authz.GetFeastRoleName(resource),
				Namespace: resource.Namespace,
			},
				roleBinding)
			Expect(err).To(HaveOccurred())
			Expect(errors.IsNotFound(err)).To(BeTrue())
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
				Handler: handler.FeastHandler{
					Client:       controllerReconciler.Client,
					Context:      ctx,
					Scheme:       controllerReconciler.Scheme,
					FeatureStore: resource,
				},
			}

			// check registry deployment
			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
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
					RegistryType:       services.RegistryFileConfigType,
					Path:               services.DefaultRegistryEphemeralPath,
					S3AdditionalKwargs: nil,
				},
				AuthzConfig: services.AuthzConfig{
					Type: services.KubernetesAuthType,
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
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			// check offline config
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.OfflineFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			regRemote := services.RegistryConfig{
				RegistryType: services.RegistryRemoteConfigType,
				Path:         fmt.Sprintf("feast-%s-registry.default.svc.cluster.local:80", resourceName),
			}
			testConfig = &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore: services.OfflineStoreConfig{
					Type: services.OfflineFilePersistenceDaskConfigType,
				},
				Registry: regRemote,
				AuthzConfig: services.AuthzConfig{
					Type: services.KubernetesAuthType,
				},
			}
			Expect(repoConfig).To(Equal(testConfig))

			// check online deployment
			deploy = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.OnlineFeastType),
				Namespace: resource.Namespace,
			},
				deploy)
			Expect(err).NotTo(HaveOccurred())
			env = getFeatureStoreYamlEnvVar(deploy.Spec.Template.Spec.Containers[0].Env)
			Expect(env).NotTo(BeNil())

			// check online config
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64(services.OnlineFeastType)
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			offlineRemote := services.OfflineStoreConfig{
				Host: fmt.Sprintf("feast-%s-offline.default.svc.cluster.local", resourceName),
				Type: services.OfflineRemoteConfigType,
				Port: services.HttpPort,
			}
			testConfig = &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore:                  offlineRemote,
				OnlineStore: services.OnlineStoreConfig{
					Path: services.DefaultOnlineStoreEphemeralPath,
					Type: services.OnlineSqliteConfigType,
				},
				Registry: regRemote,
				AuthzConfig: services.AuthzConfig{
					Type: services.KubernetesAuthType,
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
				OfflineStore:                  offlineRemote,
				OnlineStore: services.OnlineStoreConfig{
					Path: fmt.Sprintf("http://feast-%s-online.default.svc.cluster.local:80", resourceName),
					Type: services.OnlineRemoteConfigType,
				},
				Registry: services.RegistryConfig{
					RegistryType: services.RegistryRemoteConfigType,
					Path:         fmt.Sprintf("feast-%s-registry.default.svc.cluster.local:80", resourceName),
				},
				AuthzConfig: services.AuthzConfig{
					Type: services.KubernetesAuthType,
				},
			}
			Expect(repoConfigClient).To(Equal(clientConfig))
		})
	})
})
