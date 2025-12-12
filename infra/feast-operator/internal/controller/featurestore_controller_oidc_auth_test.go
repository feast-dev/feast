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
	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/authz"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("FeatureStore Controller-OIDC authorization", func() {
	Context("When deploying a resource with all ephemeral services and OIDC authorization", func() {
		const resourceName = "oidc-authorization"
		const oidcSecretName = "oidc-secret"
		var pullPolicy = corev1.PullAlways

		ctx := context.Background()

		typeNamespacedSecretName := types.NamespacedName{
			Name:      oidcSecretName,
			Namespace: "default",
		}
		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1.FeatureStore{}

		BeforeEach(func() {
			By("creating the OIDC secret")
			oidcSecret := createValidOidcSecret(oidcSecretName)
			err := k8sClient.Get(ctx, typeNamespacedSecretName, oidcSecret)
			if err != nil && errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, oidcSecret)).To(Succeed())
			}

			createEnvFromSecretAndConfigMap()

			By("creating the custom resource for the Kind FeatureStore")
			err = k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{}, withEnvFrom())
				resource.Spec.AuthzConfig = &feastdevv1.AuthzConfig{OidcAuthz: &feastdevv1.OidcAuthz{
					SecretRef: corev1.LocalObjectReference{
						Name: oidcSecretName,
					},
				}}

				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}

		})
		AfterEach(func() {
			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			oidcSecret := createValidOidcSecret(oidcSecretName)
			err = k8sClient.Get(ctx, typeNamespacedSecretName, oidcSecret)
			if err != nil && errors.IsNotFound(err) {
				By("Cleanup the OIDC secret")
				Expect(k8sClient.Delete(ctx, oidcSecret)).To(Succeed())
			}

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
					FeatureStore: convertV1ToV1Alpha1ForTests(resource),
				},
			}
			Expect(resource.Status).NotTo(BeNil())
			Expect(resource.Status.FeastVersion).To(Equal(feastversion.FeastVersion))
			Expect(resource.Status.ClientConfigMap).To(Equal(feast.GetFeastServiceName(services.ClientFeastType)))
			Expect(resource.Status.Applied.FeastProject).To(Equal(resource.Spec.FeastProject))
			expectedAuthzConfig := &feastdevv1.AuthzConfig{
				OidcAuthz: &feastdevv1.OidcAuthz{
					SecretRef: corev1.LocalObjectReference{
						Name: oidcSecretName,
					},
				},
			}
			Expect(resource.Status.Applied.AuthzConfig).To(Equal(expectedAuthzConfig))
			Expect(resource.Status.Applied.Services).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.Type).To(Equal(string(services.OfflineFilePersistenceDaskConfigType)))
			Expect(resource.Status.Applied.Services.OfflineStore.Server.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Server.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Server.Image).To(Equal(&services.DefaultImage))
			Expect(resource.Status.Applied.Services.OnlineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.Path).To(Equal(services.EphemeralPath + "/" + services.DefaultOnlineStorePath))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.Env).To(Equal(&[]corev1.EnvVar{}))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.EnvFrom).To(Equal(withEnvFrom()))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.ImagePullPolicy).To(Equal(&pullPolicy))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Server.Image).To(Equal(&image))
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.Path).To(Equal(services.EphemeralPath + "/" + services.DefaultRegistryPath))
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
			Expect(deploy.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(4))
			Expect(deploy.Spec.Template.Spec.Volumes).To(HaveLen(1))
			Expect(services.GetOfflineContainer(*deploy).VolumeMounts).To(HaveLen(1))
			Expect(services.GetOnlineContainer(*deploy).VolumeMounts).To(HaveLen(1))
			Expect(services.GetRegistryContainer(*deploy).VolumeMounts).To(HaveLen(1))

			assertEnvFrom(*services.GetOnlineContainer(*deploy))
			assertEnvFrom(*services.GetOfflineContainer(*deploy))

			// check Feast Role
			feastRole := &rbacv1.Role{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      authz.GetFeastRoleName(convertV1ToV1Alpha1ForTests(resource)),
				Namespace: resource.Namespace,
			},
				feastRole)
			Expect(err).To(HaveOccurred())
			Expect(errors.IsNotFound(err)).To(BeTrue())

			// check RoleBinding
			roleBinding := &rbacv1.RoleBinding{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      authz.GetFeastRoleName(convertV1ToV1Alpha1ForTests(resource)),
				Namespace: resource.Namespace,
			},
				roleBinding)
			Expect(err).To(HaveOccurred())
			Expect(errors.IsNotFound(err)).To(BeTrue())

			// check ServiceAccount
			sa := &corev1.ServiceAccount{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      services.GetFeastName(feast.Handler.FeatureStore),
				Namespace: resource.Namespace,
			},
				sa)
			Expect(err).NotTo(HaveOccurred())

			By("Clearing the OIDC authorization and reconciling")
			resourceNew := resource.DeepCopy()
			resourceNew.Spec.AuthzConfig = nil
			err = k8sClient.Update(ctx, resourceNew)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource = &feastdevv1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast.Handler.FeatureStore = convertV1ToV1Alpha1ForTests(resource)

			// check no RoleBinding
			roleBinding = &rbacv1.RoleBinding{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      authz.GetFeastRoleName(convertV1ToV1Alpha1ForTests(resource)),
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

			resource := &feastdevv1.FeatureStore{}
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
			Expect(svcList.Items).To(HaveLen(4))

			cmList := corev1.ConfigMapList{}
			err = k8sClient.List(ctx, &cmList, listOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmList.Items).To(HaveLen(1))

			feast := services.FeastServices{
				Handler: handler.FeastHandler{
					Client:       controllerReconciler.Client,
					Context:      ctx,
					Scheme:       controllerReconciler.Scheme,
					FeatureStore: convertV1ToV1Alpha1ForTests(resource),
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
			env := getFeatureStoreYamlEnvVar(services.GetRegistryContainer(*deploy).Env)
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
				OfflineStore: services.OfflineStoreConfig{
					Type: services.OfflineFilePersistenceDaskConfigType,
				},
				Registry: services.RegistryConfig{
					RegistryType: services.RegistryFileConfigType,
					Path:         services.EphemeralPath + "/" + services.DefaultRegistryPath,
				},
				OnlineStore: services.OnlineStoreConfig{
					Path: services.EphemeralPath + "/" + services.DefaultOnlineStorePath,
					Type: services.OnlineSqliteConfigType,
				},
				AuthzConfig: expectedServerOidcAuthorizConfig(),
			}
			Expect(repoConfig).To(Equal(testConfig))

			// check offline
			env = getFeatureStoreYamlEnvVar(services.GetOfflineContainer(*deploy).Env)
			Expect(env).NotTo(BeNil())

			// check offline config
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfig).To(Equal(testConfig))

			// check online
			env = getFeatureStoreYamlEnvVar(services.GetOnlineContainer(*deploy).Env)
			Expect(env).NotTo(BeNil())

			// check online config
			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig = &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
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
			offlineRemote := services.OfflineStoreConfig{
				Host: fmt.Sprintf("feast-%s-offline.default.svc.cluster.local", resourceName),
				Type: services.OfflineRemoteConfigType,
				Port: services.HttpPort,
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
				Registry: services.RegistryConfig{
					RegistryType: services.RegistryRemoteConfigType,
					Path:         fmt.Sprintf("feast-%s-registry.default.svc.cluster.local:80", resourceName),
				},
				AuthzConfig: expectedClientOidcAuthorizConfig(),
			}
			Expect(repoConfigClient).To(Equal(clientConfig))
		})

		It("should fail to reconcile the resource", func() {
			By("Reconciling an invalid OIDC set of properties")
			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			newOidcSecretName := "invalid-secret"
			newTypeNamespaceSecretdName := types.NamespacedName{
				Name:      newOidcSecretName,
				Namespace: "default",
			}
			newOidcSecret := createInvalidOidcSecret(newOidcSecretName)
			err := k8sClient.Get(ctx, newTypeNamespaceSecretdName, newOidcSecret)
			if err != nil && errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, newOidcSecret)).To(Succeed())
			}

			resource := &feastdevv1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			resource.Spec.AuthzConfig.OidcAuthz.SecretRef.Name = newOidcSecretName
			err = k8sClient.Update(ctx, resource)
			Expect(err).NotTo(HaveOccurred())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())

			resource = &feastdevv1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(resource.Status.Conditions).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.ReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(feastdevv1.FailedReason))
			Expect(cond.Type).To(Equal(feastdevv1.ReadyType))
			Expect(cond.Message).To(ContainSubstring("missing OIDC"))
		})
	})
})

func expectedServerOidcAuthorizConfig() services.AuthzConfig {
	return services.AuthzConfig{
		Type: services.OidcAuthType,
		OidcParameters: map[string]interface{}{
			string(services.OidcAuthDiscoveryUrl): "auth-discovery-url",
			string(services.OidcClientId):         "client-id",
		},
	}
}
func expectedClientOidcAuthorizConfig() services.AuthzConfig {
	return services.AuthzConfig{
		Type: services.OidcAuthType,
		OidcParameters: map[string]interface{}{
			string(services.OidcClientSecret): "client-secret",
			string(services.OidcUsername):     "username",
			string(services.OidcPassword):     "password"},
	}
}

func validOidcSecretMap() map[string]string {
	return map[string]string{
		string(services.OidcClientId):         "client-id",
		string(services.OidcAuthDiscoveryUrl): "auth-discovery-url",
		string(services.OidcClientSecret):     "client-secret",
		string(services.OidcUsername):         "username",
		string(services.OidcPassword):         "password",
	}
}

func createValidOidcSecret(secretName string) *corev1.Secret {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: "default",
		},
		StringData: validOidcSecretMap(),
	}

	return secret
}

func createInvalidOidcSecret(secretName string) *corev1.Secret {
	oidcProperties := validOidcSecretMap()
	delete(oidcProperties, string(services.OidcClientId))
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: "default",
		},
		StringData: oidcProperties,
	}

	return secret
}
