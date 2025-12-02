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

	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
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

var cassandraYamlString = `
hosts:
  - 192.168.1.1
  - 192.168.1.2
  - 192.168.1.3
keyspace: KeyspaceName
port: 9042                                                              
username: user                                                          
password: secret                                                        
protocol_version: 5                                                     
load_balancing:                                                         
  local_dc: datacenter1                                             
  load_balancing_policy: TokenAwarePolicy(DCAwareRoundRobinPolicy)
read_concurrency: 100                                                   
write_concurrency: 100
`

var snowflakeYamlString = `
account: snowflake_deployment.us-east-1
user: user_login
password: user_password
role: SYSADMIN
warehouse: COMPUTE_WH
database: FEAST
schema: PUBLIC
`

var sqlTypeYamlString = `
path: postgresql://postgres:mysecretpassword@127.0.0.1:55001/feast
cache_ttl_seconds: 60
sqlalchemy_config_kwargs:
  echo: false
  pool_pre_ping: true
`

var secretContainingValidTypeYamlString = `
type: cassandra
hosts:
  - 192.168.1.1
  - 192.168.1.2
  - 192.168.1.3
keyspace: KeyspaceName
port: 9042                                                              
username: user                                                          
password: secret                                                        
protocol_version: 5                                                     
load_balancing:                                                         
  local_dc: datacenter1                                             
  load_balancing_policy: TokenAwarePolicy(DCAwareRoundRobinPolicy)
read_concurrency: 100                                                   
write_concurrency: 100
`

var invalidSecretTypeYamlString = `
type: wrong
hosts:
  - 192.168.1.1
  - 192.168.1.2
  - 192.168.1.3
keyspace: KeyspaceName
port: 9042                                                              
username: user                                                          
password: secret                                                        
protocol_version: 5                                                     
load_balancing:                                                         
  local_dc: datacenter1                                             
  load_balancing_policy: TokenAwarePolicy(DCAwareRoundRobinPolicy)
read_concurrency: 100                                                   
write_concurrency: 100
`

var invalidSecretRegistryTypeYamlString = `
registry_type: sql
path: postgresql://postgres:mysecretpassword@127.0.0.1:55001/feast
cache_ttl_seconds: 60
sqlalchemy_config_kwargs:
  echo: false
  pool_pre_ping: true
`

var _ = Describe("FeatureStore Controller - db storage services", func() {
	Context("When deploying a resource with all db storage services", func() {
		const resourceName = "cr-name"
		var pullPolicy = corev1.PullAlways

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		offlineSecretNamespacedName := types.NamespacedName{
			Name:      "offline-store-secret",
			Namespace: "default",
		}

		onlineSecretNamespacedName := types.NamespacedName{
			Name:      "online-store-secret",
			Namespace: "default",
		}

		registrySecretNamespacedName := types.NamespacedName{
			Name:      "registry-store-secret",
			Namespace: "default",
		}

		featurestore := &feastdevv1alpha1.FeatureStore{}
		offlineType := services.OfflineDBPersistenceSnowflakeConfigType
		onlineType := services.OnlineDBPersistenceCassandraConfigType
		registryType := services.RegistryDBPersistenceSQLConfigType

		BeforeEach(func() {
			By("creating secrets for db stores for custom resource of Kind FeatureStore")
			secret := &corev1.Secret{}

			secretData := map[string][]byte{
				string(offlineType): []byte(snowflakeYamlString),
			}
			err := k8sClient.Get(ctx, offlineSecretNamespacedName, secret)
			if err != nil && errors.IsNotFound(err) {
				secret.ObjectMeta = metav1.ObjectMeta{
					Name:      offlineSecretNamespacedName.Name,
					Namespace: offlineSecretNamespacedName.Namespace,
				}
				secret.Data = secretData
				Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			}

			secret = &corev1.Secret{}

			secretData = map[string][]byte{
				string(onlineType): []byte(cassandraYamlString),
			}
			err = k8sClient.Get(ctx, onlineSecretNamespacedName, secret)
			if err != nil && errors.IsNotFound(err) {
				secret.ObjectMeta = metav1.ObjectMeta{
					Name:      onlineSecretNamespacedName.Name,
					Namespace: onlineSecretNamespacedName.Namespace,
				}
				secret.Data = secretData
				Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			}

			secret = &corev1.Secret{}

			secretData = map[string][]byte{
				"sql_custom_registry_key": []byte(sqlTypeYamlString),
			}
			err = k8sClient.Get(ctx, registrySecretNamespacedName, secret)
			if err != nil && errors.IsNotFound(err) {
				secret.ObjectMeta = metav1.ObjectMeta{
					Name:      registrySecretNamespacedName.Name,
					Namespace: registrySecretNamespacedName.Namespace,
				}
				secret.Data = secretData
				Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			}

			createEnvFromSecretAndConfigMap()

			By("creating the custom resource for the Kind FeatureStore")
			err = k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{}, withEnvFrom())
				resource.Spec.Services.OfflineStore.Persistence = &feastdevv1alpha1.OfflineStorePersistence{
					DBPersistence: &feastdevv1alpha1.OfflineStoreDBStorePersistence{
						Type: string(offlineType),
						SecretRef: corev1.LocalObjectReference{
							Name: "offline-store-secret",
						},
					},
				}
				resource.Spec.Services.OnlineStore.Persistence = &feastdevv1alpha1.OnlineStorePersistence{
					DBPersistence: &feastdevv1alpha1.OnlineStoreDBStorePersistence{
						Type: string(onlineType),
						SecretRef: corev1.LocalObjectReference{
							Name: "online-store-secret",
						},
					},
				}
				resource.Spec.Services.Registry.Local.Persistence = &feastdevv1alpha1.RegistryPersistence{
					DBPersistence: &feastdevv1alpha1.RegistryDBStorePersistence{
						Type: string(registryType),
						SecretRef: corev1.LocalObjectReference{
							Name: "registry-store-secret",
						},
						SecretKeyName: "sql_custom_registry_key",
					},
				}

				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})
		AfterEach(func() {
			onlineSecret := &corev1.Secret{}
			err := k8sClient.Get(ctx, onlineSecretNamespacedName, onlineSecret)
			Expect(err).NotTo(HaveOccurred())

			offlineSecret := &corev1.Secret{}
			err = k8sClient.Get(ctx, offlineSecretNamespacedName, offlineSecret)
			Expect(err).NotTo(HaveOccurred())

			registrySecret := &corev1.Secret{}
			err = k8sClient.Get(ctx, registrySecretNamespacedName, registrySecret)
			Expect(err).NotTo(HaveOccurred())

			resource := &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			deleteEnvFromSecretAndConfigMap()

			By("Cleanup the secrets")
			Expect(k8sClient.Delete(ctx, onlineSecret)).To(Succeed())
			Expect(k8sClient.Delete(ctx, offlineSecret)).To(Succeed())
			Expect(k8sClient.Delete(ctx, registrySecret)).To(Succeed())

			By("Cleanup the specific resource instance FeatureStore")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should fail reconciling the resource", func() {
			By("Referring to a secret that doesn't exist")
			resource := &feastdevv1alpha1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			resource.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretRef = corev1.LocalObjectReference{Name: "invalid_secret"}
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())

			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(Equal("secrets \"invalid_secret\" not found"))

			By("Referring to a secret with a key that doesn't exist")
			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			resource.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretRef = corev1.LocalObjectReference{Name: "online-store-secret"}
			resource.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretKeyName = "invalid.secret.key"
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())
			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(Equal("secret key invalid.secret.key doesn't exist in secret online-store-secret"))

			By("Referring to a secret that contains parameter named type with invalid value")
			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			err = k8sClient.Get(ctx, onlineSecretNamespacedName, secret)
			Expect(err).NotTo(HaveOccurred())
			secret.Data[string(services.OnlineDBPersistenceCassandraConfigType)] = []byte(invalidSecretTypeYamlString)
			Expect(k8sClient.Update(ctx, secret)).To(Succeed())

			resource.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretRef = corev1.LocalObjectReference{Name: "online-store-secret"}
			resource.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretKeyName = ""
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())
			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(Equal("secret key cassandra in secret online-store-secret contains tag named type with value wrong"))
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
			Expect(resource.Status.Applied.Services).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.DBPersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.DBPersistence.Type).To(Equal(string(offlineType)))
			Expect(resource.Status.Applied.Services.OfflineStore.Persistence.DBPersistence.SecretRef).To(Equal(corev1.LocalObjectReference{Name: "offline-store-secret"}))
			Expect(resource.Status.Applied.Services.OfflineStore.Server.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Server.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore.Server.Image).To(Equal(&services.DefaultImage))
			Expect(resource.Status.Applied.Services.OnlineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.DBPersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.DBPersistence.Type).To(Equal(string(onlineType)))
			Expect(resource.Status.Applied.Services.OnlineStore.Persistence.DBPersistence.SecretRef).To(Equal(corev1.LocalObjectReference{Name: "online-store-secret"}))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.ImagePullPolicy).To(Equal(&pullPolicy))
			Expect(resource.Status.Applied.Services.OnlineStore.Server.Resources).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore.Server.Image).To(Equal(&image))
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.DBPersistence).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.DBPersistence.Type).To(Equal(string(registryType)))
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.DBPersistence.SecretRef).To(Equal(corev1.LocalObjectReference{Name: "registry-store-secret"}))
			Expect(resource.Status.Applied.Services.Registry.Local.Persistence.DBPersistence.SecretKeyName).To(Equal("sql_custom_registry_key"))
			Expect(resource.Status.Applied.Services.Registry.Local.Server.ImagePullPolicy).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Server.Resources).To(BeNil())
			Expect(resource.Status.Applied.Services.Registry.Local.Server.Image).To(Equal(&services.DefaultImage))

			Expect(resource.Status.ServiceHostnames.OfflineStore).To(Equal(feast.GetFeastServiceName(services.OfflineFeastType) + "." + resource.Namespace + domain))
			Expect(resource.Status.ServiceHostnames.OnlineStore).To(Equal(feast.GetFeastServiceName(services.OnlineFeastType) + "." + resource.Namespace + domain))
			Expect(resource.Status.ServiceHostnames.Registry).To(Equal(feast.GetFeastServiceName(services.RegistryFeastType) + "." + resource.Namespace + domain))

			Expect(resource.Status.Conditions).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.ReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.DeploymentNotAvailableReason))
			Expect(cond.Type).To(Equal(feastdevv1alpha1.ReadyType))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.DeploymentNotAvailableMessage))

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

			Expect(resource.Status.Phase).To(Equal(feastdevv1alpha1.PendingPhase))

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
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(4))
			svc := &corev1.Service{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.RegistryFeastType),
				Namespace: resource.Namespace,
			},
				svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(svc)).To(BeTrue())
			Expect(svc.Spec.Ports[0].TargetPort).To(Equal(intstr.FromInt(int(services.FeastServiceConstants[services.RegistryFeastType].TargetHttpPort))))

			By("Referring to a secret that contains parameter named type")
			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			err = k8sClient.Get(ctx, onlineSecretNamespacedName, secret)
			Expect(err).NotTo(HaveOccurred())
			secret.Data[string(services.OnlineDBPersistenceCassandraConfigType)] = []byte(secretContainingValidTypeYamlString)
			Expect(k8sClient.Update(ctx, secret)).To(Succeed())

			resource.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretRef = corev1.LocalObjectReference{Name: "online-store-secret"}
			resource.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretKeyName = ""
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())
			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})

			Expect(err).To(Not(HaveOccurred()))

			By("Referring to a secret that contains parameter named registry_type")
			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			secret = &corev1.Secret{}
			err = k8sClient.Get(ctx, onlineSecretNamespacedName, secret)
			Expect(err).NotTo(HaveOccurred())
			secret.Data[string(services.OnlineDBPersistenceCassandraConfigType)] = []byte(cassandraYamlString)
			Expect(k8sClient.Update(ctx, secret)).To(Succeed())

			secret = &corev1.Secret{}
			err = k8sClient.Get(ctx, registrySecretNamespacedName, secret)
			Expect(err).NotTo(HaveOccurred())
			secret.Data["sql_custom_registry_key"] = nil
			secret.Data[string(services.RegistryDBPersistenceSQLConfigType)] = []byte(invalidSecretRegistryTypeYamlString)
			Expect(k8sClient.Update(ctx, secret)).To(Succeed())

			resource.Spec.Services.Registry.Local.Persistence.DBPersistence.SecretRef = corev1.LocalObjectReference{Name: "registry-store-secret"}
			resource.Spec.Services.Registry.Local.Persistence.DBPersistence.SecretKeyName = ""
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())
			resource = &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(Not(HaveOccurred()))
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
			registryContainer := services.GetRegistryContainer(*deploy)
			Expect(registryContainer.Env).To(HaveLen(1))
			env := getFeatureStoreYamlEnvVar(registryContainer.Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err := feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err := base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfig := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfig)
			Expect(err).NotTo(HaveOccurred())
			dbParametersMap := unmarshallYamlString(sqlTypeYamlString)
			copyMap := services.CopyMap(dbParametersMap)
			delete(dbParametersMap, "path")
			// Expect cache_ttl_seconds to be mapped into Registry.CacheTTLSeconds
			ttlVal, ok := copyMap["cache_ttl_seconds"].(int)
			Expect(ok).To(BeTrue())
			ttl := int32(ttlVal)
			delete(dbParametersMap, "cache_ttl_seconds")
			testConfig := &services.RepoConfig{
				Project:                       feastProject,
				Provider:                      services.LocalProviderType,
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
				OfflineStore: services.OfflineStoreConfig{
					Type:         services.OfflineDBPersistenceSnowflakeConfigType,
					DBParameters: unmarshallYamlString(snowflakeYamlString),
				},
				Registry: services.RegistryConfig{
					Path:            copyMap["path"].(string),
					RegistryType:    services.RegistryDBPersistenceSQLConfigType,
					CacheTTLSeconds: &ttl,
					DBParameters:    dbParametersMap,
				},
				OnlineStore: services.OnlineStoreConfig{
					Type:         onlineType,
					DBParameters: unmarshallYamlString(cassandraYamlString),
				},
				AuthzConfig: noAuthzConfig(),
			}
			Expect(repoConfig).To(Equal(testConfig))

			offlineContainer := services.GetOfflineContainer(*deploy)
			Expect(offlineContainer.Env).To(HaveLen(1))
			assertEnvFrom(*offlineContainer)
			env = getFeatureStoreYamlEnvVar(offlineContainer.Env)
			Expect(env).NotTo(BeNil())

			fsYamlStr, err = feast.GetServiceFeatureStoreYamlBase64()
			Expect(err).NotTo(HaveOccurred())
			Expect(fsYamlStr).To(Equal(env.Value))

			envByte, err = base64.StdEncoding.DecodeString(env.Value)
			Expect(err).NotTo(HaveOccurred())
			repoConfigOffline := &services.RepoConfig{}
			err = yaml.Unmarshal(envByte, repoConfigOffline)
			Expect(err).NotTo(HaveOccurred())
			Expect(repoConfigOffline).To(Equal(testConfig))

			onlineContainer := services.GetOnlineContainer(*deploy)
			Expect(onlineContainer.VolumeMounts).To(HaveLen(1))
			Expect(onlineContainer.Env).To(HaveLen(1))
			assertEnvFrom(*onlineContainer)
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
			onlineContainer = services.GetOnlineContainer(*deploy)
			Expect(onlineContainer.Env).To(HaveLen(1))

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
				EntityKeySerializationVersion: feastdevv1alpha1.SerializationVersion,
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
			newOnlineSecretName := "offline-store-secret"
			newOnlineDBPersistenceType := services.OnlineDBPersistenceSnowflakeConfigType
			resourceNew.Spec.Services.OnlineStore.Persistence.DBPersistence.Type = string(newOnlineDBPersistenceType)
			resourceNew.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretRef = corev1.LocalObjectReference{Name: newOnlineSecretName}
			resourceNew.Spec.Services.OnlineStore.Persistence.DBPersistence.SecretKeyName = string(services.OfflineDBPersistenceSnowflakeConfigType)
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

			// check online config
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			onlineContainer = services.GetOnlineContainer(*deploy)
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
			testConfig.OnlineStore.Type = services.OnlineDBPersistenceSnowflakeConfigType
			testConfig.OnlineStore.DBParameters = unmarshallYamlString(snowflakeYamlString)
			Expect(repoConfigOnline).To(Equal(testConfig))
		})
	})
})

func unmarshallYamlString(yamlString string) map[string]interface{} {
	var parameters map[string]interface{}

	err := yaml.Unmarshal([]byte(yamlString), &parameters)
	if err != nil {
		fmt.Println(err)
	}
	return parameters
}
