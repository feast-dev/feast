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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("FeatureStore Controller - Feast service LogLevel", func() {
	Context("When reconciling a FeatureStore resource", func() {
		const resourceName = "test-loglevel"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1.FeatureStore{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := &feastdevv1.FeatureStore{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: feastdevv1.FeatureStoreSpec{
						FeastProject: feastProject,
						Services: &feastdevv1.FeatureStoreServices{
							Registry: &feastdevv1.Registry{
								Local: &feastdevv1.LocalRegistryConfig{
									Server: &feastdevv1.RegistryServerConfigs{
										ServerConfigs: feastdevv1.ServerConfigs{
											LogLevel: strPtr("error"),
										},
									},
								},
							},
							OnlineStore: &feastdevv1.OnlineStore{
								Server: &feastdevv1.ServerConfigs{
									LogLevel: strPtr("debug"),
								},
							},
							OfflineStore: &feastdevv1.OfflineStore{
								Server: &feastdevv1.ServerConfigs{
									LogLevel: strPtr("info"),
								},
							},
							UI: &feastdevv1.ServerConfigs{
								LogLevel: strPtr("info"),
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

		It("should successfully reconcile the resource with logLevel", func() {
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
			Expect(resource.Status.Applied.FeastProject).To(Equal(resource.Spec.FeastProject))
			Expect(resource.Status.Applied.Services).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OfflineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.OnlineStore).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.Registry).NotTo(BeNil())

			Expect(resource.Status.Conditions).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.ReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).To(Equal(feastdevv1.DeploymentNotAvailableReason))
			Expect(cond.Type).To(Equal(feastdevv1.ReadyType))
			Expect(cond.Message).To(Equal(feastdevv1.DeploymentNotAvailableMessage))

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

			// check deployments per service
			registryDeploy := &appsv1.Deployment{}
			registryMeta := feast.GetObjectMetaType(services.RegistryFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      registryMeta.Name,
				Namespace: registryMeta.Namespace,
			}, registryDeploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(registryDeploy.Spec.Replicas).To(Equal(int32Ptr(1)))
			Expect(controllerutil.HasControllerReference(registryDeploy)).To(BeTrue())
			Expect(registryDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			command := services.GetRegistryContainer(*registryDeploy).Command
			Expect(command).To(ContainElement("--log-level"))
			Expect(command).To(ContainElement("ERROR"))

			offlineDeploy := &appsv1.Deployment{}
			offlineMeta := feast.GetObjectMetaType(services.OfflineFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      offlineMeta.Name,
				Namespace: offlineMeta.Namespace,
			}, offlineDeploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(offlineDeploy.Spec.Replicas).To(Equal(int32Ptr(1)))
			Expect(controllerutil.HasControllerReference(offlineDeploy)).To(BeTrue())
			Expect(offlineDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			command = services.GetOfflineContainer(*offlineDeploy).Command
			Expect(command).To(ContainElement("--log-level"))
			Expect(command).To(ContainElement("INFO"))

			onlineDeploy := &appsv1.Deployment{}
			onlineMeta := feast.GetObjectMetaType(services.OnlineFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      onlineMeta.Name,
				Namespace: onlineMeta.Namespace,
			}, onlineDeploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(onlineDeploy.Spec.Replicas).To(Equal(int32Ptr(1)))
			Expect(controllerutil.HasControllerReference(onlineDeploy)).To(BeTrue())
			Expect(onlineDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			command = services.GetOnlineContainer(*onlineDeploy).Command
			Expect(command).To(ContainElement("--log-level"))
			Expect(command).To(ContainElement("DEBUG"))

			uiDeploy := &appsv1.Deployment{}
			uiMeta := feast.GetObjectMetaType(services.UIFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      uiMeta.Name,
				Namespace: uiMeta.Namespace,
			}, uiDeploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(uiDeploy.Spec.Replicas).To(Equal(int32Ptr(1)))
			Expect(controllerutil.HasControllerReference(uiDeploy)).To(BeTrue())
			Expect(uiDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			command = services.GetUIContainer(*uiDeploy).Command
			Expect(command).To(ContainElement("--log-level"))
			Expect(command).To(ContainElement("INFO"))
		})

		It("should not include --log-level parameter when logLevel is not specified for any service", func() {
			By("Updating the FeatureStore resource without specifying logLevel for any service")
			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			resource.Spec.Services = &feastdevv1.FeatureStoreServices{
				Registry: &feastdevv1.Registry{
					Local: &feastdevv1.LocalRegistryConfig{
						Server: &feastdevv1.RegistryServerConfigs{
							ServerConfigs: feastdevv1.ServerConfigs{},
						},
					},
				},
				OfflineStore: &feastdevv1.OfflineStore{},
				UI:           &feastdevv1.ServerConfigs{},
			}
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())

			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			feast := services.FeastServices{
				Handler: handler.FeastHandler{
					Client:       controllerReconciler.Client,
					Context:      ctx,
					Scheme:       controllerReconciler.Scheme,
					FeatureStore: resource,
				},
			}

			// check deployments per service
			registryDeploy := &appsv1.Deployment{}
			registryMeta := feast.GetObjectMetaType(services.RegistryFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      registryMeta.Name,
				Namespace: registryMeta.Namespace,
			}, registryDeploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(registryDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			command := services.GetRegistryContainer(*registryDeploy).Command
			Expect(command).NotTo(ContainElement("--log-level"))

			onlineDeploy := &appsv1.Deployment{}
			onlineMeta := feast.GetObjectMetaType(services.OnlineFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      onlineMeta.Name,
				Namespace: onlineMeta.Namespace,
			}, onlineDeploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(onlineDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			command = services.GetOnlineContainer(*onlineDeploy).Command
			Expect(command).NotTo(ContainElement("--log-level"))

			uiDeploy := &appsv1.Deployment{}
			uiMeta := feast.GetObjectMetaType(services.UIFeastType)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      uiMeta.Name,
				Namespace: uiMeta.Namespace,
			}, uiDeploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(uiDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			command = services.GetUIContainer(*uiDeploy).Command
			Expect(command).NotTo(ContainElement("--log-level"))
		})

	})
})
