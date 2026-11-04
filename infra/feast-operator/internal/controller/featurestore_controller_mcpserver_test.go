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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("FeatureStore Controller - standalone MCP server", func() {
	Context("When reconciling a FeatureStore with spec.services.mcpServer", func() {
		const resourceName = "test-mcpserver"
		const configMapName = "test-mcpserver-config"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1.FeatureStore{}

		newFeast := func(resource *feastdevv1.FeatureStore, reconciler *FeatureStoreReconciler) services.FeastServices {
			return services.FeastServices{
				Handler: handler.FeastHandler{
					Client:       reconciler.Client,
					Context:      ctx,
					Scheme:       reconciler.Scheme,
					FeatureStore: resource,
				},
			}
		}

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
							OnlineStore: &feastdevv1.OnlineStore{
								Server: &feastdevv1.ServerConfigs{},
							},
							McpServer: &feastdevv1.McpServerConfig{
								Config: &feastdevv1.McpServerConfigSource{
									ConfigMapRef: corev1.LocalObjectReference{
										Name: configMapName,
									},
								},
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

		It("should deploy an mcpserver container, Service, and Ready condition", func() {
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
			feast := newFeast(resource, controllerReconciler)

			By("defaulting the MCP server image to the feature-server image")
			Expect(resource.Status.Applied.Services.McpServer).NotTo(BeNil())
			Expect(resource.Status.Applied.Services.McpServer.Image).NotTo(BeNil())

			By("verifying the mcpserver container command")
			deploy := &appsv1.Deployment{}
			objMeta := feast.GetObjectMeta()
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())

			mcpContainer := services.GetMcpServerContainer(*deploy)
			Expect(mcpContainer).NotTo(BeNil())
			Expect(mcpContainer.Command).To(Equal([]string{
				"feast", "mcp",
				"--host", "0.0.0.0",
				"--port", "8100",
				"--config", "/etc/feast/mcp/feast_mcp.yaml",
			}))

			By("mounting the config ConfigMap only into the mcpserver container")
			var mcpVolumeMounted bool
			for _, vm := range mcpContainer.VolumeMounts {
				if vm.MountPath == "/etc/feast/mcp" {
					mcpVolumeMounted = true
				}
			}
			Expect(mcpVolumeMounted).To(BeTrue())

			onlineContainer := services.GetOnlineContainer(*deploy)
			Expect(onlineContainer).NotTo(BeNil())
			for _, vm := range onlineContainer.VolumeMounts {
				Expect(vm.MountPath).NotTo(Equal("/etc/feast/mcp"))
			}

			By("creating the MCP server Service on the HTTP target port")
			svc := &corev1.Service{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.McpServerFeastType),
				Namespace: resource.Namespace,
			}, svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(svc)).To(BeTrue())
			Expect(svc.Spec.Ports[0].TargetPort).To(Equal(
				intstr.FromInt(int(services.FeastServiceConstants[services.McpServerFeastType].TargetHttpPort))))

			By("publishing the MCP server hostname and Ready condition")
			Expect(resource.Status.ServiceHostnames.McpServer).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1.McpServerReadyType)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1.ReadyReason))
		})

		It("should remove the mcpserver container and Service when mcpServer is unset", func() {
			By("Reconciling with mcpServer configured")
			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Removing spec.services.mcpServer")
			resource := &feastdevv1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			resource.Spec.Services.McpServer = nil
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource = &feastdevv1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast := newFeast(resource, controllerReconciler)

			By("dropping the mcpserver container from the deployment")
			deploy := &appsv1.Deployment{}
			objMeta := feast.GetObjectMeta()
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(services.GetMcpServerContainer(*deploy)).To(BeNil())

			By("deleting the MCP server Service")
			svc := &corev1.Service{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.GetFeastServiceName(services.McpServerFeastType),
				Namespace: resource.Namespace,
			}, svc)
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})
	})
})
