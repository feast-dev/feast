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
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/authz"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

// This exercises the full reconcile loop with services.serviceAccountName set, so the
// effective ServiceAccount is verified against real objects (Deployment, namespaced
// RoleBinding, and cluster-scoped ClusterRoleBinding) rather than only unit-level helpers.
var _ = Describe("FeatureStore Controller-ServiceAccountName override", func() {
	Context("When a custom serviceAccountName is configured", func() {
		const resourceName = "service-account-override"
		var pullPolicy = corev1.PullAlways
		const overrideSAName = "irsa-worker"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1.FeatureStore{}

		BeforeEach(func() {
			createEnvFromSecretAndConfigMap()

			By("creating the custom resource for the Kind FeatureStore with an overridden ServiceAccount")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && apierrors.IsNotFound(err) {
				resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{}, withEnvFrom())
				resource.Spec.Services.ServiceAccountName = overrideSAName

				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			deleteEnvFromSecretAndConfigMap()

			By("Cleanup the specific resource instance FeatureStore")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should run pods under the override, bind RBAC to it, and never create the default ServiceAccount", func() {
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
			Expect(services.GetFeastServiceAccountName(resource)).To(Equal(overrideSAName))

			// The Deployment's pods must run under the override.
			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      services.GetFeastName(resource),
				Namespace: resource.Namespace,
			}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Template.Spec.ServiceAccountName).To(Equal(overrideSAName))

			// The operator must never create its own default ServiceAccount when an override is set.
			defaultSA := &corev1.ServiceAccount{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      services.GetFeastName(resource),
				Namespace: resource.Namespace,
			}, defaultSA)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			// The namespaced Kubernetes-authz RoleBinding subject must follow the override.
			roleBinding := &rbacv1.RoleBinding{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      authz.GetFeastRoleName(resource),
				Namespace: resource.Namespace,
			}, roleBinding)
			Expect(err).NotTo(HaveOccurred())
			Expect(roleBinding.Subjects).To(ConsistOf(rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      overrideSAName,
				Namespace: resource.Namespace,
			}))

			// The cluster-scoped ClusterRoleBinding subject must also follow the override.
			clusterRoleBinding := &rbacv1.ClusterRoleBinding{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name: authz.GetFeastClusterRoleBindingName(resource),
			}, clusterRoleBinding)
			Expect(err).NotTo(HaveOccurred())
			Expect(clusterRoleBinding.Subjects).To(ConsistOf(rbacv1.Subject{
				Kind:      "ServiceAccount",
				Name:      overrideSAName,
				Namespace: resource.Namespace,
			}))
		})
	})
})
