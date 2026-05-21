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
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("FeatureStore Controller-KubeRay RBAC", func() {
	const (
		batchConfigMapName = "ray-batch-engine"
	)
	var pullPolicy = corev1.PullAlways
	ctx := context.Background()

	createBatchEngineConfigMap := func(yamlBody string) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: batchConfigMapName, Namespace: "default"},
			Data:       map[string]string{"config": yamlBody},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
	}

	deleteBatchEngineConfigMap := func() {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: batchConfigMapName, Namespace: "default"},
		}
		_ = k8sClient.Delete(ctx, cm)
	}

	reconcileOnce := func(name string) {
		controllerReconciler := &FeatureStoreReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	rbacKey := func(fsName string) types.NamespacedName {
		return types.NamespacedName{
			Name:      services.GetFeastName(&feastdevv1.FeatureStore{ObjectMeta: metav1.ObjectMeta{Name: fsName}}) + "-kuberay",
			Namespace: "default",
		}
	}

	Context("when batchEngine has type: ray.engine and use_kuberay: true", func() {
		const resourceName = "kuberay-rbac-enabled"

		BeforeEach(func() {
			createBatchEngineConfigMap("type: ray.engine\nuse_kuberay: true\ncluster_name: my-cluster\n")
			resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{}, nil)
			resource.Spec.BatchEngine = &feastdevv1.BatchEngineConfig{
				ConfigMapRef: &corev1.LocalObjectReference{Name: batchConfigMapName},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &feastdevv1.FeatureStore{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: "default"}, resource); err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
			deleteBatchEngineConfigMap()
		})

		It("creates a Role with KubeRay rules and a RoleBinding to the Feast SA", func() {
			reconcileOnce(resourceName)

			role := &rbacv1.Role{}
			Expect(k8sClient.Get(ctx, rbacKey(resourceName), role)).To(Succeed())
			Expect(role.Rules).To(ConsistOf(
				rbacv1.PolicyRule{
					APIGroups: []string{"ray.io"},
					Resources: []string{"rayclusters"},
					Verbs:     []string{"get", "list", "watch"},
				},
				rbacv1.PolicyRule{
					APIGroups: []string{""},
					Resources: []string{"secrets"},
					Verbs:     []string{"get", "list", "watch", "create", "update", "delete"},
				},
			))

			fs := &feastdevv1.FeatureStore{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: "default"}, fs)).To(Succeed())
			Expect(role.OwnerReferences).To(HaveLen(1))
			Expect(role.OwnerReferences[0].UID).To(Equal(fs.UID))

			binding := &rbacv1.RoleBinding{}
			Expect(k8sClient.Get(ctx, rbacKey(resourceName), binding)).To(Succeed())
			Expect(binding.RoleRef).To(Equal(rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     rbacKey(resourceName).Name,
			}))
			Expect(binding.Subjects).To(ConsistOf(rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      services.GetFeastName(fs),
				Namespace: "default",
			}))
			Expect(binding.OwnerReferences).To(HaveLen(1))
			Expect(binding.OwnerReferences[0].UID).To(Equal(fs.UID))
		})
	})

	Context("when batchEngine ConfigMap changes from use_kuberay: true to false", func() {
		const resourceName = "kuberay-rbac-disable"

		BeforeEach(func() {
			createBatchEngineConfigMap("type: ray.engine\nuse_kuberay: true\n")
			resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{}, nil)
			resource.Spec.BatchEngine = &feastdevv1.BatchEngineConfig{
				ConfigMapRef: &corev1.LocalObjectReference{Name: batchConfigMapName},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &feastdevv1.FeatureStore{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: "default"}, resource); err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
			deleteBatchEngineConfigMap()
		})

		It("deletes the Role and RoleBinding on the next reconcile", func() {
			reconcileOnce(resourceName)
			Expect(k8sClient.Get(ctx, rbacKey(resourceName), &rbacv1.Role{})).To(Succeed())

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: batchConfigMapName, Namespace: "default"}, cm)).To(Succeed())
			cm.Data["config"] = "type: ray.engine\nuse_kuberay: false\n"
			Expect(k8sClient.Update(ctx, cm)).To(Succeed())

			reconcileOnce(resourceName)

			err := k8sClient.Get(ctx, rbacKey(resourceName), &rbacv1.Role{})
			Expect(errors.IsNotFound(err)).To(BeTrue(), "expected Role to be deleted, got %v", err)
			err = k8sClient.Get(ctx, rbacKey(resourceName), &rbacv1.RoleBinding{})
			Expect(errors.IsNotFound(err)).To(BeTrue(), "expected RoleBinding to be deleted, got %v", err)
		})
	})

	Context("when no batchEngine is configured", func() {
		const resourceName = "kuberay-rbac-absent"

		BeforeEach(func() {
			resource := createFeatureStoreResource(resourceName, image, pullPolicy, &[]corev1.EnvVar{}, nil)
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &feastdevv1.FeatureStore{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: "default"}, resource); err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("does not create the KubeRay Role or RoleBinding", func() {
			reconcileOnce(resourceName)

			err := k8sClient.Get(ctx, rbacKey(resourceName), &rbacv1.Role{})
			Expect(errors.IsNotFound(err)).To(BeTrue(), "expected no Role, got %v", err)
			err = k8sClient.Get(ctx, rbacKey(resourceName), &rbacv1.RoleBinding{})
			Expect(errors.IsNotFound(err)).To(BeTrue(), "expected no RoleBinding, got %v", err)
		})
	})
})
