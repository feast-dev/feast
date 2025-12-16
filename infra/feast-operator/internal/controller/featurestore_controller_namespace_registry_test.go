/*
Copyright 2025 Feast Community.

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
	"encoding/json"
	"fmt"
	"time"

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

const DefaultNamespace = "default"
const FeastControllerNamespace = "feast-operator-system"

var ctx = context.Background()

var _ = Describe("FeatureStore Controller - Namespace Registry", func() {

	Context("When deploying a FeatureStore with namespace registry", func() {
		const resourceName = "namespace-registry-test"
		var pullPolicy = corev1.PullAlways
		var image = "feastdev/feast:latest"

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: DefaultNamespace,
		}
		featurestore := &feastdevv1.FeatureStore{}

		BeforeEach(func() {
			By("Ensuring manager namespace exists")
			namespace := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: FeastControllerNamespace,
				},
			}
			// Try to create, ignore if already exists
			err := k8sClient.Create(ctx, namespace)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			By("Creating a FeatureStore resource")
			featurestore = createFeatureStoreResource(resourceName, image, pullPolicy, nil, nil)
			Expect(k8sClient.Create(ctx, featurestore)).Should(Succeed())

			// Wait for the resource to be created
			Eventually(func() error {
				return k8sClient.Get(ctx, typeNamespacedName, featurestore)
			}, time.Second*10, time.Millisecond*250).Should(Succeed())
		})

		AfterEach(func() {
			By("Cleaning up the FeatureStore resource")
			// Only delete if the resource still exists
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err == nil {
				Expect(k8sClient.Delete(ctx, featurestore)).Should(Succeed())

				// Wait for the resource to be deleted
				Eventually(func() bool {
					err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
					return errors.IsNotFound(err)
				}, time.Second*10, time.Millisecond*250).Should(BeTrue())
			}
		})

		It("should create namespace registry ConfigMap", func() {
			By("Reconciling the FeatureStore")
			reconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking that namespace registry ConfigMap is created")
			Eventually(func() error {
				cm := &corev1.ConfigMap{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      services.NamespaceRegistryConfigMapName,
					Namespace: services.DefaultKubernetesNamespace, // Assuming Kubernetes environment
				}, cm)
			}, time.Second*30, time.Millisecond*500).Should(Succeed())
		})

		It("should create namespace registry Role and RoleBinding", func() {
			By("Reconciling the FeatureStore")
			reconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking that namespace registry Role is created")
			Eventually(func() error {
				role := &rbacv1.Role{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      services.NamespaceRegistryConfigMapName + "-reader",
					Namespace: services.DefaultKubernetesNamespace,
				}, role)
			}, time.Second*30, time.Millisecond*500).Should(Succeed())

			By("Checking that namespace registry RoleBinding is created")
			Eventually(func() error {
				roleBinding := &rbacv1.RoleBinding{}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      services.NamespaceRegistryConfigMapName + "-reader",
					Namespace: services.DefaultKubernetesNamespace,
				}, roleBinding)
			}, time.Second*30, time.Millisecond*500).Should(Succeed())
		})

		It("should register feature store in namespace registry", func() {
			By("Reconciling the FeatureStore")
			reconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking that feature store is registered in namespace registry")
			Eventually(func() error {
				cm := &corev1.ConfigMap{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      services.NamespaceRegistryConfigMapName,
					Namespace: services.DefaultKubernetesNamespace,
				}, cm)
				if err != nil {
					return err
				}

				// Check if the ConfigMap contains the expected data
				if cm.Data == nil || cm.Data[services.NamespaceRegistryDataKey] == "" {
					return fmt.Errorf("namespace registry data is empty")
				}

				// Parse the JSON data
				var registryData services.NamespaceRegistryData
				err = json.Unmarshal([]byte(cm.Data[services.NamespaceRegistryDataKey]), &registryData)
				if err != nil {
					return err
				}

				// Check if the feature store namespace is registered
				if registryData.Namespaces == nil {
					return fmt.Errorf("namespaces map is nil")
				}

				// The feature store should be registered in its namespace
				featureStoreNamespace := featurestore.Namespace
				if featureStoreNamespace == "" {
					featureStoreNamespace = DefaultNamespace
				}

				configs, exists := registryData.Namespaces[featureStoreNamespace]
				if !exists {
					return fmt.Errorf("feature store namespace %s not found in registry", featureStoreNamespace)
				}

				// Check if the client config is registered
				expectedConfigName := featurestore.Status.ClientConfigMap
				if expectedConfigName == "" {
					// If no client config name is set, we expect at least one config
					if len(configs) == 0 {
						return fmt.Errorf("no client configs found for namespace %s", featureStoreNamespace)
					}
				} else {
					// Check if the specific config is registered
					found := false
					for _, config := range configs {
						if config == expectedConfigName {
							found = true
							break
						}
					}
					if !found {
						return fmt.Errorf("expected client config %s not found in registry", expectedConfigName)
					}
				}

				return nil
			}, time.Second*30, time.Millisecond*500).Should(Succeed())
		})

		It("should clean up namespace registry entry when FeatureStore is deleted", func() {
			By("Reconciling the FeatureStore to create registry entry")
			reconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying feature store is registered")
			Eventually(func() error {
				cm := &corev1.ConfigMap{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      services.NamespaceRegistryConfigMapName,
					Namespace: services.DefaultKubernetesNamespace,
				}, cm)
				if err != nil {
					return err
				}

				if cm.Data == nil || cm.Data[services.NamespaceRegistryDataKey] == "" {
					return fmt.Errorf("namespace registry data is empty")
				}

				var registryData services.NamespaceRegistryData
				err = json.Unmarshal([]byte(cm.Data[services.NamespaceRegistryDataKey]), &registryData)
				if err != nil {
					return err
				}

				featureStoreNamespace := featurestore.Namespace
				if featureStoreNamespace == "" {
					featureStoreNamespace = DefaultNamespace
				}

				_, exists := registryData.Namespaces[featureStoreNamespace]
				if !exists {
					return fmt.Errorf("feature store not registered")
				}

				return nil
			}, time.Second*30, time.Millisecond*500).Should(Succeed())

			By("Deleting the FeatureStore")
			Expect(k8sClient.Delete(ctx, featurestore)).Should(Succeed())

			By("Reconciling the deletion")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying namespace registry entry is cleaned up")
			Eventually(func() error {
				cm := &corev1.ConfigMap{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      services.NamespaceRegistryConfigMapName,
					Namespace: services.DefaultKubernetesNamespace,
				}, cm)
				if err != nil {
					return err
				}

				if cm.Data == nil || cm.Data[services.NamespaceRegistryDataKey] == "" {
					// Empty registry is acceptable after cleanup
					return nil
				}

				var registryData services.NamespaceRegistryData
				err = json.Unmarshal([]byte(cm.Data[services.NamespaceRegistryDataKey]), &registryData)
				if err != nil {
					return err
				}

				featureStoreNamespace := featurestore.Namespace
				if featureStoreNamespace == "" {
					featureStoreNamespace = DefaultNamespace
				}

				// Check that the specific FeatureStore's config is removed
				configs, exists := registryData.Namespaces[featureStoreNamespace]
				if exists {
					expectedClientConfigName := "feast-" + featurestore.Name + "-client"
					for _, config := range configs {
						if config == expectedClientConfigName {
							return fmt.Errorf("feature store config %s still exists after deletion", expectedClientConfigName)
						}
					}
				}

				return nil
			}, time.Second*30, time.Millisecond*500).Should(Succeed())
		})
	})

	Context("When testing namespace registry data operations", func() {
		It("should correctly serialize and deserialize namespace registry data", func() {
			By("Creating test data")
			originalData := &services.NamespaceRegistryData{
				Namespaces: map[string][]string{
					"test-namespace-1": {"client-config-1", "client-config-2"},
					"test-namespace-2": {"client-config-3"},
				},
			}

			By("Marshaling to JSON")
			jsonData, err := json.Marshal(originalData)
			Expect(err).NotTo(HaveOccurred())

			By("Unmarshaling back")
			var unmarshaledData services.NamespaceRegistryData
			err = json.Unmarshal(jsonData, &unmarshaledData)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying data integrity")
			Expect(unmarshaledData.Namespaces).To(Equal(originalData.Namespaces))
		})

		It("should handle empty namespace registry data", func() {
			By("Creating empty data")
			originalData := &services.NamespaceRegistryData{
				Namespaces: make(map[string][]string),
			}

			By("Marshaling to JSON")
			jsonData, err := json.Marshal(originalData)
			Expect(err).NotTo(HaveOccurred())

			By("Unmarshaling back")
			var unmarshaledData services.NamespaceRegistryData
			err = json.Unmarshal(jsonData, &unmarshaledData)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying empty data")
			Expect(unmarshaledData.Namespaces).To(Equal(originalData.Namespaces))
			Expect(unmarshaledData.Namespaces).To(BeEmpty())
		})

		It("should correctly remove entries from namespace registry data", func() {
			By("Creating test data with multiple entries")
			originalData := &services.NamespaceRegistryData{
				Namespaces: map[string][]string{
					"namespace-1": {"config-1", "config-2", "config-3"},
					"namespace-2": {"config-4"},
				},
			}

			By("Marshaling to JSON")
			jsonData, err := json.Marshal(originalData)
			Expect(err).NotTo(HaveOccurred())

			By("Unmarshaling back")
			var data services.NamespaceRegistryData
			err = json.Unmarshal(jsonData, &data)
			Expect(err).NotTo(HaveOccurred())

			By("Simulating removal of specific config")
			namespace := "namespace-1"
			configToRemove := "config-2"

			if configs, exists := data.Namespaces[namespace]; exists {
				var updatedConfigs []string
				for _, config := range configs {
					if config != configToRemove {
						updatedConfigs = append(updatedConfigs, config)
					}
				}
				data.Namespaces[namespace] = updatedConfigs
			}

			By("Verifying removal worked")
			expectedConfigs := []string{"config-1", "config-3"}
			Expect(data.Namespaces[namespace]).To(Equal(expectedConfigs))

			By("Verifying other namespace is unchanged")
			Expect(data.Namespaces["namespace-2"]).To(Equal([]string{"config-4"}))
		})

		It("should remove entire namespace when last config is removed", func() {
			By("Creating test data with single config per namespace")
			originalData := &services.NamespaceRegistryData{
				Namespaces: map[string][]string{
					"namespace-1": {"config-1"},
					"namespace-2": {"config-2"},
				},
			}

			By("Marshaling to JSON")
			jsonData, err := json.Marshal(originalData)
			Expect(err).NotTo(HaveOccurred())

			By("Unmarshaling back")
			var data services.NamespaceRegistryData
			err = json.Unmarshal(jsonData, &data)
			Expect(err).NotTo(HaveOccurred())

			By("Simulating removal of the only config from namespace-1")
			namespace := "namespace-1"
			configToRemove := "config-1"

			if configs, exists := data.Namespaces[namespace]; exists {
				var updatedConfigs []string
				for _, config := range configs {
					if config != configToRemove {
						updatedConfigs = append(updatedConfigs, config)
					}
				}

				// If no configs left, remove the namespace entry
				if len(updatedConfigs) == 0 {
					delete(data.Namespaces, namespace)
				} else {
					data.Namespaces[namespace] = updatedConfigs
				}
			}

			By("Verifying namespace was removed")
			_, exists := data.Namespaces[namespace]
			Expect(exists).To(BeFalse())

			By("Verifying other namespace is unchanged")
			Expect(data.Namespaces["namespace-2"]).To(Equal([]string{"config-2"}))

			By("Verifying total namespace count")
			Expect(data.Namespaces).To(HaveLen(1))
		})
	})
})
