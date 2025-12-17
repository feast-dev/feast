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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("NotebookConfigMap Controller", func() {
	const (
		notebookName        = "test-notebook"
		namespace           = "default"
		projectName         = "test-project"
		featureStoreName    = "test-featurestore"
		clientConfigMapName = "test-featurestore-client"
	)

	var (
		ctx             context.Context
		reconciler      *NotebookConfigMapReconciler
		notebookGVK     schema.GroupVersionKind
		testYAMLContent string
	)

	BeforeEach(func() {
		ctx = context.Background()
		notebookGVK = schema.GroupVersionKind{
			Group:   "kubeflow.org",
			Version: "v1",
			Kind:    "Notebook",
		}

		// Create Notebook CRD using apiextensions client
		cfg := testEnv.Config
		apiextensionsClientset, err := apiextensionsclient.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())

		crd := &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name: "notebooks.kubeflow.org",
			},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: "kubeflow.org",
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name:    "v1",
						Served:  true,
						Storage: true,
						Schema: &apiextensionsv1.CustomResourceValidation{
							OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"spec": {
										Type: "object",
									},
									"status": {
										Type: "object",
									},
								},
							},
						},
					},
				},
				Scope: apiextensionsv1.NamespaceScoped,
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Plural:   "notebooks",
					Singular: "notebook",
					Kind:     "Notebook",
				},
			},
		}

		_, err = apiextensionsClientset.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		// Create and delete a dummy notebook to force the REST mapper to discover the CRD
		dummyNotebook := &unstructured.Unstructured{}
		dummyNotebook.SetGroupVersionKind(notebookGVK)
		dummyNotebook.SetName("dummy-notebook-for-crd-discovery")
		dummyNotebook.SetNamespace(namespace)
		err = k8sClient.Create(ctx, dummyNotebook)
		if err == nil {
			_ = k8sClient.Delete(ctx, dummyNotebook)
		}

		reconciler = &NotebookConfigMapReconciler{
			Client:      k8sClient,
			Scheme:      k8sClient.Scheme(),
			NotebookGVK: notebookGVK,
		}
		testYAMLContent = "project: test-project\nprovider: local"
	})

	AfterEach(func() {
		// Clean up any test notebooks
		notebookList := &unstructured.UnstructuredList{}
		notebookList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   notebookGVK.Group,
			Version: notebookGVK.Version,
			Kind:    notebookGVK.Kind + "List",
		})
		_ = k8sClient.List(ctx, notebookList, client.InNamespace(namespace))
		for i := range notebookList.Items {
			_ = k8sClient.Delete(ctx, &notebookList.Items[i])
		}

		// Clean up all configmaps in the namespace (test configmaps)
		configMapList := &corev1.ConfigMapList{}
		_ = k8sClient.List(ctx, configMapList, client.InNamespace(namespace))
		for i := range configMapList.Items {
			cm := &configMapList.Items[i]
			// Delete all test-related configmaps
			_ = k8sClient.Delete(ctx, cm)
		}

		// Clean up feature stores
		fsList := &feastdevv1.FeatureStoreList{}
		_ = k8sClient.List(ctx, fsList, client.InNamespace(namespace))
		for i := range fsList.Items {
			_ = k8sClient.Delete(ctx, &fsList.Items[i])
		}
	})

	createUnstructuredNotebook := func(name, ns string, labels, annotations map[string]string) *unstructured.Unstructured {
		notebook := &unstructured.Unstructured{}
		notebook.SetGroupVersionKind(notebookGVK)
		notebook.SetName(name)
		notebook.SetNamespace(ns)
		if labels != nil {
			notebook.SetLabels(labels)
		}
		if annotations != nil {
			notebook.SetAnnotations(annotations)
		}
		return notebook
	}

	createFeatureStore := func(name, ns, project string) *feastdevv1.FeatureStore {
		fs := &feastdevv1.FeatureStore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
			},
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: project,
			},
		}
		return fs
	}

	// Helper to set FeatureStore status after creation
	setFeatureStoreStatus := func(fs *feastdevv1.FeatureStore, project string, configMapName string) {
		fs.Status.ClientConfigMap = configMapName
		fs.Status.Applied.FeastProject = project
	}

	createClientConfigMap := func(name, ns string) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
			},
			Data: map[string]string{
				services.FeatureStoreYamlCmKey: testYAMLContent,
			},
		}
	}

	Context("Reconcile", func() {
		It("should return nil when Notebook is not found", func() {
			// Create and delete a notebook first to force the REST mapper to discover the CRD
			// Use Eventually to wait for CRD to be recognized
			tempNotebook := createUnstructuredNotebook("temp-notebook", namespace, nil, nil)
			Eventually(func() error {
				return k8sClient.Create(ctx, tempNotebook)
			}, "10s", "1s").Should(Succeed())
			Expect(k8sClient.Delete(ctx, tempNotebook)).To(Succeed())

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "non-existent",
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("should return nil when Notebook is being deleted", func() {
			notebook := createUnstructuredNotebook(notebookName, namespace, map[string]string{
				NotebookFeastIntegrationLabel: TrueValue,
			}, map[string]string{
				NotebookFeastConfigAnnotation: projectName,
			})
			now := metav1.Now()
			notebook.SetDeletionTimestamp(&now)
			Expect(k8sClient.Create(ctx, notebook)).To(Succeed())

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      notebookName,
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			Expect(k8sClient.Delete(ctx, notebook)).To(Succeed())
		})

		It("should cleanup ConfigMap when integration label is not set", func() {
			notebook := createUnstructuredNotebook(notebookName, namespace, nil, nil)
			Expect(k8sClient.Create(ctx, notebook)).To(Succeed())

			configMap := createClientConfigMap(notebookName+NotebookConfigMapNameSuffix, namespace)
			Expect(k8sClient.Create(ctx, configMap)).To(Succeed())

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      notebookName,
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: configMap.Name, Namespace: namespace}, cm)
			Expect(err).To(HaveOccurred())

			Expect(k8sClient.Delete(ctx, notebook)).To(Succeed())
		})

		It("should cleanup ConfigMap when integration label is not 'true'", func() {
			notebook := createUnstructuredNotebook(notebookName, namespace, map[string]string{
				NotebookFeastIntegrationLabel: "false",
			}, nil)
			Expect(k8sClient.Create(ctx, notebook)).To(Succeed())

			configMap := createClientConfigMap(notebookName+NotebookConfigMapNameSuffix, namespace)
			Expect(k8sClient.Create(ctx, configMap)).To(Succeed())

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      notebookName,
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: configMap.Name, Namespace: namespace}, cm)
			Expect(err).To(HaveOccurred())

			Expect(k8sClient.Delete(ctx, notebook)).To(Succeed())
		})

		It("should create ConfigMap when integration is enabled but annotation is empty", func() {
			fs := createFeatureStore(featureStoreName, namespace, projectName)
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			// Set and update status separately
			setFeatureStoreStatus(fs, projectName, clientConfigMapName)
			Expect(k8sClient.Status().Update(ctx, fs)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, fs)
			}()

			clientCM := createClientConfigMap(clientConfigMapName, namespace)
			Expect(k8sClient.Create(ctx, clientCM)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, clientCM)
			}()

			notebook := createUnstructuredNotebook(notebookName, namespace, map[string]string{
				NotebookFeastIntegrationLabel: TrueValue,
			}, nil)
			Expect(k8sClient.Create(ctx, notebook)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, notebook)
			}()

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      notebookName,
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      notebookName + NotebookConfigMapNameSuffix,
				Namespace: namespace,
			}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(BeEmpty())
		})

		It("should create ConfigMap with project config when integration is enabled with annotation", func() {
			fs := createFeatureStore(featureStoreName, namespace, projectName)
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			// Set and update status separately
			setFeatureStoreStatus(fs, projectName, clientConfigMapName)
			Expect(k8sClient.Status().Update(ctx, fs)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, fs)
			}()

			clientCM := createClientConfigMap(clientConfigMapName, namespace)
			Expect(k8sClient.Create(ctx, clientCM)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, clientCM)
			}()

			notebook := createUnstructuredNotebook(notebookName, namespace, map[string]string{
				NotebookFeastIntegrationLabel: TrueValue,
			}, map[string]string{
				NotebookFeastConfigAnnotation: projectName,
			})
			Expect(k8sClient.Create(ctx, notebook)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, notebook)
			}()

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      notebookName,
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      notebookName + NotebookConfigMapNameSuffix,
				Namespace: namespace,
			}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey(projectName))
			Expect(cm.Data[projectName]).To(Equal(testYAMLContent))
			Expect(cm.Labels["managed-by"]).To(Equal("feast-operator"))
			Expect(cm.Labels["source-resource"]).To(Equal(notebookName))
			Expect(cm.Labels["source-kind"]).To(Equal("Notebook"))
			Expect(cm.OwnerReferences).To(HaveLen(1))
			Expect(cm.OwnerReferences[0].Name).To(Equal(notebookName))
		})

		It("should handle multiple projects in annotation", func() {
			projectName2 := "test-project-2"
			featureStoreName2 := "test-featurestore-2"
			clientConfigMapName2 := "test-featurestore-2-client"

			fs1 := createFeatureStore(featureStoreName, namespace, projectName)
			Expect(k8sClient.Create(ctx, fs1)).To(Succeed())
			// Set and update status separately
			setFeatureStoreStatus(fs1, projectName, clientConfigMapName)
			Expect(k8sClient.Status().Update(ctx, fs1)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, fs1)
			}()

			fs2 := createFeatureStore(featureStoreName2, namespace, projectName2)
			Expect(k8sClient.Create(ctx, fs2)).To(Succeed())
			// Set and update status separately
			setFeatureStoreStatus(fs2, projectName2, clientConfigMapName2)
			Expect(k8sClient.Status().Update(ctx, fs2)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, fs2)
			}()

			clientCM1 := createClientConfigMap(clientConfigMapName, namespace)
			Expect(k8sClient.Create(ctx, clientCM1)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, clientCM1)
			}()

			testYAMLContent2 := "project: test-project-2\nprovider: local"
			clientCM2 := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clientConfigMapName2,
					Namespace: namespace,
				},
				Data: map[string]string{
					services.FeatureStoreYamlCmKey: testYAMLContent2,
				},
			}
			Expect(k8sClient.Create(ctx, clientCM2)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, clientCM2)
			}()

			notebook := createUnstructuredNotebook(notebookName, namespace, map[string]string{
				NotebookFeastIntegrationLabel: TrueValue,
			}, map[string]string{
				NotebookFeastConfigAnnotation: fmt.Sprintf("%s, %s", projectName, projectName2),
			})
			Expect(k8sClient.Create(ctx, notebook)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, notebook)
			}()

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      notebookName,
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      notebookName + NotebookConfigMapNameSuffix,
				Namespace: namespace,
			}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey(projectName))
			Expect(cm.Data).To(HaveKey(projectName2))
			Expect(cm.Data[projectName]).To(Equal(testYAMLContent))
			Expect(cm.Data[projectName2]).To(Equal(testYAMLContent2))
		})

		It("should handle project not found in FeatureStore", func() {
			notebook := createUnstructuredNotebook(notebookName, namespace, map[string]string{
				NotebookFeastIntegrationLabel: TrueValue,
			}, map[string]string{
				NotebookFeastConfigAnnotation: "non-existent-project",
			})
			Expect(k8sClient.Create(ctx, notebook)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, notebook)
			}()

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      notebookName,
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      notebookName + NotebookConfigMapNameSuffix,
				Namespace: namespace,
			}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).NotTo(HaveKey("non-existent-project"))
		})

		It("should update ConfigMap when project annotation changes", func() {
			fs1 := createFeatureStore(featureStoreName, namespace, projectName)
			Expect(k8sClient.Create(ctx, fs1)).To(Succeed())
			// Set and update status separately
			setFeatureStoreStatus(fs1, projectName, clientConfigMapName)
			Expect(k8sClient.Status().Update(ctx, fs1)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, fs1)
			}()

			projectName2 := "test-project-2"
			featureStoreName2 := "test-featurestore-2"
			clientConfigMapName2 := "test-featurestore-2-client"

			fs2 := createFeatureStore(featureStoreName2, namespace, projectName2)
			Expect(k8sClient.Create(ctx, fs2)).To(Succeed())
			// Set and update status separately
			setFeatureStoreStatus(fs2, projectName2, clientConfigMapName2)
			Expect(k8sClient.Status().Update(ctx, fs2)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, fs2)
			}()

			clientCM1 := createClientConfigMap(clientConfigMapName, namespace)
			Expect(k8sClient.Create(ctx, clientCM1)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, clientCM1)
			}()

			testYAMLContent2 := "project: test-project-2\nprovider: local"
			clientCM2 := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clientConfigMapName2,
					Namespace: namespace,
				},
				Data: map[string]string{
					services.FeatureStoreYamlCmKey: testYAMLContent2,
				},
			}
			Expect(k8sClient.Create(ctx, clientCM2)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, clientCM2)
			}()

			notebook := createUnstructuredNotebook(notebookName, namespace, map[string]string{
				NotebookFeastIntegrationLabel: TrueValue,
			}, map[string]string{
				NotebookFeastConfigAnnotation: projectName,
			})
			Expect(k8sClient.Create(ctx, notebook)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, notebook)
			}()

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      notebookName,
					Namespace: namespace,
				},
			}
			result, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      notebookName + NotebookConfigMapNameSuffix,
				Namespace: namespace,
			}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey(projectName))
			Expect(cm.Data).NotTo(HaveKey(projectName2))

			notebook.SetAnnotations(map[string]string{
				NotebookFeastConfigAnnotation: projectName2,
			})
			Expect(k8sClient.Update(ctx, notebook)).To(Succeed())

			result, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      notebookName + NotebookConfigMapNameSuffix,
				Namespace: namespace,
			}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).NotTo(HaveKey(projectName))
			Expect(cm.Data).To(HaveKey(projectName2))
			Expect(cm.Data[projectName2]).To(Equal(testYAMLContent2))
		})
	})

	Context("getClientConfigYAMLForProject", func() {
		It("should return YAML content for matching project", func() {
			fs := createFeatureStore(featureStoreName, namespace, projectName)
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			// Set and update status separately
			setFeatureStoreStatus(fs, projectName, clientConfigMapName)
			Expect(k8sClient.Status().Update(ctx, fs)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, fs)
			}()

			clientCM := createClientConfigMap(clientConfigMapName, namespace)
			Expect(k8sClient.Create(ctx, clientCM)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, clientCM)
			}()

			yaml, err := reconciler.getClientConfigYAMLForProject(ctx, projectName)
			Expect(err).NotTo(HaveOccurred())
			Expect(yaml).To(Equal(testYAMLContent))
		})

		It("should return error when project not found", func() {
			_, err := reconciler.getClientConfigYAMLForProject(ctx, "non-existent-project")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no FeatureStore found"))
		})

		It("should return error when ClientConfigMap doesn't exist", func() {
			fs := createFeatureStore(featureStoreName, namespace, projectName)
			Expect(k8sClient.Create(ctx, fs)).To(Succeed())
			// Set status with non-existent configmap
			setFeatureStoreStatus(fs, projectName, "non-existent-configmap")
			Expect(k8sClient.Status().Update(ctx, fs)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, fs)
			}()

			_, err := reconciler.getClientConfigYAMLForProject(ctx, projectName)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get client ConfigMap"))
		})
	})
})
