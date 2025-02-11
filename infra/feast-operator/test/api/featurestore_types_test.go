package api

import (
	"context"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createFeatureStore() *feastdevv1alpha1.FeatureStore {
	return &feastdevv1alpha1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: namespaceName,
		},
		Spec: feastdevv1alpha1.FeatureStoreSpec{
			FeastProject: "test_project",
		},
	}
}

func attemptInvalidCreationAndAsserts(ctx context.Context, featurestore *feastdevv1alpha1.FeatureStore, matcher string) {
	By("Creating the resource")
	logger := log.FromContext(ctx)
	logger.Info("Creating", "FeatureStore", featurestore)
	err := k8sClient.Create(ctx, featurestore)
	logger.Info("Got", "err", err)
	Expect(err).To(HaveOccurred())
	Expect(err.Error()).Should(ContainSubstring(matcher))
}

func onlineStoreWithAbsolutePathForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path:      "/data/online_store.db",
					PvcConfig: &feastdevv1alpha1.PvcConfig{},
				},
			},
		},
	}
	return fsCopy
}
func onlineStoreWithRelativePathForEphemeral(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path: "data/online_store.db",
				},
			},
		},
	}
	return fsCopy
}

func onlineStoreWithObjectStoreBucketForPvc(path string, featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path: path,
					PvcConfig: &feastdevv1alpha1.PvcConfig{
						Create:    &feastdevv1alpha1.PvcCreate{},
						MountPath: "/data/online",
					},
				},
			},
		},
	}
	return fsCopy
}

func offlineStoreWithUnmanagedFileType(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
					Type: "unmanaged",
				},
			},
		},
	}
	return fsCopy
}

func registryWithAbsolutePathForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path:      "/data/registry.db",
						PvcConfig: &feastdevv1alpha1.PvcConfig{},
					}},
			},
		},
	}
	return fsCopy
}
func registryWithRelativePathForEphemeral(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path: "data/online_store.db",
					},
				},
			},
		},
	}
	return fsCopy
}
func registryWithObjectStoreBucketForPvc(path string, featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path: path,
						PvcConfig: &feastdevv1alpha1.PvcConfig{
							Create:    &feastdevv1alpha1.PvcCreate{},
							MountPath: "/data/registry",
						},
					},
				},
			},
		},
	}
	return fsCopy
}
func registryWithS3AdditionalKeywordsForFile(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path:               "/data/online_store.db",
						S3AdditionalKwargs: &map[string]string{},
					},
				},
			},
		},
	}
	return fsCopy
}
func registryWithS3AdditionalKeywordsForGsBucket(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path:               "gs://online_store.db",
						S3AdditionalKwargs: &map[string]string{},
					},
				},
			},
		},
	}
	return fsCopy
}

func pvcConfigWithNeitherRefNorCreate(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
					PvcConfig: &feastdevv1alpha1.PvcConfig{},
				},
			},
		},
	}
	return fsCopy
}
func pvcConfigWithBothRefAndCreate(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
					PvcConfig: &feastdevv1alpha1.PvcConfig{
						Ref: &corev1.LocalObjectReference{
							Name: "pvc",
						},
						Create: &feastdevv1alpha1.PvcCreate{},
					},
				},
			},
		},
	}
	return fsCopy
}

func pvcConfigWithNoResources(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
					PvcConfig: &feastdevv1alpha1.PvcConfig{
						Create:    &feastdevv1alpha1.PvcCreate{},
						MountPath: "/data/offline",
					},
				},
			},
		},
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					PvcConfig: &feastdevv1alpha1.PvcConfig{
						Create:    &feastdevv1alpha1.PvcCreate{},
						MountPath: "/data/online",
					},
				},
			},
		},
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						PvcConfig: &feastdevv1alpha1.PvcConfig{
							Create:    &feastdevv1alpha1.PvcCreate{},
							MountPath: "/data/registry",
						},
					},
				},
			},
		},
	}
	return fsCopy
}

func pvcConfigWithResources(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := pvcConfigWithNoResources(featureStore)
	fsCopy.Spec.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("10Gi"),
		},
	}
	fsCopy.Spec.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		},
	}
	fsCopy.Spec.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("500Mi"),
		},
	}
	return fsCopy
}

func authzConfigWithKubernetes(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	if fsCopy.Spec.AuthzConfig == nil {
		fsCopy.Spec.AuthzConfig = &feastdevv1alpha1.AuthzConfig{}
	}
	fsCopy.Spec.AuthzConfig.KubernetesAuthz = &feastdevv1alpha1.KubernetesAuthz{
		Roles: []string{},
	}
	return fsCopy
}
func authzConfigWithOidc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	if fsCopy.Spec.AuthzConfig == nil {
		fsCopy.Spec.AuthzConfig = &feastdevv1alpha1.AuthzConfig{}
	}
	fsCopy.Spec.AuthzConfig.OidcAuthz = &feastdevv1alpha1.OidcAuthz{}
	return fsCopy
}

func onlineStoreWithDBPersistenceType(dbPersistenceType string, featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				DBPersistence: &feastdevv1alpha1.OnlineStoreDBStorePersistence{
					Type: dbPersistenceType,
				},
			},
		},
	}
	return fsCopy
}

func offlineStoreWithDBPersistenceType(dbPersistenceType string, featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				DBPersistence: &feastdevv1alpha1.OfflineStoreDBStorePersistence{
					Type: dbPersistenceType,
				},
			},
		},
	}
	return fsCopy
}

func registryStoreWithDBPersistenceType(dbPersistenceType string, featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					DBPersistence: &feastdevv1alpha1.RegistryDBStorePersistence{
						Type: dbPersistenceType,
					},
				},
			},
		},
	}
	return fsCopy
}

func quotedSlice(stringSlice []string) string {
	quotedSlice := make([]string, len(stringSlice))

	for i, str := range stringSlice {
		quotedSlice[i] = fmt.Sprintf("\"%s\"", str)
	}

	return strings.Join(quotedSlice, ", ")
}

const resourceName = "test-resource"
const namespaceName = "default"

var typeNamespacedName = types.NamespacedName{
	Name:      resourceName,
	Namespace: "default",
}

func initContext() (context.Context, *feastdevv1alpha1.FeatureStore) {
	ctx := context.Background()

	featurestore := createFeatureStore()

	BeforeEach(func() {
		By("verifying the custom resource FeatureStore is not there")
		err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
		Expect(err != nil && errors.IsNotFound(err)).To(BeTrue())
	})
	AfterEach(func() {
		By("verifying the custom resource FeatureStore is not there")
		err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
		Expect(err != nil && errors.IsNotFound(err)).To(BeTrue())
	})

	return ctx, featurestore
}

var _ = Describe("FeatureStore API", func() {
	Context("When creating an invalid Online Store", func() {
		ctx, featurestore := initContext()

		It("should fail when PVC persistence has absolute path", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithAbsolutePathForPvc(featurestore), "PVC path must be a file name only")
		})
		It("should fail when ephemeral persistence has relative path", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithRelativePathForEphemeral(featurestore), "Ephemeral stores must have absolute paths")
		})
		It("should fail when PVC persistence has object store bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithObjectStoreBucketForPvc("s3://bucket/online_store.db", featurestore), "Online store does not support S3 or GS")
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithObjectStoreBucketForPvc("gs://bucket/online_store.db", featurestore), "Online store does not support S3 or GS")
		})

		It("should fail when db persistence type is invalid", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithDBPersistenceType("invalid", featurestore), "Unsupported value: \"invalid\": supported values: "+quotedSlice(feastdevv1alpha1.ValidOnlineStoreDBStorePersistenceTypes))
		})
	})

	Context("When creating an invalid Offline Store", func() {
		ctx, featurestore := initContext()

		It("should fail when PVC persistence has absolute path", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithUnmanagedFileType(featurestore), "Unsupported value")
		})
		It("should fail when db persistence type is invalid", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithDBPersistenceType("invalid", featurestore), "Unsupported value: \"invalid\": supported values: "+quotedSlice(feastdevv1alpha1.ValidOfflineStoreDBStorePersistenceTypes))
		})
	})

	Context("When creating an invalid Registry", func() {
		ctx, featurestore := initContext()

		It("should fail when PVC persistence has absolute path", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithAbsolutePathForPvc(featurestore), "PVC path must be a file name only")
		})
		It("should fail when ephemeral persistence has relative path", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithRelativePathForEphemeral(featurestore), "Registry files must use absolute paths or be S3 ('s3://') or GS ('gs://')")
		})
		It("should fail when PVC persistence has object store bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithObjectStoreBucketForPvc("s3://bucket/registry.db", featurestore), "PVC persistence does not support S3 or GS object store URIs")
			attemptInvalidCreationAndAsserts(ctx, registryWithObjectStoreBucketForPvc("gs://bucket/registry.db", featurestore), "PVC persistence does not support S3 or GS object store URIs")
		})
		It("should fail when additional S3 settings are provided to non S3 bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithS3AdditionalKeywordsForFile(featurestore), "Additional S3 settings are available only for S3 object store URIs")
			attemptInvalidCreationAndAsserts(ctx, registryWithS3AdditionalKeywordsForGsBucket(featurestore), "Additional S3 settings are available only for S3 object store URIs")
		})
		It("should fail when db persistence type is invalid", func() {
			attemptInvalidCreationAndAsserts(ctx, registryStoreWithDBPersistenceType("invalid", featurestore), "Unsupported value: \"invalid\": supported values: "+quotedSlice(feastdevv1alpha1.ValidRegistryDBStorePersistenceTypes))
		})
	})

	Context("When creating an invalid PvcConfig", func() {
		ctx, featurestore := initContext()

		It("should fail when neither ref nor create settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, pvcConfigWithNeitherRefNorCreate(featurestore), "One selection is required")
		})
		It("should fail when both ref and create settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, pvcConfigWithBothRefAndCreate(featurestore), "One selection is required")
		})
	})

	Context("When creating a valid PvcConfig", func() {
		_, featurestore := initContext()

		It("should set the expected defaults", func() {
			resource := pvcConfigWithNoResources(featurestore)
			services.ApplyDefaultsToStatus(resource)

			storage := resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.Resources.Requests.Storage().String()
			Expect(storage).To(Equal("20Gi"))
			storage = resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.Resources.Requests.Storage().String()
			Expect(storage).To(Equal("5Gi"))
			storage = resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.Resources.Requests.Storage().String()
			Expect(storage).To(Equal("5Gi"))
		})
		It("should not override the configured resources", func() {
			resource := pvcConfigWithResources(featurestore)
			services.ApplyDefaultsToStatus(resource)
			storage := resource.Status.Applied.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.Resources.Requests.Storage().String()
			Expect(storage).To(Equal("10Gi"))
			storage = resource.Status.Applied.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.Resources.Requests.Storage().String()
			Expect(storage).To(Equal("1Gi"))
			storage = resource.Status.Applied.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.Resources.Requests.Storage().String()
			Expect(storage).To(Equal("500Mi"))
		})
	})
	Context("When omitting the AuthzConfig PvcConfig", func() {
		_, featurestore := initContext()
		It("should keep an empty AuthzConfig", func() {
			resource := featurestore
			services.ApplyDefaultsToStatus(resource)
			Expect(resource.Status.Applied.AuthzConfig).To(BeNil())
		})
	})
	Context("When configuring the AuthzConfig", func() {
		ctx, featurestore := initContext()
		It("should fail when both kubernetes and oidc settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, authzConfigWithOidc(authzConfigWithKubernetes(featurestore)), "One selection required between kubernetes or oidc")
		})
	})
})
