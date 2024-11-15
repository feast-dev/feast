package api

import (
	"context"

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

// Function to create invalid OnlineStore resource
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
	Expect(err).ToNot(BeNil())
	Expect(err.Error()).Should(ContainSubstring(matcher))
}

func onlineStoreWithAbsolutePathForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path:      "/data/online_store.db",
					PvcConfig: &feastdevv1alpha1.PvcConfig{},
				},
			},
		},
	}
	return copy
}
func onlineStoreWithRelativePathForEphemeral(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path: "data/online_store.db",
				},
			},
		},
	}
	return copy
}

func onlineStoreWithS3BucketForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path: "s3://bucket/online_store.db",
					PvcConfig: &feastdevv1alpha1.PvcConfig{
						Create:    &feastdevv1alpha1.PvcCreate{},
						MountPath: "/data/online",
					},
				},
			},
		},
	}
	return copy
}
func onlineStoreWithGsBucketForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path: "gs://bucket/online_store.db",
					PvcConfig: &feastdevv1alpha1.PvcConfig{
						Create:    &feastdevv1alpha1.PvcCreate{},
						MountPath: "/data/online",
					},
				},
			},
		},
	}
	return copy
}

func offlineStoreWithUnmanagedFileType(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
					Type: "unmanaged",
				},
			},
		},
	}
	return copy
}

func registryWithAbsolutePathForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
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
	return copy
}
func registryWithRelativePathForEphemeral(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
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
	return copy
}
func registryWithS3BucketForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path: "s3://bucket/registry.db",
						PvcConfig: &feastdevv1alpha1.PvcConfig{
							Create:    &feastdevv1alpha1.PvcCreate{},
							MountPath: "/data/registry",
						},
					},
				},
			},
		},
	}
	return copy
}
func registryWithGsBucketForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path: "gs://bucket/registry.db",
						PvcConfig: &feastdevv1alpha1.PvcConfig{
							Create:    &feastdevv1alpha1.PvcCreate{},
							MountPath: "/data/registry",
						},
					},
				},
			},
		},
	}
	return copy
}
func registryWithS3AdditionalKeywordsForFile(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
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
	return copy
}
func registryWithS3AdditionalKeywordsForGsBucket(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
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
	return copy
}

func pvcConfigWithNeitherRefNorCreate(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
					PvcConfig: &feastdevv1alpha1.PvcConfig{},
				},
			},
		},
	}
	return copy
}
func pvcConfigWithBothRefAndCreate(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
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
	return copy
}

func pvcConfigWithNoResources(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
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
	return copy
}

func pvcConfigWithResources(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := pvcConfigWithNoResources(featureStore)
	copy.Spec.Services.OfflineStore.Persistence.FilePersistence.PvcConfig.Create.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("10Gi"),
		},
	}
	copy.Spec.Services.OnlineStore.Persistence.FilePersistence.PvcConfig.Create.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		},
	}
	copy.Spec.Services.Registry.Local.Persistence.FilePersistence.PvcConfig.Create.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("500Mi"),
		},
	}
	return copy
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
		Expect(err != nil && errors.IsNotFound(err))
	})
	AfterEach(func() {
		By("verifying the custom resource FeatureStore is not there")
		err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
		Expect(err != nil && errors.IsNotFound(err))
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
		It("should fail when PVC persistence has S3 bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithS3BucketForPvc(featurestore), "Online store does not support S3 or GS")
		})
		It("should fail when PVC persistence has GS bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithGsBucketForPvc(featurestore), "Online store does not support S3 or GS")
		})
	})

	Context("When creating an invalid Offline Store", func() {
		ctx, featurestore := initContext()

		It("should fail when PVC persistence has absolute path", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithUnmanagedFileType(featurestore), "Unsupported value")
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
		It("should fail when PVC persistence has S3 bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithS3BucketForPvc(featurestore), "PVC persistence does not support S3 or GS object store URIs")
			attemptInvalidCreationAndAsserts(ctx, registryWithGsBucketForPvc(featurestore), "PVC persistence does not support S3 or GS object store URIs")
		})
		It("should fail when additional S3 settings are provided to non S3 bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithS3AdditionalKeywordsForFile(featurestore), "Additional S3 settings are available only for S3 object store URIs")
			attemptInvalidCreationAndAsserts(ctx, registryWithS3AdditionalKeywordsForGsBucket(featurestore), "Additional S3 settings are available only for S3 object store URIs")
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
})
