package api

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Function to create invalid OnlineStore resource
func createFeatureStore() *feastdevv1alpha1.FeatureStore {
	return &feastdevv1alpha1.FeatureStore{
		ObjectMeta: v1.ObjectMeta{
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

func onlineStoreWithBothFileAndStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path: "/data/online_store.db",
				},
				StorePersistence: &feastdevv1alpha1.OnlineStoreStorePersistence{
					Type: "postgres",
				},
			},
		},
	}
	return copy
}
func onlineStoreWithNeitherFileNorStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{},
		},
	}
	return copy
}
func onlineStoreWithUnmanagedStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				StorePersistence: &feastdevv1alpha1.OnlineStoreStorePersistence{
					Type: "unmanaged",
				},
			},
		},
	}
	return copy
}

func onlineStoreWithAbsolutePathForPvc(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OnlineStore: &feastdevv1alpha1.OnlineStore{
			Persistence: &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path: "/data/online_store.db",
					PvcStore: &feastdevv1alpha1.PvcStore{
						Name: "example-pvc",
					},
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
					PvcStore: &feastdevv1alpha1.PvcStore{
						Name: "example-pvc",
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
					PvcStore: &feastdevv1alpha1.PvcStore{
						Name: "example-pvc",
					},
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

func offlineStoreWithBothFileAndStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{},
				StorePersistence: &feastdevv1alpha1.OfflineStoreStorePersistence{
					Type: "postgres",
				},
			},
		},
	}
	return copy
}
func offlineStoreWithNeitherFileNorStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{},
		},
	}
	return copy
}
func offlineStoreWithUnmanagedStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		OfflineStore: &feastdevv1alpha1.OfflineStore{
			Persistence: &feastdevv1alpha1.OfflineStorePersistence{
				StorePersistence: &feastdevv1alpha1.OfflineStoreStorePersistence{
					Type: "unmanaged",
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

func registryWithBothFileAndStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path: "/data/online_store.db",
					},
					StorePersistence: &feastdevv1alpha1.RegistryStorePersistence{
						Type: "sql",
					},
				},
			},
		},
	}
	return copy
}
func registryWithNeitherFileNorStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{},
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
						Path: "/data/registry.db",
						PvcStore: &feastdevv1alpha1.PvcStore{
							Name: "example-pvc",
						},
					}},
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
						PvcStore: &feastdevv1alpha1.PvcStore{
							Name: "example-pvc",
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
						PvcStore: &feastdevv1alpha1.PvcStore{
							Name: "example-pvc",
						},
					},
				},
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
func registryWithUnmanagedStore(featureStore *feastdevv1alpha1.FeatureStore) *feastdevv1alpha1.FeatureStore {
	copy := featureStore.DeepCopy()
	copy.Spec.Services = &feastdevv1alpha1.FeatureStoreServices{
		Registry: &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					StorePersistence: &feastdevv1alpha1.RegistryStorePersistence{
						Type: "unmanaged",
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
						Path:          "/data/online_store.db",
						S3AddtlKwargs: &map[string]string{},
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
						Path:          "gs://online_store.db",
						S3AddtlKwargs: &map[string]string{},
					},
				},
			},
		},
	}
	return copy
}

const resourceName = "test-resource"
const namespaceName = "default"

func initContext() (context.Context, *feastdevv1alpha1.FeatureStore) {
	ctx := context.Background()

	typeNamespacedName := types.NamespacedName{
		Name:      resourceName,
		Namespace: "default",
	}
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

		It("should fail when both file and store settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithBothFileAndStore(featurestore), "One file or store persistence is allowed")
		})
		It("should fail when neither file nor store settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithNeitherFileNorStore(featurestore), "Either file or store persistence are required")
		})
		It("should fail when unsupported offline store is given", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithUnmanagedStore(featurestore), "Unsupported value:")
		})
		It("should fail when PVC persistence has absolute path", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithAbsolutePathForPvc(featurestore), "PVC path must be a file name only")
		})
		It("should fail when PVC persistence has S3 bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithS3BucketForPvc(featurestore), "Online store does not support S3 or GS")
		})
		It("should fail when PVC persistence has GS bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithGsBucketForPvc(featurestore), "Online store does not support S3 or GS")
		})
		It("should fail when ephemeral persistence has relative path", func() {
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithRelativePathForEphemeral(featurestore), "Ephemeral stores must have absolute paths")
		})
	})

	Context("When creating an invalid Offline Store", func() {
		ctx, featurestore := initContext()

		It("should fail when both file and store settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithBothFileAndStore(featurestore), "One file or store persistence is allowed")
		})
		It("should fail when neither file nor store settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithNeitherFileNorStore(featurestore), "Either file or store persistence are required")
		})
		It("should fail when unsupported offline store is given", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithUnmanagedStore(featurestore), "Unsupported value:")
		})
		It("should fail when PVC persistence has absolute path", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithUnmanagedFileType(featurestore), "Unsupported value")
		})
	})

	Context("When creating an invalid Registry", func() {
		ctx, featurestore := initContext()
		It("should fail when both file and store settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithBothFileAndStore(featurestore), "One file or store persistence is allowed")
		})
		It("should fail when neither file nor store settings are given", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithNeitherFileNorStore(featurestore), "Either file or store persistence are required")
		})
		It("should fail when PVC persistence has absolute path", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithAbsolutePathForPvc(featurestore), "PVC path must be a file name only")
		})
		It("should fail when PVC persistence has S3 bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithS3BucketForPvc(featurestore), "PVC persistence does not support S3 or GS buckets")
		})
		It("should fail when PVC persistence has GS bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithGsBucketForPvc(featurestore), "PVC persistence does not support S3 or GS buckets")
		})
		It("should fail when unsupported registry store is given", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithUnmanagedStore(featurestore), "Unsupported value:")
		})
		It("should fail when ephemeral persistence has relative path", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithRelativePathForEphemeral(featurestore), "Ephemeral stores must have absolute paths")
		})
		It("should fail when additional S3 settings are provided to non S3 bucket", func() {
			attemptInvalidCreationAndAsserts(ctx, registryWithS3AdditionalKeywordsForFile(featurestore), "Additional S3 settings are available only for S3 buckets")
			attemptInvalidCreationAndAsserts(ctx, registryWithS3AdditionalKeywordsForGsBucket(featurestore), "Additional S3 settings are available only for S3 buckets")
		})
	})
})
