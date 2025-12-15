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

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func boolPtr(b bool) *bool {
	return &b
}

func createFeatureStore() *feastdevv1.FeatureStore {
	return &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: namespaceName,
		},
		Spec: feastdevv1.FeatureStoreSpec{
			FeastProject: "test_project",
		},
	}
}

func attemptInvalidCreationAndAsserts(ctx context.Context, featurestore *feastdevv1.FeatureStore, matcher string) {
	By("Creating the resource")
	logger := log.FromContext(ctx)
	logger.Info("Creating", "FeatureStore", featurestore)
	err := k8sClient.Create(ctx, featurestore)
	logger.Info("Got", "err", err)
	Expect(err).To(HaveOccurred())
	Expect(err.Error()).Should(ContainSubstring(matcher))
}

func onlineStoreWithAbsolutePathForPvc(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path:      "/data/online_store.db",
					PvcConfig: &feastdevv1.PvcConfig{},
				},
			},
		},
	}
	return fsCopy
}
func onlineStoreWithRelativePathForEphemeral(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path: "data/online_store.db",
				},
			},
		},
	}
	return fsCopy
}

func onlineStoreWithObjectStoreBucketForPvc(path string, featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					Path: path,
					PvcConfig: &feastdevv1.PvcConfig{
						Create:    &feastdevv1.PvcCreate{},
						MountPath: "/data/online",
					},
				},
			},
		},
	}
	return fsCopy
}

func offlineStoreWithUnmanagedFileType(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OfflineStore: &feastdevv1.OfflineStore{
			Persistence: &feastdevv1.OfflineStorePersistence{
				FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
					Type: "unmanaged",
				},
			},
		},
	}
	return fsCopy
}

func registryWithAbsolutePathForPvc(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Persistence: &feastdevv1.RegistryPersistence{
					FilePersistence: &feastdevv1.RegistryFilePersistence{
						Path:      "/data/registry.db",
						PvcConfig: &feastdevv1.PvcConfig{},
					}},
			},
		},
	}
	return fsCopy
}
func registryWithRelativePathForEphemeral(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Persistence: &feastdevv1.RegistryPersistence{
					FilePersistence: &feastdevv1.RegistryFilePersistence{
						Path: "data/online_store.db",
					},
				},
			},
		},
	}
	return fsCopy
}
func registryWithObjectStoreBucketForPvc(path string, featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Persistence: &feastdevv1.RegistryPersistence{
					FilePersistence: &feastdevv1.RegistryFilePersistence{
						Path: path,
						PvcConfig: &feastdevv1.PvcConfig{
							Create:    &feastdevv1.PvcCreate{},
							MountPath: "/data/registry",
						},
					},
				},
			},
		},
	}
	return fsCopy
}
func registryWithS3AdditionalKeywordsForFile(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Persistence: &feastdevv1.RegistryPersistence{
					FilePersistence: &feastdevv1.RegistryFilePersistence{
						Path:               "/data/online_store.db",
						S3AdditionalKwargs: &map[string]string{},
					},
				},
			},
		},
	}
	return fsCopy
}
func registryWithS3AdditionalKeywordsForGsBucket(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Persistence: &feastdevv1.RegistryPersistence{
					FilePersistence: &feastdevv1.RegistryFilePersistence{
						Path:               "gs://online_store.db",
						S3AdditionalKwargs: &map[string]string{},
					},
				},
			},
		},
	}
	return fsCopy
}

func pvcConfigWithNeitherRefNorCreate(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OfflineStore: &feastdevv1.OfflineStore{
			Persistence: &feastdevv1.OfflineStorePersistence{
				FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
					PvcConfig: &feastdevv1.PvcConfig{},
				},
			},
		},
	}
	return fsCopy
}
func pvcConfigWithBothRefAndCreate(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OfflineStore: &feastdevv1.OfflineStore{
			Persistence: &feastdevv1.OfflineStorePersistence{
				FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
					PvcConfig: &feastdevv1.PvcConfig{
						Ref: &corev1.LocalObjectReference{
							Name: "pvc",
						},
						Create: &feastdevv1.PvcCreate{},
					},
				},
			},
		},
	}
	return fsCopy
}

func pvcConfigWithNoResources(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OfflineStore: &feastdevv1.OfflineStore{
			Persistence: &feastdevv1.OfflineStorePersistence{
				FilePersistence: &feastdevv1.OfflineStoreFilePersistence{
					PvcConfig: &feastdevv1.PvcConfig{
						Create:    &feastdevv1.PvcCreate{},
						MountPath: "/data/offline",
					},
				},
			},
		},
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				FilePersistence: &feastdevv1.OnlineStoreFilePersistence{
					PvcConfig: &feastdevv1.PvcConfig{
						Create:    &feastdevv1.PvcCreate{},
						MountPath: "/data/online",
					},
				},
			},
		},
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Persistence: &feastdevv1.RegistryPersistence{
					FilePersistence: &feastdevv1.RegistryFilePersistence{
						PvcConfig: &feastdevv1.PvcConfig{
							Create:    &feastdevv1.PvcCreate{},
							MountPath: "/data/registry",
						},
					},
				},
			},
		},
	}
	return fsCopy
}

func pvcConfigWithResources(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
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

func authzConfigWithKubernetes(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	if fsCopy.Spec.AuthzConfig == nil {
		fsCopy.Spec.AuthzConfig = &feastdevv1.AuthzConfig{}
	}
	fsCopy.Spec.AuthzConfig.KubernetesAuthz = &feastdevv1.KubernetesAuthz{
		Roles: []string{},
	}
	return fsCopy
}
func authzConfigWithOidc(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	if fsCopy.Spec.AuthzConfig == nil {
		fsCopy.Spec.AuthzConfig = &feastdevv1.AuthzConfig{}
	}
	fsCopy.Spec.AuthzConfig.OidcAuthz = &feastdevv1.OidcAuthz{}
	return fsCopy
}

func onlineStoreWithDBPersistenceType(dbPersistenceType string, featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OnlineStore: &feastdevv1.OnlineStore{
			Persistence: &feastdevv1.OnlineStorePersistence{
				DBPersistence: &feastdevv1.OnlineStoreDBStorePersistence{
					Type: dbPersistenceType,
				},
			},
		},
	}
	return fsCopy
}

func offlineStoreWithDBPersistenceType(dbPersistenceType string, featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		OfflineStore: &feastdevv1.OfflineStore{
			Persistence: &feastdevv1.OfflineStorePersistence{
				DBPersistence: &feastdevv1.OfflineStoreDBStorePersistence{
					Type: dbPersistenceType,
				},
			},
		},
	}
	return fsCopy
}

func registryStoreWithDBPersistenceType(dbPersistenceType string, featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Persistence: &feastdevv1.RegistryPersistence{
					DBPersistence: &feastdevv1.RegistryDBStorePersistence{
						Type: dbPersistenceType,
					},
				},
			},
		},
	}
	return fsCopy
}

func registryWithRestAPIFalse(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Server: &feastdevv1.RegistryServerConfigs{
					RestAPI: boolPtr(false),
				},
			},
		},
	}
	return fsCopy
}

func registryWithOnlyRestAPI(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Server: &feastdevv1.RegistryServerConfigs{
					RestAPI: boolPtr(true),
				},
			},
		},
	}
	return fsCopy
}

func registryWithOnlyGRPC(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Server: &feastdevv1.RegistryServerConfigs{
					GRPC: boolPtr(true),
				},
			},
		},
	}
	return fsCopy
}

func registryWithBothAPIs(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Server: &feastdevv1.RegistryServerConfigs{
					RestAPI: boolPtr(true),
					GRPC:    boolPtr(true),
				},
			},
		},
	}
	return fsCopy
}

func registryWithNoAPIs(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Server: &feastdevv1.RegistryServerConfigs{},
			},
		},
	}
	return fsCopy
}

func registryWithBothFalse(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Server: &feastdevv1.RegistryServerConfigs{
					RestAPI: boolPtr(false),
					GRPC:    boolPtr(false),
				},
			},
		},
	}
	return fsCopy
}

func registryWithGRPCFalse(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.Services = &feastdevv1.FeatureStoreServices{
		Registry: &feastdevv1.Registry{
			Local: &feastdevv1.LocalRegistryConfig{
				Server: &feastdevv1.RegistryServerConfigs{
					GRPC: boolPtr(false),
				},
			},
		},
	}
	return fsCopy
}

func cronJobWithAnnotations(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.CronJob = &feastdevv1.FeastCronJob{
		Annotations: map[string]string{
			"test-annotation":    "test-value",
			"another-annotation": "another-value",
		},
		Schedule: "0 0 * * *",
	}
	return fsCopy
}

func cronJobWithEmptyAnnotations(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.CronJob = &feastdevv1.FeastCronJob{
		Annotations: map[string]string{},
		Schedule:    "0 0 * * *",
	}
	return fsCopy
}

func cronJobWithoutAnnotations(featureStore *feastdevv1.FeatureStore) *feastdevv1.FeatureStore {
	fsCopy := featureStore.DeepCopy()
	fsCopy.Spec.CronJob = &feastdevv1.FeastCronJob{
		Schedule: "0 0 * * *",
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

func initContext() (context.Context, *feastdevv1.FeatureStore) {
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
			attemptInvalidCreationAndAsserts(ctx, onlineStoreWithDBPersistenceType("invalid", featurestore), "Unsupported value: \"invalid\": supported values: "+quotedSlice(feastdevv1.ValidOnlineStoreDBStorePersistenceTypes))
		})
	})

	Context("When creating an invalid Offline Store", func() {
		ctx, featurestore := initContext()

		It("should fail when PVC persistence has absolute path", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithUnmanagedFileType(featurestore), "Unsupported value")
		})
		It("should fail when db persistence type is invalid", func() {
			attemptInvalidCreationAndAsserts(ctx, offlineStoreWithDBPersistenceType("invalid", featurestore), "Unsupported value: \"invalid\": supported values: "+quotedSlice(feastdevv1.ValidOfflineStoreDBStorePersistenceTypes))
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
			attemptInvalidCreationAndAsserts(ctx, registryStoreWithDBPersistenceType("invalid", featurestore), "Unsupported value: \"invalid\": supported values: "+quotedSlice(feastdevv1.ValidRegistryDBStorePersistenceTypes))
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

	Context("When creating a Registry", func() {
		ctx := context.Background()

		BeforeEach(func() {
			By("verifying the custom resource FeatureStore is not there")
			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err != nil && errors.IsNotFound(err)).To(BeTrue())
		})
		AfterEach(func() {
			By("Cleaning up the test resource")
			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err != nil && errors.IsNotFound(err)).To(BeTrue())
		})

		Context("with valid API configurations", func() {
			It("should succeed when restAPI is false and grpc is not specified (defaults to true)", func() {
				featurestore := createFeatureStore()
				resource := registryWithRestAPIFalse(featurestore)
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			})

			It("should succeed when restAPI is true and grpc is not specified (defaults to true)", func() {
				featurestore := createFeatureStore()
				resource := registryWithOnlyRestAPI(featurestore)
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			})

			It("should succeed when only grpc is true", func() {
				featurestore := createFeatureStore()
				resource := registryWithOnlyGRPC(featurestore)
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			})

			It("should succeed when both APIs are true", func() {
				featurestore := createFeatureStore()
				resource := registryWithBothAPIs(featurestore)
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			})

			It("should succeed when no APIs are specified (grpc defaults to true)", func() {
				featurestore := createFeatureStore()
				resource := registryWithNoAPIs(featurestore)
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			})
		})

		Context("with invalid API configurations", func() {
			It("should fail when both APIs are explicitly false", func() {
				featurestore := createFeatureStore()
				resource := registryWithBothFalse(featurestore)
				attemptInvalidCreationAndAsserts(ctx, resource, "At least one of restAPI or grpc must be true")
			})

			It("should fail when grpc is false and restAPI is not specified", func() {
				featurestore := createFeatureStore()
				resource := registryWithGRPCFalse(featurestore)
				attemptInvalidCreationAndAsserts(ctx, resource, "At least one of restAPI or grpc must be true")
			})
		})
	})

	Context("When creating a CronJob", func() {
		ctx := context.Background()

		BeforeEach(func() {
			By("verifying the custom resource FeatureStore is not there")
			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err != nil && errors.IsNotFound(err)).To(BeTrue())
		})
		AfterEach(func() {
			By("Cleaning up the test resource")
			resource := &feastdevv1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err != nil && errors.IsNotFound(err)).To(BeTrue())
		})

		Context("with annotations", func() {
			It("should succeed when annotations are provided", func() {
				featurestore := createFeatureStore()
				resource := cronJobWithAnnotations(featurestore)
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			})

			It("should succeed when annotations are empty", func() {
				featurestore := createFeatureStore()
				resource := cronJobWithEmptyAnnotations(featurestore)
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			})

			It("should succeed when annotations are not specified", func() {
				featurestore := createFeatureStore()
				resource := cronJobWithoutAnnotations(featurestore)
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			})

			It("should apply the annotations correctly in the status", func() {
				featurestore := createFeatureStore()
				resource := cronJobWithAnnotations(featurestore)
				services.ApplyDefaultsToStatus(resource)

				Expect(resource.Status.Applied.CronJob).NotTo(BeNil())
				Expect(resource.Status.Applied.CronJob.Annotations).NotTo(BeNil())
				Expect(resource.Status.Applied.CronJob.Annotations).To(HaveLen(2))
				Expect(resource.Status.Applied.CronJob.Annotations["test-annotation"]).To(Equal("test-value"))
				Expect(resource.Status.Applied.CronJob.Annotations["another-annotation"]).To(Equal("another-value"))
			})

			It("should keep empty annotations in the status", func() {
				featurestore := createFeatureStore()
				resource := cronJobWithEmptyAnnotations(featurestore)
				services.ApplyDefaultsToStatus(resource)

				Expect(resource.Status.Applied.CronJob).NotTo(BeNil())
				Expect(resource.Status.Applied.CronJob.Annotations).NotTo(BeNil())
				Expect(resource.Status.Applied.CronJob.Annotations).To(BeEmpty())
			})

			It("should have nil annotations in status when not specified", func() {
				featurestore := createFeatureStore()
				resource := cronJobWithoutAnnotations(featurestore)
				services.ApplyDefaultsToStatus(resource)

				Expect(resource.Status.Applied.CronJob).NotTo(BeNil())
				Expect(resource.Status.Applied.CronJob.Annotations).To(BeNil())
			})
		})
	})
})
