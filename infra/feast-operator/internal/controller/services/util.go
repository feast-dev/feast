package services

import (
	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
)

func IsLocalRegistry(featureStore *feastdevv1alpha1.FeatureStore) bool {
	appliedServices := featureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.Registry != nil && appliedServices.Registry.Local != nil
}

func ApplyDefaultsToStatus(cr *feastdevv1alpha1.FeatureStore) {
	cr.Status.FeastVersion = feastversion.FeastVersion
	applied := cr.Spec.DeepCopy()
	if applied.Services == nil {
		applied.Services = &feastdevv1alpha1.FeatureStoreServices{}
	}

	// default to registry service deployment
	if applied.Services.Registry == nil {
		applied.Services.Registry = &feastdevv1alpha1.Registry{
			Local: &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path: DefaultRegistryPath,
					},
				},
			},
		}
	}
	// if remote registry not set, proceed w/ local registry defaults
	if applied.Services.Registry.Remote == nil {
		// if local registry not set, apply an empty pointer struct
		if applied.Services.Registry.Local == nil {
			applied.Services.Registry.Local = &feastdevv1alpha1.LocalRegistryConfig{
				Persistence: &feastdevv1alpha1.RegistryPersistence{
					FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
						Path: DefaultRegistryPath,
					},
				},
			}
		}
		if applied.Services.Registry.Local.Persistence == nil {
			applied.Services.Registry.Local.Persistence = &feastdevv1alpha1.RegistryPersistence{
				FilePersistence: &feastdevv1alpha1.RegistryFilePersistence{
					Path: DefaultRegistryPath,
				},
			}
		} else if applied.Services.Registry.Local.Persistence.FilePersistence == nil {
			applied.Services.Registry.Local.Persistence.FilePersistence = &feastdevv1alpha1.RegistryFilePersistence{
				Path: DefaultRegistryPath,
			}
		}
		setServiceDefaultConfigs(&applied.Services.Registry.Local.ServiceConfigs.DefaultConfigs)
	}
	if applied.Services.OfflineStore != nil {
		setServiceDefaultConfigs(&applied.Services.OfflineStore.ServiceConfigs.DefaultConfigs)
		if applied.Services.OfflineStore.Persistence == nil {
			applied.Services.OfflineStore.Persistence = &feastdevv1alpha1.OfflineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OfflineStoreFilePersistence{
					Type: string(OfflineDaskConfigType),
				},
			}
		} else if applied.Services.OfflineStore.Persistence.FilePersistence == nil {
			applied.Services.OfflineStore.Persistence.FilePersistence = &feastdevv1alpha1.OfflineStoreFilePersistence{
				Type: string(OfflineDaskConfigType),
			}
		}
	}
	if applied.Services.OnlineStore != nil {
		setServiceDefaultConfigs(&applied.Services.OnlineStore.ServiceConfigs.DefaultConfigs)
		if applied.Services.OnlineStore.Persistence == nil {
			applied.Services.OnlineStore.Persistence = &feastdevv1alpha1.OnlineStorePersistence{
				FilePersistence: &feastdevv1alpha1.OnlineStoreFilePersistence{
					Path: DefaultOnlinePath,
				},
			}
		} else if applied.Services.OnlineStore.Persistence.FilePersistence == nil {
			applied.Services.OnlineStore.Persistence.FilePersistence = &feastdevv1alpha1.OnlineStoreFilePersistence{
				Path: DefaultOnlinePath,
			}
		}
	}

	// overwrite status.applied with every reconcile
	applied.DeepCopyInto(&cr.Status.Applied)
}

func setServiceDefaultConfigs(defaultConfigs *feastdevv1alpha1.DefaultConfigs) {
	if defaultConfigs.Image == nil {
		defaultConfigs.Image = &DefaultImage
	}
}
