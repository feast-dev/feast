package services

import (
	"fmt"

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
	services := applied.Services

	// default to registry service deployment
	if services.Registry == nil {
		services.Registry = &feastdevv1alpha1.Registry{}
	}
	// if remote registry not set, proceed w/ local registry defaults
	if services.Registry.Remote == nil {
		// if local registry not set, apply an empty pointer struct
		if services.Registry.Local == nil {
			services.Registry.Local = &feastdevv1alpha1.LocalRegistryConfig{}
		}
		if services.Registry.Local.Persistence == nil {
			services.Registry.Local.Persistence = &feastdevv1alpha1.RegistryPersistence{}
		}
		if services.Registry.Local.Persistence.FilePersistence == nil {
			services.Registry.Local.Persistence.FilePersistence = &feastdevv1alpha1.RegistryFilePersistence{}
		}
		if len(services.Registry.Local.Persistence.FilePersistence.Path) == 0 {
			services.Registry.Local.Persistence.FilePersistence.Path = DefaultRegistryPath
		}
		setServiceDefaultConfigs(&services.Registry.Local.ServiceConfigs.DefaultConfigs)
	}
	if services.OfflineStore != nil {
		setServiceDefaultConfigs(&services.OfflineStore.ServiceConfigs.DefaultConfigs)
		if services.OfflineStore.Persistence == nil {
			services.OfflineStore.Persistence = &feastdevv1alpha1.OfflineStorePersistence{}
		}
		if services.OfflineStore.Persistence.FilePersistence == nil {
			services.OfflineStore.Persistence.FilePersistence = &feastdevv1alpha1.OfflineStoreFilePersistence{}
		}
		if len(services.OfflineStore.Persistence.FilePersistence.Type) == 0 {
			services.OfflineStore.Persistence.FilePersistence.Type = string(OfflineDaskConfigType)
		}
	}
	if services.OnlineStore != nil {
		setServiceDefaultConfigs(&services.OnlineStore.ServiceConfigs.DefaultConfigs)
		if services.OnlineStore.Persistence == nil {
			services.OnlineStore.Persistence = &feastdevv1alpha1.OnlineStorePersistence{}
		}
		if services.OnlineStore.Persistence.FilePersistence == nil {
			services.OnlineStore.Persistence.FilePersistence = &feastdevv1alpha1.OnlineStoreFilePersistence{}
		}
		if len(services.OnlineStore.Persistence.FilePersistence.Path) == 0 {
			services.OnlineStore.Persistence.FilePersistence.Path = DefaultOnlinePath
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

func checkOfflineStoreFilePersistenceType(value string) error {
	for _, v := range feastdevv1alpha1.ValidOfflineStoreFilePersistenceTypes {
		if v == value {
			return nil
		}
	}
	return fmt.Errorf("invalid  file type %s for offline store", value)
}
