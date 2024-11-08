package services

import (
	"fmt"
	"slices"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func isLocalRegistry(featureStore *feastdevv1alpha1.FeatureStore) bool {
	appliedServices := featureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.Registry != nil && appliedServices.Registry.Local != nil
}

func hasPvcConfig(featureStore *feastdevv1alpha1.FeatureStore, feastType FeastServiceType) (*feastdevv1alpha1.PvcConfig, bool) {
	services := featureStore.Status.Applied.Services
	var pvcConfig *feastdevv1alpha1.PvcConfig = nil
	if feastType == OnlineFeastType && services.OnlineStore != nil && services.OnlineStore.Persistence.FilePersistence != nil {
		pvcConfig = services.OnlineStore.Persistence.FilePersistence.PvcConfig
	}
	if feastType == OfflineFeastType && services.OfflineStore != nil && services.OfflineStore.Persistence.FilePersistence != nil {
		pvcConfig = services.OfflineStore.Persistence.FilePersistence.PvcConfig
	}
	if feastType == RegistryFeastType && isLocalRegistry(featureStore) && services.Registry.Local.Persistence.FilePersistence != nil {
		pvcConfig = services.Registry.Local.Persistence.FilePersistence.PvcConfig
	}
	return pvcConfig, pvcConfig != nil
}

func shouldCreatePvc(featureStore *feastdevv1alpha1.FeatureStore, feastType FeastServiceType) (*feastdevv1alpha1.PvcCreate, bool) {
	if pvcConfig, ok := hasPvcConfig(featureStore, feastType); ok {
		return pvcConfig.Create, pvcConfig.Create != nil
	}
	return nil, false
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
			services.Registry.Local.Persistence.FilePersistence.Path = defaultRegistryPath(services.Registry.Local.Persistence.FilePersistence)
		}
		if services.Registry.Local.Persistence.FilePersistence.PvcConfig != nil {
			pvc := services.Registry.Local.Persistence.FilePersistence.PvcConfig
			if pvc.Create != nil {
				ensureRequestedStorage(&pvc.Create.Resources, DefaultRegistryStorageRequest)
			}
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
		if services.OfflineStore.Persistence.FilePersistence.PvcConfig != nil {
			pvc := services.OfflineStore.Persistence.FilePersistence.PvcConfig
			if pvc.Create != nil {
				ensureRequestedStorage(&pvc.Create.Resources, DefaultOfflineStorageRequest)
			}
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
			services.OnlineStore.Persistence.FilePersistence.Path = defaultOnlineStorePath(services.OnlineStore.Persistence.FilePersistence)
		}
		if services.OnlineStore.Persistence.FilePersistence.PvcConfig != nil {
			pvc := services.OnlineStore.Persistence.FilePersistence.PvcConfig
			if pvc.Create != nil {
				ensureRequestedStorage(&pvc.Create.Resources, DefaultOnlineStorageRequest)
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

func checkOfflineStoreFilePersistenceType(value string) error {
	if slices.Contains(feastdevv1alpha1.ValidOfflineStoreFilePersistenceTypes, value) {
		return nil
	}
	return fmt.Errorf("invalid  file type %s for offline store", value)
}

func ensureRequestedStorage(resources *v1.VolumeResourceRequirements, requestedStorage string) {
	if resources.Requests == nil {
		resources.Requests = v1.ResourceList{}
	}
	if _, ok := resources.Requests[v1.ResourceStorage]; !ok {
		resources.Requests[v1.ResourceStorage] = resource.MustParse(requestedStorage)
	}
}

func defaultOnlineStorePath(persistence *feastdevv1alpha1.OnlineStoreFilePersistence) string {
	if persistence.PvcConfig == nil {
		return DefaultOnlineStoreEphemeralPath
	}
	return DefaultOnlineStorePvcPath
}
func defaultRegistryPath(persistence *feastdevv1alpha1.RegistryFilePersistence) string {
	if persistence.PvcConfig == nil {
		return DefaultRegistryEphemeralPath
	}
	return DefaultRegistryPvcPath
}
