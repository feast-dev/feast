package services

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func isLocalRegistry(featureStore *feastdevv1alpha1.FeatureStore) bool {
	appliedServices := featureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.Registry != nil && appliedServices.Registry.Local != nil
}

func hasPvcConfig(featureStore *feastdevv1alpha1.FeatureStore, feastType FeastServiceType) (*feastdevv1alpha1.PvcConfig, bool) {
	services := featureStore.Status.Applied.Services
	var pvcConfig *feastdevv1alpha1.PvcConfig = nil
	switch feastType {
	case OnlineFeastType:
		if services.OnlineStore != nil && services.OnlineStore.Persistence.FilePersistence != nil {
			pvcConfig = services.OnlineStore.Persistence.FilePersistence.PvcConfig
		}
	case OfflineFeastType:
		if services.OfflineStore != nil && services.OfflineStore.Persistence.FilePersistence != nil {
			pvcConfig = services.OfflineStore.Persistence.FilePersistence.PvcConfig
		}
	case RegistryFeastType:
		if isLocalRegistry(featureStore) && services.Registry.Local.Persistence.FilePersistence != nil {
			pvcConfig = services.Registry.Local.Persistence.FilePersistence.PvcConfig
		}
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

		if services.Registry.Local.Persistence.DBPersistence == nil {
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
		}

		setServiceDefaultConfigs(&services.Registry.Local.ServiceConfigs.DefaultConfigs)
	}
	if services.OfflineStore != nil {
		if services.OfflineStore.Persistence == nil {
			services.OfflineStore.Persistence = &feastdevv1alpha1.OfflineStorePersistence{}
		}

		if services.OfflineStore.Persistence.DBPersistence == nil {
			if services.OfflineStore.Persistence.FilePersistence == nil {
				services.OfflineStore.Persistence.FilePersistence = &feastdevv1alpha1.OfflineStoreFilePersistence{}
			}

			if len(services.OfflineStore.Persistence.FilePersistence.Type) == 0 {
				services.OfflineStore.Persistence.FilePersistence.Type = string(OfflineFilePersistenceDaskConfigType)
			}

			if services.OfflineStore.Persistence.FilePersistence.PvcConfig != nil {
				pvc := services.OfflineStore.Persistence.FilePersistence.PvcConfig
				if pvc.Create != nil {
					ensureRequestedStorage(&pvc.Create.Resources, DefaultOfflineStorageRequest)
				}
			}
		}

		setServiceDefaultConfigs(&services.OfflineStore.ServiceConfigs.DefaultConfigs)
	}

	if services.OnlineStore != nil {
		if services.OnlineStore.Persistence == nil {
			services.OnlineStore.Persistence = &feastdevv1alpha1.OnlineStorePersistence{}
		}

		if services.OnlineStore.Persistence.DBPersistence == nil {
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

		setServiceDefaultConfigs(&services.OnlineStore.ServiceConfigs.DefaultConfigs)
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
	return fmt.Errorf("invalid file type %s for offline store", value)
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

func checkOfflineStoreDBStorePersistenceType(value string) error {
	if slices.Contains(feastdevv1alpha1.ValidOfflineStoreDBStorePersistenceTypes, value) {
		return nil
	}
	return fmt.Errorf("invalid DB store type %s for offline store", value)
}

func checkOnlineStoreDBStorePersistenceType(value string) error {
	if slices.Contains(feastdevv1alpha1.ValidOnlineStoreDBStorePersistenceTypes, value) {
		return nil
	}
	return fmt.Errorf("invalid DB store type %s for online store", value)
}

func checkRegistryDBStorePersistenceType(value string) error {
	if slices.Contains(feastdevv1alpha1.ValidRegistryDBStorePersistenceTypes, value) {
		return nil
	}
	return fmt.Errorf("invalid DB store type %s for registry", value)
}

func (feast *FeastServices) getSecret(secretRef string) (*corev1.Secret, error) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretRef, Namespace: feast.FeatureStore.Namespace}}
	objectKey := client.ObjectKeyFromObject(secret)
	if err := feast.Client.Get(feast.Context, objectKey, secret); err != nil {
		if apierrors.IsNotFound(err) || err != nil {
			logger := log.FromContext(feast.Context)
			logger.Error(err, "invalid secret "+secretRef+" for offline store")

			return nil, err
		}
	}

	return secret, nil
}

// Function to check if a struct has a specific field or field tag and sets the value in the field if empty
func hasAttrib(s interface{}, fieldName string, value interface{}) (bool, error) {
	val := reflect.ValueOf(s)

	// Check that the object is a pointer so we can modify it
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return false, fmt.Errorf("expected a pointer to struct, got %v", val.Kind())
	}

	val = val.Elem()

	// Loop through the fields and check the tag
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := val.Type().Field(i)

		tagVal := fieldType.Tag.Get("yaml")

		// Remove other metadata if exists
		commaIndex := strings.Index(tagVal, ",")

		if commaIndex != -1 {
			tagVal = tagVal[:commaIndex]
		}

		// Check if the field name or the tag value matches the one we're looking for
		if strings.EqualFold(fieldType.Name, fieldName) || strings.EqualFold(tagVal, fieldName) {

			// Ensure the field is settable
			if !field.CanSet() {
				return false, fmt.Errorf("cannot set field %s", fieldName)
			}

			// Check if the field is empty (zero value)
			if field.IsZero() {
				// Set the field value only if it's empty
				field.Set(reflect.ValueOf(value))
			}

			return true, nil
		}
	}

	return false, nil
}

func CopyMap(original map[string]interface{}) map[string]interface{} {
	// Create a new map to store the copy
	newCopy := make(map[string]interface{})

	// Loop through the original map and copy each key-value pair
	for key, value := range original {
		newCopy[key] = value
	}

	return newCopy
}
