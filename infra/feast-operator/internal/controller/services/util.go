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
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var isOpenShift = false

func IsLocalRegistry(featureStore *feastdevv1alpha1.FeatureStore) bool {
	appliedServices := featureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.Registry != nil && appliedServices.Registry.Local != nil
}

func isRemoteRegistry(featureStore *feastdevv1alpha1.FeatureStore) bool {
	appliedServices := featureStore.Status.Applied.Services
	return appliedServices != nil && appliedServices.Registry != nil && appliedServices.Registry.Remote != nil
}

func hasPvcConfig(featureStore *feastdevv1alpha1.FeatureStore, feastType FeastServiceType) (*feastdevv1alpha1.PvcConfig, bool) {
	var pvcConfig *feastdevv1alpha1.PvcConfig
	services := featureStore.Status.Applied.Services
	if services != nil {
		switch feastType {
		case OnlineFeastType:
			if services.OnlineStore != nil && services.OnlineStore.Persistence != nil &&
				services.OnlineStore.Persistence.FilePersistence != nil {
				pvcConfig = services.OnlineStore.Persistence.FilePersistence.PvcConfig
			}
		case OfflineFeastType:
			if services.OfflineStore != nil && services.OfflineStore.Persistence != nil &&
				services.OfflineStore.Persistence.FilePersistence != nil {
				pvcConfig = services.OfflineStore.Persistence.FilePersistence.PvcConfig
			}
		case RegistryFeastType:
			if IsLocalRegistry(featureStore) && services.Registry.Local.Persistence != nil &&
				services.Registry.Local.Persistence.FilePersistence != nil {
				pvcConfig = services.Registry.Local.Persistence.FilePersistence.PvcConfig
			}
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

func shouldMountEmptyDir(featureStore *feastdevv1alpha1.FeatureStore) bool {
	_, ok := hasPvcConfig(featureStore, OfflineFeastType)
	return !ok
}

func getOfflineMountPath(featureStore *feastdevv1alpha1.FeatureStore) string {
	if pvcConfig, ok := hasPvcConfig(featureStore, OfflineFeastType); ok {
		return pvcConfig.MountPath
	}
	return EphemeralPath
}

func ApplyDefaultsToStatus(cr *feastdevv1alpha1.FeatureStore) {
	// overwrite status.applied with every reconcile
	cr.Spec.DeepCopyInto(&cr.Status.Applied)
	cr.Status.FeastVersion = feastversion.FeastVersion

	applied := &cr.Status.Applied
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
				services.Registry.Local.Persistence.FilePersistence.Path = defaultRegistryPath(cr)
			}

			ensurePVCDefaults(services.Registry.Local.Persistence.FilePersistence.PvcConfig, RegistryFeastType)
		}

		setServiceDefaultConfigs(&services.Registry.Local.ServiceConfigs.DefaultConfigs)
	} else if services.Registry.Remote.FeastRef != nil && len(services.Registry.Remote.FeastRef.Namespace) == 0 {
		services.Registry.Remote.FeastRef.Namespace = cr.Namespace
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

			ensurePVCDefaults(services.OfflineStore.Persistence.FilePersistence.PvcConfig, OfflineFeastType)
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
				services.OnlineStore.Persistence.FilePersistence.Path = defaultOnlineStorePath(cr)
			}

			ensurePVCDefaults(services.OnlineStore.Persistence.FilePersistence.PvcConfig, OnlineFeastType)
		}

		setServiceDefaultConfigs(&services.OnlineStore.ServiceConfigs.DefaultConfigs)
	}
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

func ensurePVCDefaults(pvc *feastdevv1alpha1.PvcConfig, feastType FeastServiceType) {
	if pvc != nil {
		var storageRequest string
		switch feastType {
		case OnlineFeastType:
			storageRequest = DefaultOnlineStorageRequest
		case OfflineFeastType:
			storageRequest = DefaultOfflineStorageRequest
		case RegistryFeastType:
			storageRequest = DefaultRegistryStorageRequest
		}
		if pvc.Create != nil {
			ensureRequestedStorage(&pvc.Create.Resources, storageRequest)
			if pvc.Create.AccessModes == nil {
				pvc.Create.AccessModes = DefaultPVCAccessModes
			}
		}
	}
}

func defaultOnlineStorePath(featureStore *feastdevv1alpha1.FeatureStore) string {
	if _, ok := hasPvcConfig(featureStore, OnlineFeastType); ok {
		return DefaultOnlineStorePath
	}
	// if online pvc not set, use offline's mount path.
	return getOfflineMountPath(featureStore) + "/" + DefaultOnlineStorePath
}

func defaultRegistryPath(featureStore *feastdevv1alpha1.FeatureStore) string {
	if _, ok := hasPvcConfig(featureStore, RegistryFeastType); ok {
		return DefaultRegistryPath
	}
	// if registry pvc not set, use offline's mount path.
	return getOfflineMountPath(featureStore) + "/" + DefaultRegistryPath
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
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretRef, Namespace: feast.Handler.FeatureStore.Namespace}}
	objectKey := client.ObjectKeyFromObject(secret)
	if err := feast.Handler.Client.Get(feast.Handler.Context, objectKey, secret); err != nil {
		if apierrors.IsNotFound(err) || err != nil {
			logger := log.FromContext(feast.Handler.Context)
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

// IsOpenShift is a global flag that can be safely called across reconciliation cycles, defined at the controller manager start.
func IsOpenShift() bool {
	return isOpenShift
}

// SetIsOpenShift sets the global flag isOpenShift by the controller manager.
// We don't need to keep fetching the API every reconciliation cycle that we need to know about the platform.
func SetIsOpenShift(cfg *rest.Config) {
	if cfg == nil {
		panic("Rest Config struct is nil, impossible to get cluster information")
	}
	// adapted from https://github.com/RHsyseng/operator-utils/blob/a226fabb2226a313dd3a16591c5579ebcd8a74b0/internal/platform/platform_versioner.go#L95
	client, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		panic(fmt.Sprintf("Impossible to get new client for config when fetching cluster information: %s", err))
	}
	apiList, err := client.ServerGroups()
	if err != nil {
		panic(fmt.Sprintf("issue occurred while fetching ServerGroups: %s", err))
	}

	for _, v := range apiList.Groups {
		if v.Name == "route.openshift.io" {
			isOpenShift = true
			break
		}
	}
}

func missingOidcSecretProperty(property OidcPropertyType) error {
	return fmt.Errorf(OidcMissingSecretError, property)
}

// getEnvVar returns the position of the EnvVar found by name
func getEnvVar(envName string, env []corev1.EnvVar) int {
	for pos, v := range env {
		if v.Name == envName {
			return pos
		}
	}
	return -1
}

// envOverride replaces or appends the provided EnvVar to the collection
func envOverride(dst, src []corev1.EnvVar) []corev1.EnvVar {
	for _, cre := range src {
		pos := getEnvVar(cre.Name, dst)
		if pos != -1 {
			dst[pos] = cre
		} else {
			dst = append(dst, cre)
		}
	}
	return dst
}

func GetRegistryContainer(containers []corev1.Container) *corev1.Container {
	_, container := getContainerByType(RegistryFeastType, containers)
	return container
}

func GetOfflineContainer(containers []corev1.Container) *corev1.Container {
	_, container := getContainerByType(OfflineFeastType, containers)
	return container
}

func GetOnlineContainer(containers []corev1.Container) *corev1.Container {
	_, container := getContainerByType(OnlineFeastType, containers)
	return container
}

func getContainerByType(feastType FeastServiceType, containers []corev1.Container) (int, *corev1.Container) {
	for i, c := range containers {
		if c.Name == string(feastType) {
			return i, &c
		}
	}
	return -1, nil
}

func GetRegistryVolume(featureStore *feastdevv1alpha1.FeatureStore, volumes []corev1.Volume) *corev1.Volume {
	return getVolumeByType(RegistryFeastType, featureStore, volumes)
}

func GetOnlineVolume(featureStore *feastdevv1alpha1.FeatureStore, volumes []corev1.Volume) *corev1.Volume {
	return getVolumeByType(OnlineFeastType, featureStore, volumes)
}

func GetOfflineVolume(featureStore *feastdevv1alpha1.FeatureStore, volumes []corev1.Volume) *corev1.Volume {
	return getVolumeByType(OfflineFeastType, featureStore, volumes)
}

func getVolumeByType(feastType FeastServiceType, featureStore *feastdevv1alpha1.FeatureStore, volumes []corev1.Volume) *corev1.Volume {
	for _, v := range volumes {
		if v.Name == GetFeastServiceName(featureStore, feastType) {
			return &v
		}
	}
	return nil
}

func GetRegistryVolumeMount(featureStore *feastdevv1alpha1.FeatureStore, volumeMounts []corev1.VolumeMount) *corev1.VolumeMount {
	return getVolumeMountByType(RegistryFeastType, featureStore, volumeMounts)
}

func GetOnlineVolumeMount(featureStore *feastdevv1alpha1.FeatureStore, volumeMounts []corev1.VolumeMount) *corev1.VolumeMount {
	return getVolumeMountByType(OnlineFeastType, featureStore, volumeMounts)
}

func GetOfflineVolumeMount(featureStore *feastdevv1alpha1.FeatureStore, volumeMounts []corev1.VolumeMount) *corev1.VolumeMount {
	return getVolumeMountByType(OfflineFeastType, featureStore, volumeMounts)
}

func getVolumeMountByType(feastType FeastServiceType, featureStore *feastdevv1alpha1.FeatureStore, volumeMounts []corev1.VolumeMount) *corev1.VolumeMount {
	for _, vm := range volumeMounts {
		if vm.Name == GetFeastServiceName(featureStore, feastType) {
			return &vm
		}
	}
	return nil
}
