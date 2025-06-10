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

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Feast phases:
	ReadyPhase   = "Ready"
	PendingPhase = "Pending"
	FailedPhase  = "Failed"

	// Feast condition types:
	ClientReadyType        = "Client"
	OfflineStoreReadyType  = "OfflineStore"
	OnlineStoreReadyType   = "OnlineStore"
	RegistryReadyType      = "Registry"
	UIReadyType            = "UI"
	ReadyType              = "FeatureStore"
	AuthorizationReadyType = "Authorization"
	CronJobReadyType       = "CronJob"

	// Feast condition reasons:
	ReadyReason                  = "Ready"
	FailedReason                 = "FeatureStoreFailed"
	DeploymentNotAvailableReason = "DeploymentNotAvailable"
	OfflineStoreFailedReason     = "OfflineStoreDeploymentFailed"
	OnlineStoreFailedReason      = "OnlineStoreDeploymentFailed"
	RegistryFailedReason         = "RegistryDeploymentFailed"
	UIFailedReason               = "UIDeploymentFailed"
	ClientFailedReason           = "ClientDeploymentFailed"
	CronJobFailedReason          = "CronJobDeploymentFailed"
	KubernetesAuthzFailedReason  = "KubernetesAuthorizationDeploymentFailed"

	// Feast condition messages:
	ReadyMessage                  = "FeatureStore installation complete"
	OfflineStoreReadyMessage      = "Offline Store installation complete"
	OnlineStoreReadyMessage       = "Online Store installation complete"
	RegistryReadyMessage          = "Registry installation complete"
	UIReadyMessage                = "UI installation complete"
	ClientReadyMessage            = "Client installation complete"
	CronJobReadyMessage           = "CronJob installation complete"
	KubernetesAuthzReadyMessage   = "Kubernetes authorization installation complete"
	DeploymentNotAvailableMessage = "Deployment is not available"

	// entity_key_serialization_version
	SerializationVersion = 3
)

// FeatureStoreSpec defines the desired state of FeatureStore
type FeatureStoreSpec struct {
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9][A-Za-z0-9_]*$"
	// FeastProject is the Feast project id. This can be any alphanumeric string with underscores, but it cannot start with an underscore. Required.
	FeastProject    string                `json:"feastProject"`
	FeastProjectDir *FeastProjectDir      `json:"feastProjectDir,omitempty"`
	Services        *FeatureStoreServices `json:"services,omitempty"`
	AuthzConfig     *AuthzConfig          `json:"authz,omitempty"`
	CronJob         *FeastCronJob         `json:"cronJob,omitempty"`
}

// FeastProjectDir defines how to create the feast project directory.
// +kubebuilder:validation:XValidation:rule="[has(self.git), has(self.init)].exists_one(c, c)",message="One selection required between init or git."
type FeastProjectDir struct {
	Git  *GitCloneOptions  `json:"git,omitempty"`
	Init *FeastInitOptions `json:"init,omitempty"`
}

// GitCloneOptions describes how a clone should be performed.
// +kubebuilder:validation:XValidation:rule="has(self.featureRepoPath) ? !self.featureRepoPath.startsWith('/') : true",message="RepoPath must be a file name only, with no slashes."
type GitCloneOptions struct {
	// The repository URL to clone from.
	URL string `json:"url"`
	// Reference to a branch / tag / commit
	Ref string `json:"ref,omitempty"`
	// Configs passed to git via `-c`
	// e.g. http.sslVerify: 'false'
	// OR 'url."https://api:\${TOKEN}@github.com/".insteadOf': 'https://github.com/'
	Configs map[string]string `json:"configs,omitempty"`
	// FeatureRepoPath is the relative path to the feature repo subdirectory. Default is 'feature_repo'.
	FeatureRepoPath string                  `json:"featureRepoPath,omitempty"`
	Env             *[]corev1.EnvVar        `json:"env,omitempty"`
	EnvFrom         *[]corev1.EnvFromSource `json:"envFrom,omitempty"`
}

// FeastInitOptions defines how to run a `feast init`.
type FeastInitOptions struct {
	Minimal bool `json:"minimal,omitempty"`
	// Template for the created project
	// +kubebuilder:validation:Enum=local;gcp;aws;snowflake;spark;postgres;hbase;cassandra;hazelcast;ikv;couchbase;clickhouse
	Template string `json:"template,omitempty"`
}

// FeastCronJob defines a CronJob to execute against a Feature Store deployment.
type FeastCronJob struct {
	// Specification of the desired behavior of a job.
	JobSpec          *JobSpec                 `json:"jobSpec,omitempty"`
	ContainerConfigs *CronJobContainerConfigs `json:"containerConfigs,omitempty"`

	// The schedule in Cron format, see https://en.wikipedia.org/wiki/Cron.
	Schedule string `json:"schedule,omitempty"`

	// The time zone name for the given schedule, see https://en.wikipedia.org/wiki/List_of_tz_database_time_zones.
	// If not specified, this will default to the time zone of the kube-controller-manager process.
	// The set of valid time zone names and the time zone offset is loaded from the system-wide time zone
	// database by the API server during CronJob validation and the controller manager during execution.
	// If no system-wide time zone database can be found a bundled version of the database is used instead.
	// If the time zone name becomes invalid during the lifetime of a CronJob or due to a change in host
	// configuration, the controller will stop creating new new Jobs and will create a system event with the
	// reason UnknownTimeZone.
	// More information can be found in https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#time-zones
	TimeZone *string `json:"timeZone,omitempty"`

	// Optional deadline in seconds for starting the job if it misses scheduled
	// time for any reason.  Missed jobs executions will be counted as failed ones.
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// Specifies how to treat concurrent executions of a Job.
	// Valid values are:
	//
	// - "Allow" (default): allows CronJobs to run concurrently;
	// - "Forbid": forbids concurrent runs, skipping next run if previous run hasn't finished yet;
	// - "Replace": cancels currently running job and replaces it with a new one
	ConcurrencyPolicy batchv1.ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// This flag tells the controller to suspend subsequent executions, it does
	// not apply to already started executions.
	Suspend *bool `json:"suspend,omitempty"`

	// The number of successful finished jobs to retain. Value must be non-negative integer.
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// The number of failed finished jobs to retain. Value must be non-negative integer.
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
}

// JobSpec describes how the job execution will look like.
type JobSpec struct {
	// Specifies the maximum desired number of pods the job should
	// run at any given time. The actual number of pods running in steady state will
	// be less than this number when ((.spec.completions - .status.successful) < .spec.parallelism),
	// i.e. when the work left to do is less than max parallelism.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
	Parallelism *int32 `json:"parallelism,omitempty"`

	// Specifies the desired number of successfully finished pods the
	// job should be run with.  Setting to null means that the success of any
	// pod signals the success of all pods, and allows parallelism to have any positive
	// value.  Setting to 1 means that parallelism is limited to 1 and the success of that
	// pod signals the success of the job.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
	Completions *int32 `json:"completions,omitempty"`

	// Specifies the duration in seconds relative to the startTime that the job
	// may be continuously active before the system tries to terminate it; value
	// must be positive integer. If a Job is suspended (at creation or through an
	// update), this timer will effectively be stopped and reset when the Job is
	// resumed again.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// Specifies the policy of handling failed pods. In particular, it allows to
	// specify the set of actions and conditions which need to be
	// satisfied to take the associated action.
	// If empty, the default behaviour applies - the counter of failed pods,
	// represented by the jobs's .status.failed field, is incremented and it is
	// checked against the backoffLimit. This field cannot be used in combination
	// with restartPolicy=OnFailure.
	//
	// This field is beta-level. It can be used when the `JobPodFailurePolicy`
	// feature gate is enabled (enabled by default).
	PodFailurePolicy *batchv1.PodFailurePolicy `json:"podFailurePolicy,omitempty"`

	// Specifies the number of retries before marking this job failed.
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// Specifies the limit for the number of retries within an
	// index before marking this index as failed. When enabled the number of
	// failures per index is kept in the pod's
	// batch.kubernetes.io/job-index-failure-count annotation. It can only
	// be set when Job's completionMode=Indexed, and the Pod's restart
	// policy is Never. The field is immutable.
	// This field is beta-level. It can be used when the `JobBackoffLimitPerIndex`
	// feature gate is enabled (enabled by default).
	BackoffLimitPerIndex *int32 `json:"backoffLimitPerIndex,omitempty"`

	// Specifies the maximal number of failed indexes before marking the Job as
	// failed, when backoffLimitPerIndex is set. Once the number of failed
	// indexes exceeds this number the entire Job is marked as Failed and its
	// execution is terminated. When left as null the job continues execution of
	// all of its indexes and is marked with the `Complete` Job condition.
	// It can only be specified when backoffLimitPerIndex is set.
	// It can be null or up to completions. It is required and must be
	// less than or equal to 10^4 when is completions greater than 10^5.
	// This field is beta-level. It can be used when the `JobBackoffLimitPerIndex`
	// feature gate is enabled (enabled by default).
	MaxFailedIndexes *int32 `json:"maxFailedIndexes,omitempty"`

	// ttlSecondsAfterFinished limits the lifetime of a Job that has finished
	// execution (either Complete or Failed). If this field is set,
	// ttlSecondsAfterFinished after the Job finishes, it is eligible to be
	// automatically deleted. When the Job is being deleted, its lifecycle
	// guarantees (e.g. finalizers) will be honored. If this field is unset,
	// the Job won't be automatically deleted. If this field is set to zero,
	// the Job becomes eligible to be deleted immediately after it finishes.
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// completionMode specifies how Pod completions are tracked. It can be
	// `NonIndexed` (default) or `Indexed`.
	//
	// `NonIndexed` means that the Job is considered complete when there have
	// been .spec.completions successfully completed Pods. Each Pod completion is
	// homologous to each other.
	//
	// `Indexed` means that the Pods of a
	// Job get an associated completion index from 0 to (.spec.completions - 1),
	// available in the annotation batch.kubernetes.io/job-completion-index.
	// The Job is considered complete when there is one successfully completed Pod
	// for each index.
	// When value is `Indexed`, .spec.completions must be specified and
	// `.spec.parallelism` must be less than or equal to 10^5.
	// In addition, The Pod name takes the form
	// `$(job-name)-$(index)-$(random-string)`,
	// the Pod hostname takes the form `$(job-name)-$(index)`.
	//
	// More completion modes can be added in the future.
	// If the Job controller observes a mode that it doesn't recognize, which
	// is possible during upgrades due to version skew, the controller
	// skips updates for the Job.
	CompletionMode *batchv1.CompletionMode `json:"completionMode,omitempty"`

	// suspend specifies whether the Job controller should create Pods or not. If
	// a Job is created with suspend set to true, no Pods are created by the Job
	// controller. If a Job is suspended after creation (i.e. the flag goes from
	// false to true), the Job controller will delete all active Pods associated
	// with this Job. Users must design their workload to gracefully handle this.
	// Suspending a Job will reset the StartTime field of the Job, effectively
	// resetting the ActiveDeadlineSeconds timer too.
	//
	Suspend *bool `json:"suspend,omitempty"`

	// podReplacementPolicy specifies when to create replacement Pods.
	// Possible values are:
	// - TerminatingOrFailed means that we recreate pods
	//   when they are terminating (has a metadata.deletionTimestamp) or failed.
	// - Failed means to wait until a previously created Pod is fully terminated (has phase
	//   Failed or Succeeded) before creating a replacement Pod.
	//
	// When using podFailurePolicy, Failed is the the only allowed value.
	// TerminatingOrFailed and Failed are allowed values when podFailurePolicy is not in use.
	// This is an beta field. To use this, enable the JobPodReplacementPolicy feature toggle.
	// This is on by default.
	PodReplacementPolicy *batchv1.PodReplacementPolicy `json:"podReplacementPolicy,omitempty"`
}

// FeatureStoreServices defines the desired feast services. An ephemeral onlineStore feature server is deployed by default.
type FeatureStoreServices struct {
	OfflineStore *OfflineStore `json:"offlineStore,omitempty"`
	OnlineStore  *OnlineStore  `json:"onlineStore,omitempty"`
	Registry     *Registry     `json:"registry,omitempty"`
	// Creates a UI server container
	UI                 *ServerConfigs             `json:"ui,omitempty"`
	DeploymentStrategy *appsv1.DeploymentStrategy `json:"deploymentStrategy,omitempty"`
	SecurityContext    *corev1.PodSecurityContext `json:"securityContext,omitempty"`
	// Disable the 'feast repo initialization' initContainer
	DisableInitContainers bool `json:"disableInitContainers,omitempty"`
	// Volumes specifies the volumes to mount in the FeatureStore deployment. A corresponding `VolumeMount` should be added to whichever feast service(s) require access to said volume(s).
	Volumes []corev1.Volume `json:"volumes,omitempty"`
}

// OfflineStore configures the offline store service
type OfflineStore struct {
	// Creates a remote offline server container
	Server      *ServerConfigs           `json:"server,omitempty"`
	Persistence *OfflineStorePersistence `json:"persistence,omitempty"`
}

// OfflineStorePersistence configures the persistence settings for the offline store service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type OfflineStorePersistence struct {
	FilePersistence *OfflineStoreFilePersistence    `json:"file,omitempty"`
	DBPersistence   *OfflineStoreDBStorePersistence `json:"store,omitempty"`
}

// OfflineStoreFilePersistence configures the file-based persistence for the offline store service
type OfflineStoreFilePersistence struct {
	// +kubebuilder:validation:Enum=file;dask;duckdb
	Type      string     `json:"type,omitempty"`
	PvcConfig *PvcConfig `json:"pvc,omitempty"`
}

var ValidOfflineStoreFilePersistenceTypes = []string{
	"dask",
	"duckdb",
	"file",
}

// OfflineStoreDBStorePersistence configures the DB store persistence for the offline store service
type OfflineStoreDBStorePersistence struct {
	// Type of the persistence type you want to use.
	// +kubebuilder:validation:Enum=snowflake.offline;bigquery;redshift;spark;postgres;trino;athena;mssql;couchbase.offline;clickhouse
	Type string `json:"type"`
	// Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// By default, the selected store "type" is used as the SecretKeyName
	SecretKeyName string `json:"secretKeyName,omitempty"`
}

var ValidOfflineStoreDBStorePersistenceTypes = []string{
	"snowflake.offline",
	"bigquery",
	"redshift",
	"spark",
	"postgres",
	"trino",
	"athena",
	"mssql",
	"couchbase.offline",
	"clickhouse",
}

// OnlineStore configures the online store service
type OnlineStore struct {
	// Creates a feature server container
	Server      *ServerConfigs          `json:"server,omitempty"`
	Persistence *OnlineStorePersistence `json:"persistence,omitempty"`
}

// OnlineStorePersistence configures the persistence settings for the online store service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type OnlineStorePersistence struct {
	FilePersistence *OnlineStoreFilePersistence    `json:"file,omitempty"`
	DBPersistence   *OnlineStoreDBStorePersistence `json:"store,omitempty"`
}

// OnlineStoreFilePersistence configures the file-based persistence for the online store service
// +kubebuilder:validation:XValidation:rule="(!has(self.pvc) && has(self.path)) ? self.path.startsWith('/') : true",message="Ephemeral stores must have absolute paths."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !self.path.startsWith('/') : true",message="PVC path must be a file name only, with no slashes."
// +kubebuilder:validation:XValidation:rule="has(self.path) ? !(self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="Online store does not support S3 or GS buckets."
type OnlineStoreFilePersistence struct {
	Path      string     `json:"path,omitempty"`
	PvcConfig *PvcConfig `json:"pvc,omitempty"`
}

// OnlineStoreDBStorePersistence configures the DB store persistence for the online store service
type OnlineStoreDBStorePersistence struct {
	// Type of the persistence type you want to use.
	// +kubebuilder:validation:Enum=snowflake.online;redis;ikv;datastore;dynamodb;bigtable;postgres;cassandra;mysql;hazelcast;singlestore;hbase;elasticsearch;qdrant;couchbase.online;milvus
	Type string `json:"type"`
	// Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// By default, the selected store "type" is used as the SecretKeyName
	SecretKeyName string `json:"secretKeyName,omitempty"`
}

var ValidOnlineStoreDBStorePersistenceTypes = []string{
	"snowflake.online",
	"redis",
	"ikv",
	"datastore",
	"dynamodb",
	"bigtable",
	"postgres",
	"cassandra",
	"mysql",
	"hazelcast",
	"singlestore",
	"hbase",
	"elasticsearch",
	"qdrant",
	"couchbase.online",
	"milvus",
}

// LocalRegistryConfig configures the registry service
type LocalRegistryConfig struct {
	// Creates a registry server container
	Server      *RegistryServerConfigs `json:"server,omitempty"`
	Persistence *RegistryPersistence   `json:"persistence,omitempty"`
}

// RegistryPersistence configures the persistence settings for the registry service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type RegistryPersistence struct {
	FilePersistence *RegistryFilePersistence    `json:"file,omitempty"`
	DBPersistence   *RegistryDBStorePersistence `json:"store,omitempty"`
}

// RegistryFilePersistence configures the file-based persistence for the registry service
// +kubebuilder:validation:XValidation:rule="(!has(self.pvc) && has(self.path)) ? (self.path.startsWith('/') || self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="Registry files must use absolute paths or be S3 ('s3://') or GS ('gs://') object store URIs."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !self.path.startsWith('/') : true",message="PVC path must be a file name only, with no slashes."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !(self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="PVC persistence does not support S3 or GS object store URIs."
// +kubebuilder:validation:XValidation:rule="(has(self.s3_additional_kwargs) && has(self.path)) ? self.path.startsWith('s3://') : true",message="Additional S3 settings are available only for S3 object store URIs."
type RegistryFilePersistence struct {
	Path               string             `json:"path,omitempty"`
	PvcConfig          *PvcConfig         `json:"pvc,omitempty"`
	S3AdditionalKwargs *map[string]string `json:"s3_additional_kwargs,omitempty"`
}

// RegistryDBStorePersistence configures the DB store persistence for the registry service
type RegistryDBStorePersistence struct {
	// Type of the persistence type you want to use.
	// +kubebuilder:validation:Enum=sql;snowflake.registry
	Type string `json:"type"`
	// Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// By default, the selected store "type" is used as the SecretKeyName
	SecretKeyName string `json:"secretKeyName,omitempty"`
}

var ValidRegistryDBStorePersistenceTypes = []string{
	"sql",
	"snowflake.registry",
}

// PvcConfig defines the settings for a persistent file store based on PVCs.
// We can refer to an existing PVC using the `Ref` field, or create a new one using the `Create` field.
// +kubebuilder:validation:XValidation:rule="[has(self.ref), has(self.create)].exists_one(c, c)",message="One selection is required between ref and create."
// +kubebuilder:validation:XValidation:rule="self.mountPath.matches('^/[^:]*$')",message="Mount path must start with '/' and must not contain ':'"
type PvcConfig struct {
	// Reference to an existing field
	Ref *corev1.LocalObjectReference `json:"ref,omitempty"`
	// Settings for creating a new PVC
	Create *PvcCreate `json:"create,omitempty"`
	// MountPath within the container at which the volume should be mounted.
	// Must start by "/" and cannot contain ':'.
	MountPath string `json:"mountPath"`
}

// PvcCreate defines the immutable settings to create a new PVC mounted at the given path.
// The PVC name is the same as the associated deployment & feast service name.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="PvcCreate is immutable"
type PvcCreate struct {
	// AccessModes k8s persistent volume access modes. Defaults to ["ReadWriteOnce"].
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
	// StorageClassName is the name of an existing StorageClass to which this persistent volume belongs. Empty value
	// means that this volume does not belong to any StorageClass and the cluster default will be used.
	StorageClassName *string `json:"storageClassName,omitempty"`
	// Resources describes the storage resource requirements for a volume.
	// Default requested storage size depends on the associated service:
	// - 10Gi for offline store
	// - 5Gi for online store
	// - 5Gi for registry
	Resources corev1.VolumeResourceRequirements `json:"resources,omitempty"`
}

// Registry configures the registry service. One selection is required. Local is the default setting.
// +kubebuilder:validation:XValidation:rule="[has(self.local), has(self.remote)].exists_one(c, c)",message="One selection required."
type Registry struct {
	Local  *LocalRegistryConfig  `json:"local,omitempty"`
	Remote *RemoteRegistryConfig `json:"remote,omitempty"`
}

// RemoteRegistryConfig points to a remote feast registry server. When set, the operator will not deploy a registry for this FeatureStore CR.
// Instead, this FeatureStore CR's online/offline services will use a remote registry. One selection is required.
// +kubebuilder:validation:XValidation:rule="[has(self.hostname), has(self.feastRef)].exists_one(c, c)",message="One selection required."
type RemoteRegistryConfig struct {
	// Host address of the remote registry service - <domain>:<port>, e.g. `registry.<namespace>.svc.cluster.local:80`
	Hostname *string `json:"hostname,omitempty"`
	// Reference to an existing `FeatureStore` CR in the same k8s cluster.
	FeastRef *FeatureStoreRef          `json:"feastRef,omitempty"`
	TLS      *TlsRemoteRegistryConfigs `json:"tls,omitempty"`
}

// FeatureStoreRef defines which existing FeatureStore's registry should be used
type FeatureStoreRef struct {
	// Name of the FeatureStore
	Name string `json:"name"`
	// Namespace of the FeatureStore
	Namespace string `json:"namespace,omitempty"`
}

// ServerConfigs creates a server for the feast service, with specified container configurations.
type ServerConfigs struct {
	ContainerConfigs `json:",inline"`
	TLS              *TlsConfigs `json:"tls,omitempty"`
	// LogLevel sets the logging level for the server
	// Allowed values: "debug", "info", "warning", "error", "critical".
	// +kubebuilder:validation:Enum=debug;info;warning;error;critical
	LogLevel *string `json:"logLevel,omitempty"`
	// VolumeMounts defines the list of volumes that should be mounted into the feast container.
	// This allows attaching persistent storage, config files, secrets, or other resources
	// required by the Feast components. Ensure that each volume mount has a corresponding
	// volume definition in the Volumes field.
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
}

// RegistryServerConfigs creates a registry server for the feast service, with specified container configurations.
type RegistryServerConfigs struct {
	ServerConfigs `json:",inline"`

	// Enable REST API registry server. Defaults to false if unset.
	RestAPI *bool `json:"restAPI,omitempty"`

	// Enable gRPC registry server. Defaults to true if unset.
	GRPC *bool `json:"grpc,omitempty"`
}

// CronJobContainerConfigs k8s container settings for the CronJob
type CronJobContainerConfigs struct {
	ContainerConfigs `json:",inline"`

	// Array of commands to be executed (in order) against a Feature Store deployment.
	// Defaults to "feast apply" & "feast materialize-incremental $(date -u +'%Y-%m-%dT%H:%M:%S')"
	Commands []string `json:"commands,omitempty"`
}

// ContainerConfigs k8s container settings for the server
type ContainerConfigs struct {
	DefaultCtrConfigs  `json:",inline"`
	OptionalCtrConfigs `json:",inline"`
}

// DefaultCtrConfigs k8s container settings that are applied by default
type DefaultCtrConfigs struct {
	Image *string `json:"image,omitempty"`
}

// OptionalCtrConfigs k8s container settings that are optional
type OptionalCtrConfigs struct {
	Env             *[]corev1.EnvVar             `json:"env,omitempty"`
	EnvFrom         *[]corev1.EnvFromSource      `json:"envFrom,omitempty"`
	ImagePullPolicy *corev1.PullPolicy           `json:"imagePullPolicy,omitempty"`
	Resources       *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// AuthzConfig defines the authorization settings for the deployed Feast services.
// +kubebuilder:validation:XValidation:rule="[has(self.kubernetes), has(self.oidc)].exists_one(c, c)",message="One selection required between kubernetes or oidc."
type AuthzConfig struct {
	KubernetesAuthz *KubernetesAuthz `json:"kubernetes,omitempty"`
	OidcAuthz       *OidcAuthz       `json:"oidc,omitempty"`
}

// KubernetesAuthz provides a way to define the authorization settings using Kubernetes RBAC resources.
// https://kubernetes.io/docs/reference/access-authn-authz/rbac/
type KubernetesAuthz struct {
	// The Kubernetes RBAC roles to be deployed in the same namespace of the FeatureStore.
	// Roles are managed by the operator and created with an empty list of rules.
	// See the Feast permission model at https://docs.feast.dev/getting-started/concepts/permission
	// The feature store admin is not obligated to manage roles using the Feast operator, roles can be managed independently.
	// This configuration option is only providing a way to automate this procedure.
	// Important note: the operator cannot ensure that these roles will match the ones used in the configured Feast permissions.
	Roles []string `json:"roles,omitempty"`
}

// OidcAuthz defines the authorization settings for deployments using an Open ID Connect identity provider.
// https://auth0.com/docs/authenticate/protocols/openid-connect-protocol
type OidcAuthz struct {
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// TlsConfigs configures server TLS for a feast service. in an openshift cluster, this is configured by default using service serving certificates.
// +kubebuilder:validation:XValidation:rule="(!has(self.disable) || !self.disable) ? has(self.secretRef) : true",message="`secretRef` required if `disable` is false."
type TlsConfigs struct {
	// references the local k8s secret where the TLS key and cert reside
	SecretRef      *corev1.LocalObjectReference `json:"secretRef,omitempty"`
	SecretKeyNames SecretKeyNames               `json:"secretKeyNames,omitempty"`
	// will disable TLS for the feast service. useful in an openshift cluster, for example, where TLS is configured by default
	Disable *bool `json:"disable,omitempty"`
}

// `secretRef` required if `disable` is false.
func (tls *TlsConfigs) IsTLS() bool {
	if tls != nil {
		if tls.Disable != nil && *tls.Disable {
			return false
		} else if tls.SecretRef == nil {
			return false
		}
		return true
	}
	return false
}

// TlsRemoteRegistryConfigs configures client TLS for a remote feast registry. in an openshift cluster, this is configured by default when the remote feast registry is using service serving certificates.
type TlsRemoteRegistryConfigs struct {
	// references the local k8s configmap where the TLS cert resides
	ConfigMapRef corev1.LocalObjectReference `json:"configMapRef"`
	// defines the configmap key name for the client TLS cert.
	CertName string `json:"certName"`
}

// SecretKeyNames defines the secret key names for the TLS key and cert.
type SecretKeyNames struct {
	// defaults to "tls.crt"
	TlsCrt string `json:"tlsCrt,omitempty"`
	// defaults to "tls.key"
	TlsKey string `json:"tlsKey,omitempty"`
}

// FeatureStoreStatus defines the observed state of FeatureStore
type FeatureStoreStatus struct {
	// Shows the currently applied feast configuration, including any pertinent defaults
	Applied FeatureStoreSpec `json:"applied,omitempty"`
	// ConfigMap in this namespace containing a client `feature_store.yaml` for this feast deployment
	ClientConfigMap string `json:"clientConfigMap,omitempty"`
	// CronJob in this namespace for this feast deployment
	CronJob          string             `json:"cronJob,omitempty"`
	Conditions       []metav1.Condition `json:"conditions,omitempty"`
	FeastVersion     string             `json:"feastVersion,omitempty"`
	Phase            string             `json:"phase,omitempty"`
	ServiceHostnames ServiceHostnames   `json:"serviceHostnames,omitempty"`
}

// ServiceHostnames defines the service hostnames in the format of <domain>:<port>, e.g. example.svc.cluster.local:80
type ServiceHostnames struct {
	OfflineStore string `json:"offlineStore,omitempty"`
	OnlineStore  string `json:"onlineStore,omitempty"`
	Registry     string `json:"registry,omitempty"`
	UI           string `json:"ui,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=feast
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// FeatureStore is the Schema for the featurestores API
type FeatureStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FeatureStoreSpec   `json:"spec,omitempty"`
	Status FeatureStoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// FeatureStoreList contains a list of FeatureStore
type FeatureStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FeatureStore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FeatureStore{}, &FeatureStoreList{})
}
