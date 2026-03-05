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

package v1

import (
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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
// +kubebuilder:validation:XValidation:rule="self.replicas <= 1 || !has(self.services) || !has(self.services.scaling) || !has(self.services.scaling.autoscaling)",message="replicas > 1 and services.scaling.autoscaling are mutually exclusive."
// +kubebuilder:validation:XValidation:rule="self.replicas <= 1 && (!has(self.services) || !has(self.services.scaling) || !has(self.services.scaling.autoscaling)) || (has(self.services) && has(self.services.onlineStore) && has(self.services.onlineStore.persistence) && has(self.services.onlineStore.persistence.store))",message="Scaling requires DB-backed persistence for the online store. Configure services.onlineStore.persistence.store when using replicas > 1 or autoscaling."
// +kubebuilder:validation:XValidation:rule="self.replicas <= 1 && (!has(self.services) || !has(self.services.scaling) || !has(self.services.scaling.autoscaling)) || (!has(self.services) || !has(self.services.offlineStore) || (has(self.services.offlineStore.persistence) && has(self.services.offlineStore.persistence.store)))",message="Scaling requires DB-backed persistence for the offline store. Configure services.offlineStore.persistence.store when using replicas > 1 or autoscaling."
// +kubebuilder:validation:XValidation:rule="self.replicas <= 1 && (!has(self.services) || !has(self.services.scaling) || !has(self.services.scaling.autoscaling)) || (has(self.services) && has(self.services.registry) && (has(self.services.registry.remote) || (has(self.services.registry.local) && has(self.services.registry.local.persistence) && (has(self.services.registry.local.persistence.store) || (has(self.services.registry.local.persistence.file) && has(self.services.registry.local.persistence.file.path) && (self.services.registry.local.persistence.file.path.startsWith('s3://') || self.services.registry.local.persistence.file.path.startsWith('gs://')))))))",message="Scaling requires DB-backed or remote registry. Configure registry.local.persistence.store or use a remote registry when using replicas > 1 or autoscaling. S3/GCS-backed registry is also allowed."
type FeatureStoreSpec struct {
	// feastProject is the Feast project id. This can be any alphanumeric string with underscores and hyphens, but it cannot start with an underscore or hyphen. Required.
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9][A-Za-z0-9_-]*$"
	// +required
	FeastProject string `json:"feastProject"`
	// feastProjectDir defines how to create the feast project directory.
	// +optional
	FeastProjectDir *FeastProjectDir `json:"feastProjectDir,omitempty"`
	// services configures the Feast services for this FeatureStore.
	// +optional
	Services *FeatureStoreServices `json:"services,omitempty"`
	// authz configures authorization for Feast services.
	// +optional
	AuthzConfig *AuthzConfig `json:"authz,omitempty"`
	// cronJob configures CronJobs to run against this FeatureStore deployment.
	// +optional
	CronJob *FeastCronJob `json:"cronJob,omitempty"`
	// batchEngine configures the batch compute engine settings.
	// +optional
	BatchEngine *BatchEngineConfig `json:"batchEngine,omitempty"`
	// replicas is the desired number of pod replicas. Used by the scale sub-resource.
	// Mutually exclusive with services.scaling.autoscaling.
	// +default=1
	// +optional
	// +kubebuilder:validation:Minimum=1
	Replicas *int32 `json:"replicas,omitempty"`
}

// FeastProjectDir defines how to create the feast project directory.
// +kubebuilder:validation:XValidation:rule="[has(self.git), has(self.init)].exists_one(c, c)",message="One selection required between init or git."
type FeastProjectDir struct {
	// git configures cloning the feature repository.
	// +optional
	Git *GitCloneOptions `json:"git,omitempty"`
	// init configures `feast init` project creation.
	// +optional
	Init *FeastInitOptions `json:"init,omitempty"`
}

// GitCloneOptions describes how a clone should be performed.
// +kubebuilder:validation:XValidation:rule="has(self.featureRepoPath) ? !self.featureRepoPath.startsWith('/') : true",message="RepoPath must be a file name only, with no slashes."
type GitCloneOptions struct {
	// url is the repository URL to clone from.
	// +required
	URL string `json:"url"`
	// ref is a reference to a branch / tag / commit.
	// +optional
	Ref string `json:"ref,omitempty"`
	// configs are passed to git via `-c`.
	// e.g. http.sslVerify: 'false'
	// OR 'url."https://api:\${TOKEN}@github.com/".insteadOf': 'https://github.com/'
	// +optional
	Configs map[string]string `json:"configs,omitempty"`
	// featureRepoPath is the relative path to the feature repo subdirectory. Default is 'feature_repo'.
	// +optional
	FeatureRepoPath string `json:"featureRepoPath,omitempty"`
	// env adds environment variables for cloning the repository.
	// +optional
	Env *[]corev1.EnvVar `json:"env,omitempty"`
	// envFrom adds environment sources for cloning the repository.
	// +optional
	EnvFrom *[]corev1.EnvFromSource `json:"envFrom,omitempty"`
}

// FeastInitOptions defines how to run a `feast init`.
type FeastInitOptions struct {
	// minimal configures whether to use the minimal project template.
	// +optional
	Minimal bool `json:"minimal,omitempty"`
	// template selects the created project template.
	// +kubebuilder:validation:Enum=local;gcp;aws;snowflake;spark;postgres;hbase;cassandra;hazelcast;couchbase;clickhouse
	// +optional
	Template string `json:"template,omitempty"`
}

// FeastCronJob defines a CronJob to execute against a Feature Store deployment.
type FeastCronJob struct {
	// annotations are added to the CronJob metadata.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// jobSpec specifies the desired behavior of a job.
	// +optional
	JobSpec *JobSpec `json:"jobSpec,omitempty"`
	// containerConfigs configures the CronJob container settings.
	// +optional
	ContainerConfigs *CronJobContainerConfigs `json:"containerConfigs,omitempty"`

	// schedule is in Cron format, see https://en.wikipedia.org/wiki/Cron.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// timeZone is the time zone name for the given schedule, see https://en.wikipedia.org/wiki/List_of_tz_database_time_zones.
	// If not specified, this will default to the time zone of the kube-controller-manager process.
	// The set of valid time zone names and the time zone offset is loaded from the system-wide time zone
	// database by the API server during CronJob validation and the controller manager during execution.
	// If no system-wide time zone database can be found a bundled version of the database is used instead.
	// If the time zone name becomes invalid during the lifetime of a CronJob or due to a change in host
	// configuration, the controller will stop creating new new Jobs and will create a system event with the
	// reason UnknownTimeZone.
	// More information can be found in https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#time-zones
	// +optional
	TimeZone *string `json:"timeZone,omitempty"`

	// startingDeadlineSeconds is an optional deadline in seconds for starting the job if it misses scheduled
	// time for any reason.  Missed jobs executions will be counted as failed ones.
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// concurrencyPolicy specifies how to treat concurrent executions of a Job.
	// Valid values are:
	//
	// - "Allow" (default): allows CronJobs to run concurrently;
	// - "Forbid": forbids concurrent runs, skipping next run if previous run hasn't finished yet;
	// - "Replace": cancels currently running job and replaces it with a new one
	// +optional
	ConcurrencyPolicy batchv1.ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// suspend tells the controller to suspend subsequent executions, it does
	// not apply to already started executions.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// successfulJobsHistoryLimit is the number of successful finished jobs to retain. Value must be non-negative integer.
	// +optional
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// failedJobsHistoryLimit is the number of failed finished jobs to retain. Value must be non-negative integer.
	// +optional
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
}

// BatchEngineConfig defines the batch compute engine configuration.
type BatchEngineConfig struct {
	// configMapRef references a ConfigMap containing the batch engine configuration.
	// The ConfigMap should contain YAML-formatted config with 'type' and engine-specific fields.
	// +optional
	ConfigMapRef *corev1.LocalObjectReference `json:"configMapRef,omitempty"`
	// configMapKey is the key name in the ConfigMap. Defaults to "config" if not specified.
	// +optional
	ConfigMapKey string `json:"configMapKey,omitempty"`
}

// JobSpec describes how the job execution will look like.
type JobSpec struct {
	// podTemplateAnnotations are annotations to be applied to the CronJob's PodTemplate
	// metadata. This is separate from the CronJob-level annotations and must be
	// set explicitly by users if they want annotations on the PodTemplate.
	// +optional
	PodTemplateAnnotations map[string]string `json:"podTemplateAnnotations,omitempty"`

	// parallelism specifies the maximum desired number of pods the job should
	// run at any given time. The actual number of pods running in steady state will
	// be less than this number when ((.spec.completions - .status.successful) < .spec.parallelism),
	// i.e. when the work left to do is less than max parallelism.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
	// +optional
	Parallelism *int32 `json:"parallelism,omitempty"`

	// completions specifies the desired number of successfully finished pods the
	// job should be run with.  Setting to null means that the success of any
	// pod signals the success of all pods, and allows parallelism to have any positive
	// value.  Setting to 1 means that parallelism is limited to 1 and the success of that
	// pod signals the success of the job.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
	// +optional
	Completions *int32 `json:"completions,omitempty"`

	// activeDeadlineSeconds specifies the duration in seconds relative to the startTime that the job
	// may be continuously active before the system tries to terminate it; value
	// must be positive integer. If a Job is suspended (at creation or through an
	// update), this timer will effectively be stopped and reset when the Job is
	// resumed again.
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// podFailurePolicy specifies the policy of handling failed pods. In particular, it allows to
	// specify the set of actions and conditions which need to be
	// satisfied to take the associated action.
	// If empty, the default behaviour applies - the counter of failed pods,
	// represented by the jobs's .status.failed field, is incremented and it is
	// checked against the backoffLimit. This field cannot be used in combination
	// with restartPolicy=OnFailure.
	//
	// This field is beta-level. It can be used when the `JobPodFailurePolicy`
	// feature gate is enabled (enabled by default).
	// +optional
	PodFailurePolicy *batchv1.PodFailurePolicy `json:"podFailurePolicy,omitempty"`

	// backoffLimit specifies the number of retries before marking this job failed.
	// +optional
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// backoffLimitPerIndex specifies the limit for the number of retries within an
	// index before marking this index as failed. When enabled the number of
	// failures per index is kept in the pod's
	// batch.kubernetes.io/job-index-failure-count annotation. It can only
	// be set when Job's completionMode=Indexed, and the Pod's restart
	// policy is Never. The field is immutable.
	// This field is beta-level. It can be used when the `JobBackoffLimitPerIndex`
	// feature gate is enabled (enabled by default).
	// +optional
	BackoffLimitPerIndex *int32 `json:"backoffLimitPerIndex,omitempty"`

	// maxFailedIndexes specifies the maximal number of failed indexes before marking the Job as
	// failed, when backoffLimitPerIndex is set. Once the number of failed
	// indexes exceeds this number the entire Job is marked as Failed and its
	// execution is terminated. When left as null the job continues execution of
	// all of its indexes and is marked with the `Complete` Job condition.
	// It can only be specified when backoffLimitPerIndex is set.
	// It can be null or up to completions. It is required and must be
	// less than or equal to 10^4 when is completions greater than 10^5.
	// This field is beta-level. It can be used when the `JobBackoffLimitPerIndex`
	// feature gate is enabled (enabled by default).
	// +optional
	MaxFailedIndexes *int32 `json:"maxFailedIndexes,omitempty"`

	// ttlSecondsAfterFinished limits the lifetime of a Job that has finished
	// execution (either Complete or Failed). If this field is set,
	// ttlSecondsAfterFinished after the Job finishes, it is eligible to be
	// automatically deleted. When the Job is being deleted, its lifecycle
	// guarantees (e.g. finalizers) will be honored. If this field is unset,
	// the Job won't be automatically deleted. If this field is set to zero,
	// the Job becomes eligible to be deleted immediately after it finishes.
	// +optional
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
	// +optional
	CompletionMode *batchv1.CompletionMode `json:"completionMode,omitempty"`

	// suspend specifies whether the Job controller should create Pods or not. If
	// a Job is created with suspend set to true, no Pods are created by the Job
	// controller. If a Job is suspended after creation (i.e. the flag goes from
	// false to true), the Job controller will delete all active Pods associated
	// with this Job. Users must design their workload to gracefully handle this.
	// Suspending a Job will reset the StartTime field of the Job, effectively
	// resetting the ActiveDeadlineSeconds timer too.
	//
	// +optional
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
	// +optional
	PodReplacementPolicy *batchv1.PodReplacementPolicy `json:"podReplacementPolicy,omitempty"`
}

// FeatureStoreServices defines the desired feast services. An ephemeral onlineStore feature server is deployed by default.
type FeatureStoreServices struct {
	// offlineStore configures the offline store service.
	// +optional
	OfflineStore *OfflineStore `json:"offlineStore,omitempty"`
	// onlineStore configures the online store service.
	// +optional
	OnlineStore *OnlineStore `json:"onlineStore,omitempty"`
	// registry configures the registry service.
	// +optional
	Registry *Registry `json:"registry,omitempty"`
	// ui creates a UI server container.
	// +optional
	UI *ServerConfigs `json:"ui,omitempty"`
	// deploymentStrategy configures the deployment strategy for Feast services.
	// +optional
	DeploymentStrategy *appsv1.DeploymentStrategy `json:"deploymentStrategy,omitempty"`
	// securityContext configures the pod security context for Feast services.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`
	// disableInitContainers disables the 'feast repo initialization' initContainer.
	// +optional
	DisableInitContainers bool `json:"disableInitContainers,omitempty"`
	// volumes specifies the volumes to mount in the FeatureStore deployment. A corresponding `VolumeMount` should be added to whichever feast service(s) require access to said volume(s).
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`
	// scaling configures horizontal scaling for the FeatureStore deployment (e.g. HPA autoscaling).
	// For static replicas, use spec.replicas instead.
	// +optional
	Scaling *ScalingConfig `json:"scaling,omitempty"`
	// podDisruptionBudgets configures a PodDisruptionBudget for the FeatureStore deployment.
	// Only created when scaling is enabled (replicas > 1 or autoscaling).
	// +optional
	PodDisruptionBudgets *PDBConfig `json:"podDisruptionBudgets,omitempty"`
	// topologySpreadConstraints defines how pods are spread across topology domains.
	// When scaling is enabled and this is not set, the operator auto-injects a soft
	// zone-spread constraint (whenUnsatisfiable: ScheduleAnyway).
	// Set to an empty array to disable auto-injection.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	// affinity defines the pod scheduling constraints for the FeatureStore deployment.
	// When scaling is enabled and this is not set, the operator auto-injects a soft
	// pod anti-affinity rule to prefer spreading pods across nodes.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
}

// ScalingConfig configures horizontal scaling for the FeatureStore deployment.
type ScalingConfig struct {
	// autoscaling configures a HorizontalPodAutoscaler for the FeatureStore deployment.
	// Mutually exclusive with spec.replicas.
	// +optional
	Autoscaling *AutoscalingConfig `json:"autoscaling,omitempty"`
}

// AutoscalingConfig defines HPA settings for the FeatureStore deployment.
type AutoscalingConfig struct {
	// minReplicas is the lower limit for the number of replicas. Defaults to 1.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MinReplicas *int32 `json:"minReplicas,omitempty"`
	// maxReplicas is the upper limit for the number of replicas. Required.
	// +kubebuilder:validation:Minimum=1
	// +required
	MaxReplicas int32 `json:"maxReplicas"`
	// metrics contains the specifications for which to use to calculate the desired replica count.
	// If not set, defaults to 80% CPU utilization.
	// +optional
	Metrics []autoscalingv2.MetricSpec `json:"metrics,omitempty"`
	// behavior configures the scaling behavior of the target.
	// +optional
	Behavior *autoscalingv2.HorizontalPodAutoscalerBehavior `json:"behavior,omitempty"`
}

// PDBConfig configures a PodDisruptionBudget for the FeatureStore deployment.
// Exactly one of minAvailable or maxUnavailable must be set.
// +kubebuilder:validation:XValidation:rule="[has(self.minAvailable), has(self.maxUnavailable)].exists_one(c, c)",message="Exactly one of minAvailable or maxUnavailable must be set."
type PDBConfig struct {
	// minAvailable specifies the minimum number/percentage of pods that must remain available.
	// Mutually exclusive with maxUnavailable.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// maxUnavailable specifies the maximum number/percentage of pods that can be unavailable.
	// Mutually exclusive with minAvailable.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// OfflineStore configures the offline store service
type OfflineStore struct {
	// server creates a remote offline server container.
	// +optional
	Server *ServerConfigs `json:"server,omitempty"`
	// persistence configures offline store persistence.
	// +optional
	Persistence *OfflineStorePersistence `json:"persistence,omitempty"`
}

// OfflineStorePersistence configures the persistence settings for the offline store service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type OfflineStorePersistence struct {
	// file configures file-based persistence.
	// +optional
	FilePersistence *OfflineStoreFilePersistence `json:"file,omitempty"`
	// store configures store-based persistence.
	// +optional
	DBPersistence *OfflineStoreDBStorePersistence `json:"store,omitempty"`
}

// OfflineStoreFilePersistence configures the file-based persistence for the offline store service
type OfflineStoreFilePersistence struct {
	// type is the file-based persistence type.
	// +kubebuilder:validation:Enum=file;dask;duckdb
	// +optional
	Type string `json:"type,omitempty"`
	// pvc configures persistent volume claims for file-based persistence.
	// +optional
	PvcConfig *PvcConfig `json:"pvc,omitempty"`
}

var ValidOfflineStoreFilePersistenceTypes = []string{
	"dask",
	"duckdb",
	"file",
}

// OfflineStoreDBStorePersistence configures the DB store persistence for the offline store service
type OfflineStoreDBStorePersistence struct {
	// type is the persistence type you want to use.
	// +kubebuilder:validation:Enum=snowflake.offline;bigquery;redshift;spark;postgres;trino;athena;mssql;couchbase.offline;clickhouse;ray
	// +required
	Type string `json:"type"`
	// secretRef holds data store parameters from "feature_store.yaml". "registry_type" & "type" fields should be removed.
	// +required
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// secretKeyName defaults to the selected store "type".
	// +optional
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
	"ray",
}

// OnlineStore configures the online store service
type OnlineStore struct {
	// server creates a feature server container.
	// +optional
	Server *ServerConfigs `json:"server,omitempty"`
	// persistence configures online store persistence.
	// +optional
	Persistence *OnlineStorePersistence `json:"persistence,omitempty"`
}

// OnlineStorePersistence configures the persistence settings for the online store service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type OnlineStorePersistence struct {
	// file configures file-based persistence.
	// +optional
	FilePersistence *OnlineStoreFilePersistence `json:"file,omitempty"`
	// store configures store-based persistence.
	// +optional
	DBPersistence *OnlineStoreDBStorePersistence `json:"store,omitempty"`
}

// OnlineStoreFilePersistence configures the file-based persistence for the online store service
// +kubebuilder:validation:XValidation:rule="(!has(self.pvc) && has(self.path)) ? self.path.startsWith('/') : true",message="Ephemeral stores must have absolute paths."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !self.path.startsWith('/') : true",message="PVC path must be a file name only, with no slashes."
// +kubebuilder:validation:XValidation:rule="has(self.path) ? !(self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="Online store does not support S3 or GS buckets."
type OnlineStoreFilePersistence struct {
	// path is the filesystem path for file-based persistence.
	// +optional
	Path string `json:"path,omitempty"`
	// pvc configures persistent volume claims for file-based persistence.
	// +optional
	PvcConfig *PvcConfig `json:"pvc,omitempty"`
}

// OnlineStoreDBStorePersistence configures the DB store persistence for the online store service
type OnlineStoreDBStorePersistence struct {
	// type is the persistence type you want to use.
	// +kubebuilder:validation:Enum=snowflake.online;redis;datastore;dynamodb;bigtable;postgres;cassandra;mysql;hazelcast;singlestore;hbase;elasticsearch;qdrant;couchbase.online;milvus;hybrid
	// +required
	Type string `json:"type"`
	// secretRef holds data store parameters from "feature_store.yaml". "registry_type" & "type" fields should be removed.
	// +required
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// secretKeyName defaults to the selected store "type".
	// +optional
	SecretKeyName string `json:"secretKeyName,omitempty"`
}

var ValidOnlineStoreDBStorePersistenceTypes = []string{
	"snowflake.online",
	"redis",
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
	"hybrid",
}

// LocalRegistryConfig configures the registry service
type LocalRegistryConfig struct {
	// server creates a registry server container.
	// +optional
	Server *RegistryServerConfigs `json:"server,omitempty"`
	// persistence configures registry persistence.
	// +optional
	Persistence *RegistryPersistence `json:"persistence,omitempty"`
}

// RegistryPersistence configures the persistence settings for the registry service
// +kubebuilder:validation:XValidation:rule="[has(self.file), has(self.store)].exists_one(c, c)",message="One selection required between file or store."
type RegistryPersistence struct {
	// file configures file-based persistence.
	// +optional
	FilePersistence *RegistryFilePersistence `json:"file,omitempty"`
	// store configures store-based persistence.
	// +optional
	DBPersistence *RegistryDBStorePersistence `json:"store,omitempty"`
}

// RegistryFilePersistence configures the file-based persistence for the registry service
// +kubebuilder:validation:XValidation:rule="(!has(self.pvc) && has(self.path)) ? (self.path.startsWith('/') || self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="Registry files must use absolute paths or be S3 ('s3://') or GS ('gs://') object store URIs."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !self.path.startsWith('/') : true",message="PVC path must be a file name only, with no slashes."
// +kubebuilder:validation:XValidation:rule="(has(self.pvc) && has(self.path)) ? !(self.path.startsWith('s3://') || self.path.startsWith('gs://')) : true",message="PVC persistence does not support S3 or GS object store URIs."
// +kubebuilder:validation:XValidation:rule="(has(self.s3_additional_kwargs) && has(self.path)) ? self.path.startsWith('s3://') : true",message="Additional S3 settings are available only for S3 object store URIs."
type RegistryFilePersistence struct {
	// path is the filesystem path or object store URI for registry persistence.
	// +optional
	Path string `json:"path,omitempty"`
	// pvc configures persistent volume claims for registry persistence.
	// +optional
	PvcConfig *PvcConfig `json:"pvc,omitempty"`
	// s3_additional_kwargs configures additional S3 settings for registry persistence.
	// +optional
	S3AdditionalKwargs *map[string]string `json:"s3_additional_kwargs,omitempty"`

	// cache_ttl_seconds defines the TTL (in seconds) for the registry cache.
	// +kubebuilder:validation:Minimum=0
	// +optional
	CacheTTLSeconds *int32 `json:"cache_ttl_seconds,omitempty"`

	// cache_mode defines the registry cache update strategy.
	// Allowed values are "sync" and "thread".
	// +kubebuilder:validation:Enum=none;sync;thread
	// +optional
	CacheMode *string `json:"cache_mode,omitempty"`
}

// RegistryDBStorePersistence configures the DB store persistence for the registry service
type RegistryDBStorePersistence struct {
	// type is the persistence type you want to use.
	// +kubebuilder:validation:Enum=sql;snowflake.registry
	// +required
	Type string `json:"type"`
	// secretRef holds data store parameters from "feature_store.yaml". "registry_type" & "type" fields should be removed.
	// +required
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
	// secretKeyName defaults to the selected store "type".
	// +optional
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
	// ref references an existing PVC.
	// +optional
	Ref *corev1.LocalObjectReference `json:"ref,omitempty"`
	// create specifies settings for creating a new PVC.
	// +optional
	Create *PvcCreate `json:"create,omitempty"`
	// mountPath is the path within the container at which the volume should be mounted.
	// Must start by "/" and cannot contain ':'.
	// +required
	MountPath string `json:"mountPath"`
}

// PvcCreate defines the immutable settings to create a new PVC mounted at the given path.
// The PVC name is the same as the associated deployment & feast service name.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="PvcCreate is immutable"
type PvcCreate struct {
	// accessModes are the k8s persistent volume access modes. Defaults to ["ReadWriteOnce"].
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
	// storageClassName is the name of an existing StorageClass to which this persistent volume belongs. Empty value
	// means that this volume does not belong to any StorageClass and the cluster default will be used.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
	// resources describes the storage resource requirements for a volume.
	// Default requested storage size depends on the associated service:
	// - 10Gi for offline store
	// - 5Gi for online store
	// - 5Gi for registry
	// +optional
	Resources corev1.VolumeResourceRequirements `json:"resources,omitempty"`
}

// Registry configures the registry service. One selection is required. Local is the default setting.
// +kubebuilder:validation:XValidation:rule="[has(self.local), has(self.remote)].exists_one(c, c)",message="One selection required."
type Registry struct {
	// local configures a local registry deployment.
	// +optional
	Local *LocalRegistryConfig `json:"local,omitempty"`
	// remote configures a remote registry endpoint.
	// +optional
	Remote *RemoteRegistryConfig `json:"remote,omitempty"`
}

// RemoteRegistryConfig points to a remote feast registry server. When set, the operator will not deploy a registry for this FeatureStore CR.
// Instead, this FeatureStore CR's online/offline services will use a remote registry. One selection is required.
// +kubebuilder:validation:XValidation:rule="[has(self.hostname), has(self.feastRef)].exists_one(c, c)",message="One selection required."
type RemoteRegistryConfig struct {
	// hostname is the host address of the remote registry service - <domain>:<port>, e.g. `registry.<namespace>.svc.cluster.local:80`.
	// +optional
	Hostname *string `json:"hostname,omitempty"`
	// feastRef references an existing `FeatureStore` CR in the same k8s cluster.
	// +optional
	FeastRef *FeatureStoreRef `json:"feastRef,omitempty"`
	// tls configures TLS settings for the remote registry.
	// +optional
	TLS *TlsRemoteRegistryConfigs `json:"tls,omitempty"`
}

// FeatureStoreRef defines which existing FeatureStore's registry should be used
type FeatureStoreRef struct {
	// name is the name of the FeatureStore.
	// +required
	Name string `json:"name"`
	// namespace is the namespace of the FeatureStore.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// ServerConfigs creates a server for the feast service, with specified container configurations.
type ServerConfigs struct {
	ContainerConfigs `json:",inline"`
	// tls configures TLS settings for the server.
	// +optional
	TLS *TlsConfigs `json:"tls,omitempty"`
	// logLevel sets the logging level for the server.
	// Allowed values: "debug", "info", "warning", "error", "critical".
	// +kubebuilder:validation:Enum=debug;info;warning;error;critical
	// +optional
	LogLevel *string `json:"logLevel,omitempty"`
	// metrics exposes Prometheus-compatible metrics for the Feast server when enabled.
	// +optional
	Metrics *bool `json:"metrics,omitempty"`
	// volumeMounts defines the list of volumes that should be mounted into the feast container.
	// This allows attaching persistent storage, config files, secrets, or other resources
	// required by the Feast components. Ensure that each volume mount has a corresponding
	// volume definition in the Volumes field.
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
	// workerConfigs defines the worker configuration for the Feast server.
	// These options are primarily used for production deployments to optimize performance.
	// +optional
	WorkerConfigs *WorkerConfigs `json:"workerConfigs,omitempty"`
}

// WorkerConfigs defines the worker configuration for Feast servers.
// These settings control gunicorn worker processes for production deployments.
type WorkerConfigs struct {
	// workers is the number of worker processes. Use -1 to auto-calculate based on CPU cores (2 * CPU + 1).
	// Defaults to 1 if not specified.
	// +kubebuilder:validation:Minimum=-1
	// +optional
	Workers *int32 `json:"workers,omitempty"`
	// workerConnections is the maximum number of simultaneous clients per worker process.
	// Defaults to 1000.
	// +kubebuilder:validation:Minimum=1
	// +optional
	WorkerConnections *int32 `json:"workerConnections,omitempty"`
	// maxRequests is the maximum number of requests a worker will process before restarting.
	// This helps prevent memory leaks. Defaults to 1000.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxRequests *int32 `json:"maxRequests,omitempty"`
	// maxRequestsJitter is the maximum jitter to add to max-requests to prevent
	// thundering herd effect on worker restart. Defaults to 50.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxRequestsJitter *int32 `json:"maxRequestsJitter,omitempty"`
	// keepAliveTimeout is the timeout for keep-alive connections in seconds.
	// Defaults to 30.
	// +kubebuilder:validation:Minimum=1
	// +optional
	KeepAliveTimeout *int32 `json:"keepAliveTimeout,omitempty"`
	// registryTTLSeconds is the number of seconds after which the registry is refreshed.
	// Higher values reduce refresh overhead but increase staleness. Defaults to 60.
	// +kubebuilder:validation:Minimum=0
	// +optional
	RegistryTTLSeconds *int32 `json:"registryTTLSeconds,omitempty"`
}

// RegistryServerConfigs creates a registry server for the feast service, with specified container configurations.
// +kubebuilder:validation:XValidation:rule="self.restAPI == true || self.grpc == true || !has(self.grpc)", message="At least one of restAPI or grpc must be true"
type RegistryServerConfigs struct {
	ServerConfigs `json:",inline"`

	// restAPI enables the REST API registry server.
	// +optional
	RestAPI *bool `json:"restAPI,omitempty"`

	// grpc enables the gRPC registry server. Defaults to true if unset.
	// +optional
	GRPC *bool `json:"grpc,omitempty"`
}

// CronJobContainerConfigs k8s container settings for the CronJob
type CronJobContainerConfigs struct {
	ContainerConfigs `json:",inline"`

	// commands are executed (in order) against a Feature Store deployment.
	// Defaults to "feast apply" & "feast materialize-incremental $(date -u +'%Y-%m-%dT%H:%M:%S')"
	// +optional
	Commands []string `json:"commands,omitempty"`
}

// ContainerConfigs k8s container settings for the server
type ContainerConfigs struct {
	DefaultCtrConfigs  `json:",inline"`
	OptionalCtrConfigs `json:",inline"`
}

// DefaultCtrConfigs k8s container settings that are applied by default
type DefaultCtrConfigs struct {
	// image overrides the default container image.
	// +optional
	Image *string `json:"image,omitempty"`
}

// OptionalCtrConfigs k8s container settings that are optional
type OptionalCtrConfigs struct {
	// env overrides container environment variables.
	// +optional
	Env *[]corev1.EnvVar `json:"env,omitempty"`
	// envFrom overrides container environment sources.
	// +optional
	EnvFrom *[]corev1.EnvFromSource `json:"envFrom,omitempty"`
	// imagePullPolicy overrides the container image pull policy.
	// +optional
	ImagePullPolicy *corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// resources overrides the container resource requirements.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	// nodeSelector overrides the container node selector.
	// +optional
	NodeSelector *map[string]string `json:"nodeSelector,omitempty"`
}

// AuthzConfig defines the authorization settings for the deployed Feast services.
// +kubebuilder:validation:XValidation:rule="[has(self.kubernetes), has(self.oidc)].exists_one(c, c)",message="One selection required between kubernetes or oidc."
type AuthzConfig struct {
	// kubernetes configures Kubernetes RBAC authorization.
	// +optional
	KubernetesAuthz *KubernetesAuthz `json:"kubernetes,omitempty"`
	// oidc configures OpenID Connect authorization.
	// +optional
	OidcAuthz *OidcAuthz `json:"oidc,omitempty"`
}

// KubernetesAuthz provides a way to define the authorization settings using Kubernetes RBAC resources.
// https://kubernetes.io/docs/reference/access-authn-authz/rbac/
type KubernetesAuthz struct {
	// roles are the Kubernetes RBAC roles to be deployed in the same namespace of the FeatureStore.
	// Roles are managed by the operator and created with an empty list of rules.
	// See the Feast permission model at https://docs.feast.dev/getting-started/concepts/permission
	// The feature store admin is not obligated to manage roles using the Feast operator, roles can be managed independently.
	// This configuration option is only providing a way to automate this procedure.
	// Important note: the operator cannot ensure that these roles will match the ones used in the configured Feast permissions.
	// +optional
	Roles []string `json:"roles,omitempty"`
}

// OidcAuthz defines the authorization settings for deployments using an Open ID Connect identity provider.
// https://auth0.com/docs/authenticate/protocols/openid-connect-protocol
type OidcAuthz struct {
	// secretRef references the secret containing OIDC configuration.
	// +required
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// TlsConfigs configures server TLS for a feast service. in an openshift cluster, this is configured by default using service serving certificates.
// +kubebuilder:validation:XValidation:rule="(!has(self.disable) || !self.disable) ? has(self.secretRef) : true",message="`secretRef` required if `disable` is false."
type TlsConfigs struct {
	// secretRef references the local k8s secret where the TLS key and cert reside.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
	// secretKeyNames defines the secret key names for TLS.
	// +optional
	SecretKeyNames SecretKeyNames `json:"secretKeyNames,omitempty"`
	// disable will disable TLS for the feast service. useful in an openshift cluster, for example, where TLS is configured by default.
	// +optional
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
	// configMapRef references the local k8s configmap where the TLS cert resides.
	// +required
	ConfigMapRef corev1.LocalObjectReference `json:"configMapRef"`
	// certName defines the configmap key name for the client TLS cert.
	// +required
	CertName string `json:"certName"`
}

// SecretKeyNames defines the secret key names for the TLS key and cert.
type SecretKeyNames struct {
	// tlsCrt defaults to "tls.crt".
	// +optional
	TlsCrt string `json:"tlsCrt,omitempty"`
	// tlsKey defaults to "tls.key".
	// +optional
	TlsKey string `json:"tlsKey,omitempty"`
}

// FeatureStoreStatus defines the observed state of FeatureStore
type FeatureStoreStatus struct {
	// conditions report the current status conditions.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
	// applied shows the currently applied feast configuration, including any pertinent defaults.
	// +required
	Applied FeatureStoreSpec `json:"applied,omitempty"`
	// clientConfigMap is the ConfigMap containing a client `feature_store.yaml` for this feast deployment.
	// +optional
	ClientConfigMap string `json:"clientConfigMap,omitempty"`
	// cronJob is the CronJob in this namespace for this feast deployment.
	// +optional
	CronJob string `json:"cronJob,omitempty"`
	// feastVersion is the resolved Feast version for this deployment.
	// +optional
	FeastVersion string `json:"feastVersion,omitempty"`
	// phase is the current lifecycle phase.
	// +optional
	Phase string `json:"phase,omitempty"`
	// serviceHostnames contains resolved service hostnames.
	// +optional
	ServiceHostnames ServiceHostnames `json:"serviceHostnames,omitempty"`
	// replicas is the current number of ready pod replicas (used by the scale sub-resource).
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// selector is the label selector for pods managed by the FeatureStore deployment (used by the scale sub-resource).
	// +optional
	Selector string `json:"selector,omitempty"`
	// scalingStatus reports the current scaling state of the FeatureStore deployment.
	// +optional
	ScalingStatus *ScalingStatus `json:"scalingStatus,omitempty"`
}

// ScalingStatus reports the observed scaling state.
type ScalingStatus struct {
	// currentReplicas is the current number of pod replicas.
	// +optional
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`
	// desiredReplicas is the desired number of pod replicas.
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`
}

// ServiceHostnames defines the service hostnames in the format of <domain>:<port>, e.g. example.svc.cluster.local:80
type ServiceHostnames struct {
	// offlineStore is the offline store service hostname.
	// +optional
	OfflineStore string `json:"offlineStore,omitempty"`
	// onlineStore is the online store service hostname.
	// +optional
	OnlineStore string `json:"onlineStore,omitempty"`
	// registry is the registry service hostname.
	// +optional
	Registry string `json:"registry,omitempty"`
	// registryRest is the registry REST service hostname.
	// +optional
	RegistryRest string `json:"registryRest,omitempty"`
	// ui is the UI service hostname.
	// +optional
	UI string `json:"ui,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=feast
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:storageversion

// FeatureStore is the Schema for the featurestores API
type FeatureStore struct {
	metav1.TypeMeta `json:",inline"`
	// metadata contains the object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of FeatureStore.
	// +required
	Spec FeatureStoreSpec `json:"spec,omitempty"`
	// status defines the observed state of FeatureStore.
	// +required
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
