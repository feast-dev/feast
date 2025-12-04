package services

import (
	"os"
	"strconv"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (feast *FeastServices) deployCronJob() error {
	if err := feast.createCronJobRole(); err != nil {
		return feast.setFeastServiceCondition(err, CronJobFeastType)
	}
	if err := feast.createCronJobRoleBinding(); err != nil {
		return feast.setFeastServiceCondition(err, CronJobFeastType)
	}
	if err := feast.createCronJob(); err != nil {
		return feast.setFeastServiceCondition(err, CronJobFeastType)
	}
	return feast.setFeastServiceCondition(nil, CronJobFeastType)
}

func (feast *FeastServices) createCronJob() error {
	logger := log.FromContext(feast.Handler.Context)
	cronJob := feast.initCronJob()
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, cronJob, controllerutil.MutateFn(func() error {
		return feast.setCronJob(cronJob)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "CronJob", cronJob.Name, "operation", op)
	}

	return nil
}

func (feast *FeastServices) initCronJob() *batchv1.CronJob {
	cronJob := &batchv1.CronJob{
		ObjectMeta: feast.GetObjectMeta(),
	}
	cronJob.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("CronJob"))

	return cronJob
}

func (feast *FeastServices) setCronJob(cronJob *batchv1.CronJob) error {
	appliedCronJob := feast.Handler.FeatureStore.Status.Applied.CronJob
	cronJob.Labels = feast.getFeastTypeLabels(CronJobFeastType)

	if appliedCronJob.Annotations != nil {
		cronJob.Annotations = make(map[string]string, len(appliedCronJob.Annotations))
		for k, v := range appliedCronJob.Annotations {
			cronJob.Annotations[k] = v
		}
	}
	cronJob.Spec = batchv1.CronJobSpec{
		Schedule: appliedCronJob.Schedule,
		JobTemplate: batchv1.JobTemplateSpec{
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{},
					Spec:       feast.getCronJobPodSpec(),
				},
			},
		},
	}
	if appliedCronJob.Suspend != nil {
		cronJob.Spec.Suspend = appliedCronJob.Suspend
	}
	if len(appliedCronJob.ConcurrencyPolicy) > 0 {
		cronJob.Spec.ConcurrencyPolicy = appliedCronJob.ConcurrencyPolicy
	}
	if appliedCronJob.TimeZone != nil {
		cronJob.Spec.TimeZone = appliedCronJob.TimeZone
	}
	if appliedCronJob.StartingDeadlineSeconds != nil {
		cronJob.Spec.StartingDeadlineSeconds = appliedCronJob.StartingDeadlineSeconds
	}
	if appliedCronJob.SuccessfulJobsHistoryLimit != nil {
		cronJob.Spec.SuccessfulJobsHistoryLimit = appliedCronJob.SuccessfulJobsHistoryLimit
	}
	if appliedCronJob.FailedJobsHistoryLimit != nil {
		cronJob.Spec.FailedJobsHistoryLimit = appliedCronJob.FailedJobsHistoryLimit
	}

	appliedJobSpec := appliedCronJob.JobSpec
	if appliedJobSpec != nil {
		jobSpec := &cronJob.Spec.JobTemplate.Spec

		// apply PodTemplateAnnotations into the PodTemplate metadata if provided
		if appliedJobSpec.PodTemplateAnnotations != nil {
			if jobSpec.Template.Annotations == nil {
				jobSpec.Template.Annotations = make(map[string]string, len(appliedJobSpec.PodTemplateAnnotations))
			}
			for k, v := range appliedJobSpec.PodTemplateAnnotations {
				jobSpec.Template.Annotations[k] = v
			}
		}

		if appliedJobSpec.ActiveDeadlineSeconds != nil {
			jobSpec.ActiveDeadlineSeconds = appliedJobSpec.ActiveDeadlineSeconds
		}
		if appliedJobSpec.BackoffLimit != nil {
			jobSpec.BackoffLimit = appliedJobSpec.BackoffLimit
		}
		if appliedJobSpec.BackoffLimitPerIndex != nil {
			jobSpec.BackoffLimitPerIndex = appliedJobSpec.BackoffLimitPerIndex
		}
		if appliedJobSpec.CompletionMode != nil {
			jobSpec.CompletionMode = appliedJobSpec.CompletionMode
		}
		if appliedJobSpec.Completions != nil {
			jobSpec.Completions = appliedJobSpec.Completions
		}
		if appliedJobSpec.MaxFailedIndexes != nil {
			jobSpec.MaxFailedIndexes = appliedJobSpec.MaxFailedIndexes
		}
		if appliedJobSpec.Parallelism != nil {
			jobSpec.Parallelism = appliedJobSpec.Parallelism
		}
		if appliedJobSpec.PodFailurePolicy != nil {
			jobSpec.PodFailurePolicy = appliedJobSpec.PodFailurePolicy
		}
		if appliedJobSpec.PodReplacementPolicy != nil {
			jobSpec.PodReplacementPolicy = appliedJobSpec.PodReplacementPolicy
		}
		if appliedJobSpec.Suspend != nil {
			jobSpec.Suspend = appliedJobSpec.Suspend
		}
		if appliedJobSpec.TTLSecondsAfterFinished != nil {
			jobSpec.TTLSecondsAfterFinished = appliedJobSpec.TTLSecondsAfterFinished
		}
	}

	feast.Handler.FeatureStore.Status.CronJob = cronJob.Name
	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, cronJob, feast.Handler.Scheme)
}

func (feast *FeastServices) getCronJobPodSpec() corev1.PodSpec {
	podSpec := corev1.PodSpec{
		ServiceAccountName: feast.initFeastSA().Name,
		RestartPolicy:      corev1.RestartPolicyNever,
	}
	feast.setCronJobContainers(&podSpec)
	return podSpec
}

func (feast *FeastServices) setCronJobContainers(podSpec *corev1.PodSpec) {
	appliedCronJob := feast.Handler.FeatureStore.Status.Applied.CronJob
	lastCmdIndex := len(appliedCronJob.ContainerConfigs.Commands) - 1
	for i, cronJobCmd := range appliedCronJob.ContainerConfigs.Commands {
		containerName := handler.FeastPrefix + strconv.Itoa(i)
		if i < lastCmdIndex {
			podSpec.InitContainers = append(podSpec.InitContainers, feast.getCronJobContainer(containerName, cronJobCmd))
		} else {
			podSpec.Containers = append(podSpec.Containers, feast.getCronJobContainer(containerName, cronJobCmd))
		}
	}
}

func (feast *FeastServices) getCronJobContainer(containerName, cronJobCmd string) corev1.Container {
	return *getContainer(
		containerName,
		"",
		[]string{
			"kubectl", "exec", "deploy/" + feast.initFeastDeploy().Name, "-i",
			"--",
			"bash", "-c", cronJobCmd,
		},
		feast.Handler.FeatureStore.Status.Applied.CronJob.ContainerConfigs.ContainerConfigs,
		"",
	)
}

func (feast *FeastServices) createCronJobRole() error {
	logger := log.FromContext(feast.Handler.Context)
	role := feast.initCronJobRole()
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, role, controllerutil.MutateFn(func() error {
		return feast.setCronJobRole(role)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Role", role.Name, "operation", op)
	}

	return nil
}

func (feast *FeastServices) initCronJobRole() *rbacv1.Role {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: feast.getCronJobRoleName(), Namespace: feast.Handler.FeatureStore.Namespace},
	}
	role.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("Role"))

	return role
}

func (feast *FeastServices) setCronJobRole(role *rbacv1.Role) error {
	role.Labels = feast.getFeastTypeLabels(CronJobFeastType)
	role.Rules = []rbacv1.PolicyRule{
		{
			APIGroups:     []string{appsv1.GroupName},
			Resources:     []string{"deployments"},
			ResourceNames: []string{feast.initFeastDeploy().Name},
			Verbs:         []string{"get"},
		},
		{
			APIGroups: []string{corev1.GroupName},
			Resources: []string{"pods"},
			Verbs:     []string{"list"},
		},
		{
			APIGroups: []string{corev1.GroupName},
			Resources: []string{"pods/exec"},
			Verbs:     []string{"create"},
		},
	}

	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, role, feast.Handler.Scheme)
}

func (feast *FeastServices) createCronJobRoleBinding() error {
	logger := log.FromContext(feast.Handler.Context)
	roleBinding := feast.initCronJobRoleBinding()
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, roleBinding, controllerutil.MutateFn(func() error {
		return feast.setCronJobRoleBinding(roleBinding)
	})); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "RoleBinding", roleBinding.Name, "operation", op)
	}

	return nil
}

func (feast *FeastServices) initCronJobRoleBinding() *rbacv1.RoleBinding {
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: feast.getCronJobRoleName(), Namespace: feast.Handler.FeatureStore.Namespace},
	}
	roleBinding.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))

	return roleBinding
}

func (feast *FeastServices) setCronJobRoleBinding(roleBinding *rbacv1.RoleBinding) error {
	roleBinding.Labels = feast.getFeastTypeLabels(CronJobFeastType)
	roleBinding.Subjects = []rbacv1.Subject{{
		Kind:      rbacv1.ServiceAccountKind,
		Name:      feast.initFeastSA().Name,
		Namespace: feast.Handler.FeatureStore.Namespace,
	}}
	roleBinding.RoleRef = rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "Role",
		Name:     feast.getCronJobRoleName(),
	}

	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, roleBinding, feast.Handler.Scheme)
}

func (feast *FeastServices) getCronJobRoleName() string {
	return GetFeastServiceName(feast.Handler.FeatureStore, CronJobFeastType)
}

// defaults to a CronJob configuration that will never run. this default Job can be executed manually, however.
// e.g. kubectl create job --from=cronjob/feast-sample feast-sample-job
func setDefaultCronJobConfigs(feastCronJob *feastdevv1.FeastCronJob) {
	if len(feastCronJob.Schedule) == 0 {
		feastCronJob.Schedule = "@yearly"
		if feastCronJob.Suspend == nil {
			feastCronJob.Suspend = boolPtr(true)
		}
		if len(feastCronJob.ConcurrencyPolicy) == 0 {
			feastCronJob.ConcurrencyPolicy = batchv1.ReplaceConcurrent
		}
		if feastCronJob.StartingDeadlineSeconds == nil {
			feastCronJob.StartingDeadlineSeconds = int64Ptr(5)
		}
	}
	if feastCronJob.ContainerConfigs == nil {
		feastCronJob.ContainerConfigs = &feastdevv1.CronJobContainerConfigs{}
	}
	if feastCronJob.ContainerConfigs.Image == nil {
		feastCronJob.ContainerConfigs.Image = getCronJobImage()
	}
	if len(feastCronJob.ContainerConfigs.Commands) == 0 {
		feastCronJob.ContainerConfigs.Commands = []string{
			"feast apply",
			"feast materialize-incremental $(date -u +'%Y-%m-%dT%H:%M:%S')",
		}
	}
}

func getCronJobImage() *string {
	if img, exists := os.LookupEnv(cronJobImageVar); exists {
		return &img
	}

	return &DefaultCronJobImage
}
