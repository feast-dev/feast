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

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
)

var _ = Describe("FeatureStore Controller - Feast CronJob", func() {
	Context("When reconciling a FeatureStore resource", func() {
		const resourceName = "test-cronjob"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		featurestore := &feastdevv1alpha1.FeatureStore{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind FeatureStore")
			err := k8sClient.Get(ctx, typeNamespacedName, featurestore)
			if err != nil && errors.IsNotFound(err) {
				resource := &feastdevv1alpha1.FeatureStore{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: feastdevv1alpha1.FeatureStoreSpec{FeastProject: feastProject},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})
		AfterEach(func() {
			resource := &feastdevv1alpha1.FeatureStore{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance FeatureStore")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource with a CronJob", func() {
			By("Reconciling the created resource with default CronJob settings")
			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource := &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			feast := services.FeastServices{
				Handler: handler.FeastHandler{
					Client:       controllerReconciler.Client,
					Context:      ctx,
					Scheme:       controllerReconciler.Scheme,
					FeatureStore: resource,
				},
			}

			objMeta := feast.GetObjectMeta()
			Expect(resource.Status).NotTo(BeNil())
			Expect(resource.Status.CronJob).To(Equal(objMeta.Name))
			Expect(resource.Status.Applied.CronJob.Schedule).NotTo(BeEmpty())

			Expect(resource.Status.Conditions).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.CronJobReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Type).To(Equal(feastdevv1alpha1.CronJobReadyType))
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.CronJobReadyMessage))

			// check CronJob
			cronJob := &batchv1.CronJob{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, cronJob)
			Expect(err).NotTo(HaveOccurred())
			Expect(controllerutil.HasControllerReference(cronJob)).To(BeTrue())
			Expect(cronJob.Spec.Schedule).To(Equal("@yearly"))
			suspend := true
			Expect(cronJob.Spec.Suspend).To(Equal(&suspend))
			Expect(cronJob.Spec.ConcurrencyPolicy).To(Equal(batchv1.ReplaceConcurrent))
			startingDeadlineSeconds := int64(5)
			Expect(cronJob.Spec.StartingDeadlineSeconds).To(Equal(&startingDeadlineSeconds))

			checkCronJob(resource.Status.Applied.CronJob, cronJob.Spec)
		})

		It("should successfully reconcile the resource with a CronJob", func() {
			By("Reconciling the created resource with custom CronJob configurations")
			controllerReconciler := &FeatureStoreReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			resource := &feastdevv1alpha1.FeatureStore{}
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			schedule := "5 4 * * *"
			image := "image:latest"
			timeZone := "Etc/UTC"
			suspend := false
			int32Var := int32(2)
			int64Var := int64(10)
			completionMode := batchv1.IndexedCompletion
			replacementPolicy := batchv1.TerminatingOrFailed
			always := corev1.PullAlways
			startingDeadlineSeconds := int64(6)
			resource.Spec.CronJob = &feastdevv1alpha1.FeastCronJob{
				Schedule:                   schedule,
				StartingDeadlineSeconds:    &startingDeadlineSeconds,
				TimeZone:                   &timeZone,
				ConcurrencyPolicy:          batchv1.ForbidConcurrent,
				Suspend:                    &suspend,
				SuccessfulJobsHistoryLimit: &int32Var,
				FailedJobsHistoryLimit:     &int32Var,
				ContainerConfigs: &feastdevv1alpha1.CronJobContainerConfigs{
					Commands: []string{
						"feast apply",
						"feast feature-views list",
						"feast entities list",
						"feast on-demand-feature-views list",
						"feast projects list",
					},
					ContainerConfigs: feastdevv1alpha1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1alpha1.DefaultCtrConfigs{
							Image: &image,
						},
						OptionalCtrConfigs: feastdevv1alpha1.OptionalCtrConfigs{
							Env: &[]corev1.EnvVar{
								{
									Name:  "test",
									Value: "var",
								},
							},
							ImagePullPolicy: &always,
						},
					},
				},
				JobSpec: &feastdevv1alpha1.JobSpec{
					Parallelism:             &int32Var,
					Completions:             &int32Var,
					ActiveDeadlineSeconds:   &int64Var,
					BackoffLimit:            &int32Var,
					BackoffLimitPerIndex:    &int32Var,
					MaxFailedIndexes:        &int32Var,
					TTLSecondsAfterFinished: &int32Var,
					CompletionMode:          &completionMode,
					Suspend:                 &suspend,
					PodReplacementPolicy:    &replacementPolicy,
				},
			}
			err = k8sClient.Update(ctx, resource)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			feast := services.FeastServices{
				Handler: handler.FeastHandler{
					Client:       controllerReconciler.Client,
					Context:      ctx,
					Scheme:       controllerReconciler.Scheme,
					FeatureStore: resource,
				},
			}

			objMeta := feast.GetObjectMeta()
			Expect(resource.Status).NotTo(BeNil())
			Expect(resource.Status.CronJob).To(Equal(objMeta.Name))
			Expect(resource.Status.Applied.CronJob.Schedule).NotTo(BeEmpty())
			Expect(resource.Status.Applied.CronJob.Suspend).To(Equal(&suspend))
			Expect(resource.Status.Applied.CronJob.ConcurrencyPolicy).To(Equal(batchv1.ForbidConcurrent))
			Expect(resource.Status.Applied.CronJob.FailedJobsHistoryLimit).To(Equal(&int32Var))

			Expect(resource.Status.Conditions).NotTo(BeEmpty())
			cond := apimeta.FindStatusCondition(resource.Status.Conditions, feastdevv1alpha1.CronJobReadyType)
			Expect(cond).ToNot(BeNil())
			Expect(cond.Type).To(Equal(feastdevv1alpha1.CronJobReadyType))
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(feastdevv1alpha1.ReadyReason))
			Expect(cond.Message).To(Equal(feastdevv1alpha1.CronJobReadyMessage))

			// check CronJob
			cronJob := &batchv1.CronJob{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      objMeta.Name,
				Namespace: objMeta.Namespace,
			}, cronJob)
			Expect(err).NotTo(HaveOccurred())

			Expect(cronJob.Spec.Schedule).To(Equal(schedule))
			Expect(cronJob.Spec.StartingDeadlineSeconds).To(Equal(&startingDeadlineSeconds))
			Expect(cronJob.Spec.JobTemplate.Spec.Parallelism).To(Equal(&int32Var))

			checkCronJob(resource.Status.Applied.CronJob, cronJob.Spec)
		})
	})
})

func checkCronJob(appliedFeastCronJob *feastdevv1alpha1.FeastCronJob, cronSpec batchv1.CronJobSpec) {
	Expect(appliedFeastCronJob).NotTo(BeNil())
	Expect(appliedFeastCronJob.Schedule).To(Equal(cronSpec.Schedule))
	if len(appliedFeastCronJob.ConcurrencyPolicy) > 0 {
		Expect(appliedFeastCronJob.ConcurrencyPolicy).To(Equal(cronSpec.ConcurrencyPolicy))
	}
	if appliedFeastCronJob.FailedJobsHistoryLimit != nil {
		Expect(appliedFeastCronJob.FailedJobsHistoryLimit).To(Equal(cronSpec.FailedJobsHistoryLimit))
	}
	if appliedFeastCronJob.StartingDeadlineSeconds != nil {
		Expect(appliedFeastCronJob.StartingDeadlineSeconds).To(Equal(cronSpec.StartingDeadlineSeconds))
	}
	if appliedFeastCronJob.SuccessfulJobsHistoryLimit != nil {
		Expect(appliedFeastCronJob.SuccessfulJobsHistoryLimit).To(Equal(cronSpec.SuccessfulJobsHistoryLimit))
	}
	if appliedFeastCronJob.Suspend != nil {
		Expect(appliedFeastCronJob.Suspend).To(Equal(cronSpec.Suspend))
	}
	if appliedFeastCronJob.TimeZone != nil {
		Expect(appliedFeastCronJob.TimeZone).To(Equal(cronSpec.TimeZone))
	}
	checkJobSpec(appliedFeastCronJob.JobSpec, cronSpec.JobTemplate.Spec)
	checkCronJobContainers(appliedFeastCronJob.ContainerConfigs, cronSpec.JobTemplate.Spec.Template.Spec)
}

func checkJobSpec(appliedFeastJobSpec *feastdevv1alpha1.JobSpec, jobSpec batchv1.JobSpec) {
	if appliedFeastJobSpec != nil {
		if appliedFeastJobSpec.ActiveDeadlineSeconds != nil {
			Expect(appliedFeastJobSpec.ActiveDeadlineSeconds).To(Equal(jobSpec.ActiveDeadlineSeconds))
		}
		if appliedFeastJobSpec.BackoffLimit != nil {
			Expect(appliedFeastJobSpec.BackoffLimit).To(Equal(jobSpec.BackoffLimit))
		}
		if appliedFeastJobSpec.BackoffLimitPerIndex != nil {
			Expect(appliedFeastJobSpec.BackoffLimitPerIndex).To(Equal(jobSpec.BackoffLimitPerIndex))
		}
		if appliedFeastJobSpec.CompletionMode != nil {
			Expect(appliedFeastJobSpec.CompletionMode).To(Equal(jobSpec.CompletionMode))
		}
		if appliedFeastJobSpec.Completions != nil {
			Expect(appliedFeastJobSpec.Completions).To(Equal(jobSpec.Completions))
		}
		if appliedFeastJobSpec.MaxFailedIndexes != nil {
			Expect(appliedFeastJobSpec.MaxFailedIndexes).To(Equal(jobSpec.MaxFailedIndexes))
		}
		if appliedFeastJobSpec.Parallelism != nil {
			Expect(appliedFeastJobSpec.Parallelism).To(Equal(jobSpec.Parallelism))
		}
		if appliedFeastJobSpec.PodFailurePolicy != nil {
			Expect(appliedFeastJobSpec.PodFailurePolicy).To(Equal(jobSpec.PodFailurePolicy))
		}
		if appliedFeastJobSpec.PodReplacementPolicy != nil {
			Expect(appliedFeastJobSpec.PodReplacementPolicy).To(Equal(jobSpec.PodReplacementPolicy))
		}
		if appliedFeastJobSpec.Suspend != nil {
			Expect(appliedFeastJobSpec.Suspend).To(Equal(jobSpec.Suspend))
		}
		if appliedFeastJobSpec.TTLSecondsAfterFinished != nil {
			Expect(appliedFeastJobSpec.TTLSecondsAfterFinished).To(Equal(jobSpec.TTLSecondsAfterFinished))
		}
	}
}

func checkCronJobContainers(cronJobContainerConfigs *feastdevv1alpha1.CronJobContainerConfigs, podSpec corev1.PodSpec) {
	Expect(cronJobContainerConfigs).NotTo(BeNil())
	Expect(cronJobContainerConfigs.Image).NotTo(BeNil())
	Expect(cronJobContainerConfigs.Commands).NotTo(BeEmpty())
	Expect(cronJobContainerConfigs.Commands).To(HaveLen(len(podSpec.InitContainers) + len(podSpec.Containers)))

	lastCmdIndex := len(cronJobContainerConfigs.Commands) - 1
	for i, cmd := range cronJobContainerConfigs.Commands {
		if i < lastCmdIndex {
			Expect(podSpec.InitContainers[i].Command).To(ContainElement(cmd))
		} else {
			Expect(podSpec.Containers[0].Command).To(ContainElement(cmd))
		}
	}
}
