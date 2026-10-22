/*
Copyright 2026 Feast Community.

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

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Packaged feature repositories", func() {
	const (
		resourceName    = "packaged-feature-repo"
		packagedImage   = "registry.example.com/feature-server@sha256:0123456789abcdef"
		packagedRepoDir = "/opt/feast/feature_repo"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: "default"}

	newFeatureStore := func() *feastdevv1.FeatureStore {
		return &feastdevv1.FeatureStore{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: feastProject,
				FeastProjectDir: &feastdevv1.FeastProjectDir{
					Packaged: &feastdevv1.FeastPackagedOptions{
						Image:           packagedImage,
						FeatureRepoPath: packagedRepoDir,
					},
				},
			},
		}
	}

	reconcileFeatureStore := func() (*feastdevv1.FeatureStore, *appsv1.Deployment) {
		reconciler := &FeatureStoreReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		featureStore := &feastdevv1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
		feastServices := services.FeastServices{
			Handler: handler.FeastHandler{
				Client:       k8sClient,
				Context:      ctx,
				Scheme:       k8sClient.Scheme(),
				FeatureStore: featureStore,
			},
		}
		deployment := &appsv1.Deployment{}
		meta := feastServices.GetObjectMeta()
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: meta.Name, Namespace: meta.Namespace}, deployment)).To(Succeed())
		return featureStore, deployment
	}

	BeforeEach(func() {
		Expect(k8sClient.Create(ctx, newFeatureStore())).To(Succeed())
	})

	AfterEach(func() {
		featureStore := &feastdevv1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
		Expect(k8sClient.Delete(ctx, featureStore)).To(Succeed())
	})

	It("stages the packaged repository and applies it from the shared directory", func() {
		featureStore, deployment := reconcileFeatureStore()

		canonicalRepoDir := services.EphemeralPath + "/" + feastProject + "/" + services.FeatureRepoDir
		Expect(deployment.Spec.Template.Spec.InitContainers).To(HaveLen(2))
		initContainer := deployment.Spec.Template.Spec.InitContainers[0]
		Expect(initContainer.Name).To(Equal("feast-init"))
		Expect(initContainer.Image).To(Equal(packagedImage))
		Expect(initContainer.WorkingDir).To(Equal(services.EphemeralPath))
		Expect(initContainer.Env).To(ContainElements(
			corev1.EnvVar{Name: "FEAST_PACKAGED_FEATURE_REPO_PATH", Value: packagedRepoDir},
			corev1.EnvVar{Name: "FEAST_STAGED_FEATURE_REPO_PATH", Value: canonicalRepoDir},
		))
		Expect(initContainer.Args).To(HaveLen(1))
		Expect(initContainer.Args[0]).To(ContainSubstring(`rm -rf -- "${FEAST_STAGED_FEATURE_REPO_PATH}"`))
		Expect(initContainer.Args[0]).To(ContainSubstring(`cp -a -- "${FEAST_PACKAGED_FEATURE_REPO_PATH}/." "${FEAST_STAGED_FEATURE_REPO_PATH}/"`))
		Expect(initContainer.Args[0]).To(ContainSubstring(`printf '%s' "${TMP_FEATURE_STORE_YAML_BASE64}" | base64 -d`))
		Expect(initContainer.Args[0]).To(ContainSubstring(`"${FEAST_STAGED_FEATURE_REPO_PATH}/feature_store.yaml"`))

		applyContainer := deployment.Spec.Template.Spec.InitContainers[1]
		Expect(applyContainer.Name).To(Equal("feast-apply"))
		Expect(applyContainer.Image).To(Equal(packagedImage))
		Expect(applyContainer.Command).To(Equal([]string{"feast", "apply"}))
		Expect(applyContainer.WorkingDir).To(Equal(canonicalRepoDir))

		online := services.GetOnlineContainer(*deployment)
		Expect(online.Image).To(Equal(packagedImage))
		Expect(online.WorkingDir).To(Equal(canonicalRepoDir))
		Expect(*featureStore.Status.Applied.Services.OnlineStore.Server.Image).To(Equal(packagedImage))
	})

	It("supports staging without applying and direct use of the baked repository", func() {
		featureStore := &feastdevv1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
		featureStore.Spec.Services = &feastdevv1.FeatureStoreServices{RunFeastApplyOnInit: ptr(false)}
		Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())

		featureStore, deployment := reconcileFeatureStore()
		Expect(deployment.Spec.Template.Spec.InitContainers).To(HaveLen(1))
		Expect(deployment.Spec.Template.Spec.InitContainers[0].Name).To(Equal("feast-init"))

		featureStore.Spec.Services.DisableInitContainers = true
		Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())
		_, deployment = reconcileFeatureStore()
		Expect(deployment.Spec.Template.Spec.InitContainers).To(BeEmpty())
		online := services.GetOnlineContainer(*deployment)
		Expect(online.Image).To(Equal(packagedImage))
		Expect(online.WorkingDir).To(Equal(packagedRepoDir))
	})

	It("keeps explicit service images ahead of the packaged image", func() {
		const serviceImage = "registry.example.com/online-server:custom"
		featureStore := &feastdevv1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
		featureStore.Spec.Services = &feastdevv1.FeatureStoreServices{
			OnlineStore: &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{Image: ptr(serviceImage)},
					},
				},
			},
		}
		Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())

		_, deployment := reconcileFeatureStore()
		Expect(deployment.Spec.Template.Spec.InitContainers[0].Image).To(Equal(packagedImage))
		Expect(services.GetOnlineContainer(*deployment).Image).To(Equal(serviceImage))
	})

	It("keeps an explicit init image ahead of the packaged image", func() {
		const initImage = "registry.example.com/feast-init:custom"
		featureStore := &feastdevv1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
		featureStore.Spec.Services = &feastdevv1.FeatureStoreServices{
			InitImage: ptr(initImage),
		}
		Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())

		_, deployment := reconcileFeatureStore()
		Expect(deployment.Spec.Template.Spec.InitContainers).To(HaveLen(2))
		Expect(deployment.Spec.Template.Spec.InitContainers[0].Image).To(Equal(initImage))
		Expect(deployment.Spec.Template.Spec.InitContainers[1].Image).To(Equal(initImage))
		Expect(services.GetOnlineContainer(*deployment).Image).To(Equal(packagedImage))
	})

	It("supports path-only direct mode with an explicit service image", func() {
		const serviceImage = "registry.example.com/online-server:air-gapped"
		featureStore := &feastdevv1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
		featureStore.Spec.FeastProjectDir.Packaged.Image = ""
		featureStore.Spec.Services = &feastdevv1.FeatureStoreServices{
			DisableInitContainers: true,
			OnlineStore: &feastdevv1.OnlineStore{
				Server: &feastdevv1.ServerConfigs{
					ContainerConfigs: feastdevv1.ContainerConfigs{
						DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{Image: ptr(serviceImage)},
					},
				},
			},
		}
		Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())

		_, deployment := reconcileFeatureStore()
		Expect(deployment.Spec.Template.Spec.InitContainers).To(BeEmpty())
		online := services.GetOnlineContainer(*deployment)
		Expect(online.Image).To(Equal(serviceImage))
		Expect(online.WorkingDir).To(Equal(packagedRepoDir))
	})

	It("retains the operator image fallback when the packaged image is omitted", func() {
		featureStore := &feastdevv1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
		featureStore.Spec.FeastProjectDir.Packaged.Image = ""
		Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())

		_, deployment := reconcileFeatureStore()
		initImage := deployment.Spec.Template.Spec.InitContainers[0].Image
		Expect(initImage).NotTo(BeEmpty())
		Expect(services.GetOnlineContainer(*deployment).Image).To(Equal(initImage))
	})

	DescribeTable("rejects packaged and staged repository path overlap",
		func(featureRepoPath string) {
			featureStore := &feastdevv1.FeatureStore{}
			Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
			featureStore.Spec.FeastProjectDir.Packaged.FeatureRepoPath = featureRepoPath
			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())

			reconciler := &FeatureStoreReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).To(MatchError(ContainSubstring("overlaps staged repository path")))
		},
		Entry("equal paths", services.EphemeralPath+"/"+feastProject+"/"+services.FeatureRepoDir),
		Entry("packaged path is an ancestor", services.EphemeralPath+"/"+feastProject),
		Entry("packaged path is a descendant", services.EphemeralPath+"/"+feastProject+"/"+services.FeatureRepoDir+"/baked"),
	)

	It("allows similar path prefixes that do not overlap", func() {
		featureStore := &feastdevv1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, featureStore)).To(Succeed())
		featureStore.Spec.FeastProjectDir.Packaged.FeatureRepoPath =
			services.EphemeralPath + "/" + feastProject + "/" + services.FeatureRepoDir + "-image"
		Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())

		reconcileFeatureStore()
	})
})
