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

package api

import (
	"context"
	"strings"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type packagedFeatureStoreFactory func(name, featureRepoPath string) client.Object
type conflictingPackagedFeatureStoreFactory func(name, conflictingMode string) client.Object

func newV1PackagedFeatureStore(name, featureRepoPath string) client.Object {
	return &feastdevv1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespaceName},
		Spec: feastdevv1.FeatureStoreSpec{
			FeastProject: "test_project",
			FeastProjectDir: &feastdevv1.FeastProjectDir{
				Packaged: &feastdevv1.FeastPackagedOptions{FeatureRepoPath: featureRepoPath},
			},
		},
	}
}

func newV1Alpha1PackagedFeatureStore(name, featureRepoPath string) client.Object {
	return &feastdevv1alpha1.FeatureStore{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespaceName},
		Spec: feastdevv1alpha1.FeatureStoreSpec{
			FeastProject: "test_project",
			FeastProjectDir: &feastdevv1alpha1.FeastProjectDir{
				Packaged: &feastdevv1alpha1.FeastPackagedOptions{FeatureRepoPath: featureRepoPath},
			},
		},
	}
}

func newV1ConflictingPackagedFeatureStore(name, conflictingMode string) client.Object {
	featureStore := newV1PackagedFeatureStore(name, "/opt/feast/feature_repo").(*feastdevv1.FeatureStore)
	switch conflictingMode {
	case "init":
		featureStore.Spec.FeastProjectDir.Init = &feastdevv1.FeastInitOptions{}
	case "git":
		featureStore.Spec.FeastProjectDir.Git = &feastdevv1.GitCloneOptions{
			URL: "https://example.com/feature-repo.git",
		}
	}
	return featureStore
}

func newV1Alpha1ConflictingPackagedFeatureStore(name, conflictingMode string) client.Object {
	featureStore := newV1Alpha1PackagedFeatureStore(name, "/opt/feast/feature_repo").(*feastdevv1alpha1.FeatureStore)
	switch conflictingMode {
	case "init":
		featureStore.Spec.FeastProjectDir.Init = &feastdevv1alpha1.FeastInitOptions{}
	case "git":
		featureStore.Spec.FeastProjectDir.Git = &feastdevv1alpha1.GitCloneOptions{
			URL: "https://example.com/feature-repo.git",
		}
	}
	return featureStore
}

var _ = Describe("Packaged feature repository path validation", func() {
	ctx := context.Background()
	apiVersions := []struct {
		name               string
		id                 string
		factory            packagedFeatureStoreFactory
		conflictingFactory conflictingPackagedFeatureStoreFactory
	}{
		{
			name:               "feast.dev/v1",
			id:                 "v1",
			factory:            newV1PackagedFeatureStore,
			conflictingFactory: newV1ConflictingPackagedFeatureStore,
		},
		{
			name:               "feast.dev/v1alpha1",
			id:                 "v1alpha1",
			factory:            newV1Alpha1PackagedFeatureStore,
			conflictingFactory: newV1Alpha1ConflictingPackagedFeatureStore,
		},
	}

	for _, apiVersion := range apiVersions {
		apiVersion := apiVersion
		Context(apiVersion.name, func() {
			DescribeTable("accepts canonical absolute non-root paths",
				func(nameSuffix, featureRepoPath string) {
					featureStore := apiVersion.factory(
						"packaged-"+apiVersion.id+"-"+nameSuffix,
						featureRepoPath,
					)
					Expect(k8sClient.Create(ctx, featureStore)).To(Succeed())
					Expect(k8sClient.Delete(ctx, featureStore)).To(Succeed())
				},
				Entry("standard", "standard", "/opt/feast/feature_repo"),
				Entry("hidden component", "hidden", "/opt/.feast/feature_repo"),
				Entry("dot in component", "dot-name", "/opt/feature_repo.v2"),
			)

			DescribeTable("rejects non-canonical, relative, or root paths",
				func(nameSuffix, featureRepoPath string) {
					featureStore := apiVersion.factory(
						"packaged-"+apiVersion.id+"-"+nameSuffix,
						featureRepoPath,
					)
					err := k8sClient.Create(ctx, featureStore)
					Expect(err).To(HaveOccurred())
					Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected invalid error, got %v", err)
					Expect(strings.ToLower(err.Error())).To(ContainSubstring("canonical absolute, non-root path"))
				},
				Entry("relative", "relative", "opt/feast/feature_repo"),
				Entry("root", "root", "/"),
				Entry("parent collapses to root", "parent-root", "/opt/.."),
				Entry("leading parent traversal", "leading-parent", "/../x"),
				Entry("repeated separator", "repeated-separator", "/opt//feature_repo"),
				Entry("current-directory component", "current-dir", "/opt/./feature_repo"),
				Entry("trailing separator", "trailing-separator", "/opt/feature_repo/"),
				Entry("nested traversal", "nested-traversal", "/a/../../etc"),
				Entry("repeated root separator", "repeated-root", "//"),
			)

			DescribeTable("rejects packaged together with another project directory mode",
				func(nameSuffix, conflictingMode string) {
					featureStore := apiVersion.conflictingFactory(
						"packaged-"+apiVersion.id+"-"+nameSuffix,
						conflictingMode,
					)
					err := k8sClient.Create(ctx, featureStore)
					Expect(err).To(HaveOccurred())
					Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected invalid error, got %v", err)
					Expect(err.Error()).To(ContainSubstring("One selection required between init, git, or packaged"))
				},
				Entry("init", "with-init", "init"),
				Entry("git", "with-git", "git"),
			)
		})
	}
})
