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

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("FeatureStore v1alpha1 scheduling configuration", func() {
	It("accepts and preserves tolerations and nodeSelector", func() {
		ctx := context.Background()
		key := types.NamespacedName{Name: "v1alpha1-scheduling", Namespace: namespaceName}
		expectedTolerations := []corev1.Toleration{{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "feast",
			Effect:   corev1.TaintEffectNoSchedule,
		}}
		expectedNodeSelector := map[string]string{"kubernetes.io/os": "linux"}
		featureStore := &feastdevv1alpha1.FeatureStore{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Spec: feastdevv1alpha1.FeatureStoreSpec{
				FeastProject: "test_project",
				Services: &feastdevv1alpha1.FeatureStoreServices{
					Tolerations:  expectedTolerations,
					NodeSelector: expectedNodeSelector,
				},
			},
		}

		Expect(k8sClient.Create(ctx, featureStore)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Delete(ctx, featureStore)).To(Succeed())
		})

		actual := &feastdevv1alpha1.FeatureStore{}
		Expect(k8sClient.Get(ctx, key, actual)).To(Succeed())
		Expect(actual.Spec.Services.Tolerations).To(Equal(expectedTolerations))
		Expect(actual.Spec.Services.NodeSelector).To(Equal(expectedNodeSelector))
	})
})
