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

package services

import (
	"context"
	"os"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("getNamespaceRegistryNamespace", func() {
	var feast *FeastServices

	BeforeEach(func() {
		feast = &FeastServices{
			Handler: handler.FeastHandler{
				Context: context.Background(),
				FeatureStore: &feastdevv1.FeatureStore{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-fs",
						Namespace: "default",
					},
				},
			},
		}
		os.Unsetenv("FEAST_OPERATOR_NAMESPACE")
		os.Unsetenv("POD_NAMESPACE")
	})

	AfterEach(func() {
		os.Unsetenv("FEAST_OPERATOR_NAMESPACE")
		os.Unsetenv("POD_NAMESPACE")
	})

	It("should return FEAST_OPERATOR_NAMESPACE when set", func() {
		os.Setenv("FEAST_OPERATOR_NAMESPACE", "custom-ns")
		Expect(feast.getNamespaceRegistryNamespace()).To(Equal("custom-ns"))
	})

	It("should prefer FEAST_OPERATOR_NAMESPACE over POD_NAMESPACE", func() {
		os.Setenv("FEAST_OPERATOR_NAMESPACE", "feast-ns")
		os.Setenv("POD_NAMESPACE", "pod-ns")
		Expect(feast.getNamespaceRegistryNamespace()).To(Equal("feast-ns"))
	})

	It("should return POD_NAMESPACE when FEAST_OPERATOR_NAMESPACE is not set and SA file is missing", func() {
		os.Setenv("POD_NAMESPACE", "my-pod-ns")
		Expect(feast.getNamespaceRegistryNamespace()).To(Equal("my-pod-ns"))
	})

	It("should fall back to DefaultKubernetesNamespace when no env vars are set and SA file is missing", func() {
		Expect(feast.getNamespaceRegistryNamespace()).To(Equal(DefaultKubernetesNamespace))
	})

	It("should ignore empty FEAST_OPERATOR_NAMESPACE", func() {
		os.Setenv("FEAST_OPERATOR_NAMESPACE", "")
		os.Setenv("POD_NAMESPACE", "pod-ns")
		Expect(feast.getNamespaceRegistryNamespace()).To(Equal("pod-ns"))
	})

	It("should ignore empty POD_NAMESPACE and fall back to default", func() {
		os.Setenv("POD_NAMESPACE", "")
		Expect(feast.getNamespaceRegistryNamespace()).To(Equal(DefaultKubernetesNamespace))
	})
})
