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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
)

var _ = Describe("Batch Engine RBAC", func() {

	Describe("loadBatchEngineTemplate", func() {
		It("should load spark_application template", func() {
			tmpl, err := loadBatchEngineTemplate("spark_application")
			Expect(err).NotTo(HaveOccurred())
			Expect(tmpl).NotTo(BeNil())
			Expect(tmpl.EngineType).To(Equal("spark_application"))
			Expect(tmpl.Server).NotTo(BeNil())
			Expect(tmpl.Server.Rules).NotTo(BeEmpty())
			Expect(tmpl.Driver).NotTo(BeNil())
			Expect(tmpl.Driver.CreateServiceAccount).To(BeTrue())
			Expect(tmpl.Driver.Rules).NotTo(BeEmpty())
		})

		It("should return nil for unknown engine type", func() {
			tmpl, err := loadBatchEngineTemplate("nonexistent_engine")
			Expect(err).NotTo(HaveOccurred())
			Expect(tmpl).To(BeNil())
		})

		It("should contain correct server rules for spark_application", func() {
			tmpl, err := loadBatchEngineTemplate("spark_application")
			Expect(err).NotTo(HaveOccurred())

			serverRules := tmpl.Server.Rules
			Expect(serverRules).To(HaveLen(4))

			hasConfigMapRule := false
			hasSparkAppRule := false
			hasPodListRule := false
			hasPodLogRule := false

			for _, rule := range serverRules {
				if containsResource(rule, "configmaps") && containsVerb(rule, "create") && containsVerb(rule, "delete") {
					hasConfigMapRule = true
				}
				if containsResource(rule, "sparkapplications") && containsVerb(rule, "create") && containsVerb(rule, "get") && containsVerb(rule, "delete") {
					hasSparkAppRule = true
				}
				if containsResource(rule, "pods") && containsVerb(rule, "list") {
					hasPodListRule = true
				}
				if containsResource(rule, "pods/log") && containsVerb(rule, "get") {
					hasPodLogRule = true
				}
			}

			Expect(hasConfigMapRule).To(BeTrue(), "should have configmaps create/delete rule")
			Expect(hasSparkAppRule).To(BeTrue(), "should have sparkapplications create/get/delete rule")
			Expect(hasPodListRule).To(BeTrue(), "should have pods list rule")
			Expect(hasPodLogRule).To(BeTrue(), "should have pods/log get rule")
		})

		It("should contain correct driver rules for spark_application", func() {
			tmpl, err := loadBatchEngineTemplate("spark_application")
			Expect(err).NotTo(HaveOccurred())

			driverRules := tmpl.Driver.Rules
			Expect(driverRules).To(HaveLen(2))

			hasPodRule := false
			hasResourceRule := false
			for _, rule := range driverRules {
				if containsResource(rule, "pods") &&
					containsVerb(rule, "create") &&
					containsVerb(rule, "deletecollection") {
					hasPodRule = true
				}
				if containsResource(rule, "services") &&
					containsResource(rule, "configmaps") &&
					containsResource(rule, "persistentvolumeclaims") &&
					containsVerb(rule, "deletecollection") {
					hasResourceRule = true
				}
			}
			Expect(hasPodRule).To(BeTrue(), "should have pods CRUD + deletecollection rule")
			Expect(hasResourceRule).To(BeTrue(), "should have services/configmaps/PVCs CRUD + deletecollection rule")
		})
	})

	Describe("BatchEngineRBACTemplate YAML parsing", func() {
		It("should correctly unmarshal a template", func() {
			yamlData := `
engine_type: test_engine
server:
  rules:
    - apiGroups: [""]
      resources: ["secrets"]
      verbs: ["get", "list"]
driver:
  create_service_account: true
  rules:
    - apiGroups: [""]
      resources: ["pods"]
      verbs: ["get"]
`
			var tmpl BatchEngineRBACTemplate
			err := yaml.Unmarshal([]byte(yamlData), &tmpl)
			Expect(err).NotTo(HaveOccurred())
			Expect(tmpl.EngineType).To(Equal("test_engine"))
			Expect(tmpl.Server).NotTo(BeNil())
			Expect(tmpl.Server.Rules).To(HaveLen(1))
			Expect(tmpl.Driver).NotTo(BeNil())
			Expect(tmpl.Driver.CreateServiceAccount).To(BeTrue())
			Expect(tmpl.Driver.Rules).To(HaveLen(1))
		})

		It("should handle server-only template (no driver)", func() {
			yamlData := `
engine_type: server_only
server:
  rules:
    - apiGroups: ["batch"]
      resources: ["jobs"]
      verbs: ["create", "delete"]
`
			var tmpl BatchEngineRBACTemplate
			err := yaml.Unmarshal([]byte(yamlData), &tmpl)
			Expect(err).NotTo(HaveOccurred())
			Expect(tmpl.Server).NotTo(BeNil())
			Expect(tmpl.Driver).To(BeNil())
		})
	})
})

var _ = Describe("resolveBatchDriverSAName", func() {
	It("defaults to feast-<name>-batch-driver when service_account is omitted", func() {
		fs := &feastdevv1.FeatureStore{ObjectMeta: metav1.ObjectMeta{Name: "spark-pg-e2e", Namespace: "feast-spark"}}
		Expect(resolveBatchDriverSAName(fs, map[string]interface{}{
			"type":  "spark_application",
			"image": "quay.io/example/driver:v1",
		})).To(Equal("feast-spark-pg-e2e-batch-driver"))
	})

	It("defaults when service_account is empty string", func() {
		fs := &feastdevv1.FeatureStore{ObjectMeta: metav1.ObjectMeta{Name: "spark-pg-e2e"}}
		Expect(resolveBatchDriverSAName(fs, map[string]interface{}{
			"service_account": "",
		})).To(Equal("feast-spark-pg-e2e-batch-driver"))
	})

	It("keeps an explicit service_account override", func() {
		fs := &feastdevv1.FeatureStore{ObjectMeta: metav1.ObjectMeta{Name: "spark-pg-e2e"}}
		Expect(resolveBatchDriverSAName(fs, map[string]interface{}{
			"service_account": "my-custom-driver",
		})).To(Equal("my-custom-driver"))
	})
})

func containsResource(rule rbacv1.PolicyRule, resource string) bool {
	for _, r := range rule.Resources {
		if r == resource {
			return true
		}
	}
	return false
}

func containsVerb(rule rbacv1.PolicyRule, verb string) bool {
	for _, v := range rule.Verbs {
		if v == verb {
			return true
		}
	}
	return false
}
