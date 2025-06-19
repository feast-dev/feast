/*
Copyright 2025 Feast Community.

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

package e2erhoai

import (
	"fmt"

	. "github.com/feast-dev/feast/infra/feast-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Feast PreUpgrade scenario Testing", Ordered, func() {
	const (
		namespace           = "test-ns-feast-upgrade"
		replaceNamespace    = "test-ns-feast"
		testDir             = "/test/e2e_rhoai"
		feastDeploymentName = FeastPrefix + "credit-scoring"
		feastCRName         = "credit-scoring"
	)

	filesToUpdateNamespace := []string{
		"test/testdata/feast_integration_test_crs/postgres.yaml",
		"test/testdata/feast_integration_test_crs/redis.yaml",
		"test/testdata/feast_integration_test_crs/feast.yaml",
	}

	BeforeAll(func() {
		By(fmt.Sprintf("Creating test namespace: %s", namespace))
		Expect(CreateNamespace(namespace, testDir)).To(Succeed())
		fmt.Printf("Namespace %s created successfully\n", namespace)

		By("Replacing placeholder namespace in CR YAMLs for test setup")
		Expect(ReplaceNamespaceInYamlFilesInPlace(filesToUpdateNamespace, replaceNamespace, namespace)).To(Succeed())
	})

	AfterAll(func() {
		By("Restoring original namespace in CR YAMLs")
		Expect(ReplaceNamespaceInYamlFilesInPlace(filesToUpdateNamespace, namespace, replaceNamespace)).To(Succeed())

		if CurrentSpecReport().Failed() {
			By(fmt.Sprintf("Deleting test namespace: %s", namespace))
			Expect(DeleteNamespace(namespace, testDir)).To(Succeed())
			fmt.Printf("Namespace %s deleted successfully\n", namespace)
		}
	})

	runPreUpgradeTest := func() {
		By("Applying Feast infra manifests and verifying setup")
		ApplyFeastInfraManifestsAndVerify(namespace, testDir)

		By("Applying and validating the credit-scoring FeatureStore CR")
		ApplyFeastYamlAndVerify(namespace, testDir, feastDeploymentName, feastCRName)
	}

	// This context ensures the Feast CR setup is functional prior to any upgrade
	Context("Feast Pre Upgrade Test", func() {
		It("Should create and run a feastPreUpgrade test scenario feast credit-scoring CR setup successfully", runPreUpgradeTest)
	})
})
