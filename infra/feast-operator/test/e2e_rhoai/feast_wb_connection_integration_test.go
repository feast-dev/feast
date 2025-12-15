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

// Package e2erhoai provides end-to-end (E2E) test coverage for Feast integration with
// Red Hat OpenShift AI (RHOAI) environments.
// This specific test validates the functionality
// of executing a Feast workbench integration connection with kubernetes auth and without auth successfully
package e2erhoai

import (
	"fmt"

	utils "github.com/feast-dev/feast/infra/feast-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Feast Workbench Integration Connection Testing", Ordered, func() {
	const (
		namespace           = "test-ns-feast"
		configMapName       = "feast-wb-cm"
		rolebindingName     = "rb-feast-test"
		notebookFile        = "test/e2e_rhoai/resources/feast-wb-connection-credit-scoring.ipynb"
		pvcFile             = "test/e2e_rhoai/resources/pvc.yaml"
		permissionFile      = "test/e2e_rhoai/resources/permissions.py"
		notebookPVC         = "jupyterhub-nb-kube-3aadmin-pvc"
		testDir             = "/test/e2e_rhoai"
		notebookName        = "feast-wb-connection-credit-scoring.ipynb"
		feastDeploymentName = utils.FeastPrefix + "credit-scoring"
		feastCRName         = "credit-scoring"
	)

	// Create and monitor notebook
	createAndMonitorNotebook := func() {
		nbParams := utils.GetNotebookParams(namespace, configMapName, notebookPVC, notebookName, testDir)
		By("Creating Jupyter Notebook")
		Expect(utils.CreateNotebook(nbParams)).To(Succeed(), "Failed to create notebook")

		By("Monitoring notebook logs")
		Expect(utils.MonitorNotebookPod(namespace, "jupyter-nb-", notebookName)).To(Succeed(), "Notebook execution failed")
	}

	// Parameterized test function that handles both auth and non-auth scenarios
	runFeastWorkbenchIntegration := func(authEnabled bool) {
		// Apply permissions only if auth is enabled
		if authEnabled {
			By("Applying Feast permissions for kubernetes authenticated scenario")
			utils.ApplyFeastPermissions(permissionFile, "/feast-data/credit_scoring_local/feature_repo/permissions.py", namespace, feastDeploymentName)
		}

		By(fmt.Sprintf("Setting namespace context to : %s", namespace))
		Expect(utils.SetNamespaceContext(namespace, testDir)).To(Succeed())
		fmt.Printf("Successfully set namespace context to: %s\n", namespace)

		By(fmt.Sprintf("Creating Config map: %s", configMapName))
		Expect(utils.CreateNotebookConfigMap(namespace, configMapName, notebookFile, "test/e2e_rhoai/resources/feature_repo", testDir)).To(Succeed())
		fmt.Printf("ConfigMap %s created successfully\n", configMapName)

		By(fmt.Sprintf("Creating Persistent volume claim: %s", notebookPVC))
		Expect(utils.CreateNotebookPVC(pvcFile, testDir)).To(Succeed())
		fmt.Printf("Persistent Volume Claim %s created successfully\n", notebookPVC)

		By(fmt.Sprintf("Creating rolebinding %s for the user", rolebindingName))
		Expect(utils.CreateNotebookRoleBinding(namespace, rolebindingName, utils.GetOCUser(testDir), testDir)).To(Succeed())
		fmt.Printf("Created rolebinding %s successfully\n", rolebindingName)

		// Create and monitor notebook for execution status
		createAndMonitorNotebook()
	}

	BeforeAll(func() {
		By(fmt.Sprintf("Creating test namespace: %s", namespace))
		Expect(utils.CreateNamespace(namespace, testDir)).To(Succeed())
		fmt.Printf("Namespace %s created successfully\n", namespace)

		By("Applying Feast infra manifests and verifying setup")
		utils.ApplyFeastInfraManifestsAndVerify(namespace, testDir)
	})

	AfterAll(func() {
		By(fmt.Sprintf("Deleting test namespace: %s", namespace))
		Expect(utils.DeleteNamespace(namespace, testDir)).To(Succeed())
		fmt.Printf("Namespace %s deleted successfully\n", namespace)
	})

	Context("Feast Workbench Integration Tests - Without Auth", func() {
		BeforeEach(func() {
			By("Applying and validating the credit-scoring FeatureStore CR without auth")
			utils.ApplyFeastYamlAndVerify(namespace, testDir, feastDeploymentName, feastCRName, "test/testdata/feast_integration_test_crs/feast.yaml")

			By("Verify Feature Store CR is in Ready state")
			utils.ValidateFeatureStoreCRStatus(namespace, feastCRName)

			By("Running `feast apply` and `feast materialize-incremental` to validate registry definitions")
			utils.VerifyApplyFeatureStoreDefinitions(namespace, feastCRName, feastDeploymentName)
		})

		It("Should create and run a FeastWorkbenchIntegrationWithoutAuth scenario successfully", func() {
			runFeastWorkbenchIntegration(false)
		})
	})

	Context("Feast Workbench Integration Tests - With Auth", func() {
		BeforeEach(func() {
			By("Applying and validating the credit-scoring FeatureStore CR (with auth)")
			utils.ApplyFeastYamlAndVerify(namespace, testDir, feastDeploymentName, feastCRName, "test/e2e_rhoai/resources/feast_kube_auth.yaml")

			By("Verify Feature Store CR is in Ready state")
			utils.ValidateFeatureStoreCRStatus(namespace, feastCRName)

			By("Running `feast apply` and `feast materialize-incremental` to validate registry definitions")
			utils.VerifyApplyFeatureStoreDefinitions(namespace, feastCRName, feastDeploymentName)
		})

		It("Should create and run a FeastWorkbenchIntegrationWithAuth scenario successfully", func() {
			runFeastWorkbenchIntegration(true)
		})
	})
})
