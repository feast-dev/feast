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
	"time"

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

	// Verify feast ConfigMap
	verifyFeastConfigMap := func(authEnabled bool) {
		feastConfigMapName := "jupyter-nb-kube-3aadmin-feast-config"
		configMapKey := "credit_scoring_local"
		By(fmt.Sprintf("Listing ConfigMaps and verifying %s exists with correct content", feastConfigMapName))

		// Build expected content based on auth type
		expectedContent := []string{
			"project: credit_scoring_local",
		}
		if authEnabled {
			expectedContent = append(expectedContent, "type: kubernetes")
		} else {
			expectedContent = append(expectedContent, "type: no_auth")
		}

		// First, list ConfigMaps and check if target ConfigMap exists
		// Retry with polling since the ConfigMap may be created asynchronously
		const maxRetries = 5
		const retryInterval = 5 * time.Second
		var configMapExists bool
		var err error

		for i := 0; i < maxRetries; i++ {
			exists, listErr := utils.VerifyConfigMapExistsInList(namespace, feastConfigMapName)
			if listErr != nil {
				err = listErr
				if i < maxRetries-1 {
					fmt.Printf("Failed to list ConfigMaps, retrying in %v... (attempt %d/%d)\n", retryInterval, i+1, maxRetries)
					time.Sleep(retryInterval)
					continue
				}
			} else if exists {
				configMapExists = true
				fmt.Printf("ConfigMap %s found in ConfigMap list\n", feastConfigMapName)
				break
			}

			if i < maxRetries-1 {
				fmt.Printf("ConfigMap %s not found in list yet, retrying in %v... (attempt %d/%d)\n", feastConfigMapName, retryInterval, i+1, maxRetries)
				time.Sleep(retryInterval)
			}
		}

		if !configMapExists {
			Expect(err).ToNot(HaveOccurred(), fmt.Sprintf("Failed to find ConfigMap %s in ConfigMap list after %d attempts: %v", feastConfigMapName, maxRetries, err))
		}

		// Once ConfigMap exists in list, verify content (project name and auth type)
		err = utils.VerifyFeastConfigMapContent(namespace, feastConfigMapName, configMapKey, expectedContent)
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf("Failed to verify Feast ConfigMap %s content: %v", feastConfigMapName, err))
		fmt.Printf("Feast ConfigMap %s verified successfully with project and auth type\n", feastConfigMapName)
	}

	// Parameterized test function that handles both auth and non-auth scenarios
	runFeastWorkbenchIntegration := func(authEnabled bool) {
		// Apply permissions only if auth is enabled
		if authEnabled {
			By("Applying Feast permissions for kubernetes authenticated scenario")
			utils.ApplyFeastPermissions(permissionFile, "/feast-data/credit_scoring_local/feature_repo/permissions.py", namespace, feastDeploymentName)
		}

		// Create notebook with all setup steps
		// Pass feastProject parameter to set the opendatahub.io/feast-config annotation
		utils.CreateNotebookTest(namespace, configMapName, notebookFile, "test/e2e_rhoai/resources/feature_repo", pvcFile, rolebindingName, notebookPVC, notebookName, testDir, "credit_scoring_local")

		// Verify Feast ConfigMap was created with correct auth type
		verifyFeastConfigMap(authEnabled)

		// Monitor notebook execution
		utils.MonitorNotebookTest(namespace, notebookName)
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
