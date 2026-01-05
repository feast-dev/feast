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
// Red Hat OpenShift AI (RHOAI) environments. This specific test validates the functionality
// of executing a Feast Jupyter notebook with Ray offline store within a fully configured OpenShift namespace
package e2erhoai

import (
	"fmt"
	"os/exec"

	utils "github.com/feast-dev/feast/infra/feast-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Feast Jupyter Notebook Testing with Ray Offline Store", Ordered, func() {
	const (
		namespace          = "test-ns-feast-wb-ray"
		configMapName      = "feast-wb-ray-cm"
		rolebindingName    = "rb-feast-ray-test"
		notebookFile       = "test/e2e_rhoai/resources/feast-wb-ray-test.ipynb"
		pvcFile            = "test/e2e_rhoai/resources/pvc.yaml"
		kueueResourcesFile = "test/e2e_rhoai/resources/kueue_resources_setup.yaml"
		notebookPVC        = "jupyterhub-nb-kube-3aadmin-pvc"
		testDir            = "/test/e2e_rhoai"
		notebookName       = "feast-wb-ray-test.ipynb"
		feastRayTest       = "TestFeastRayOfflineStoreNotebook"
	)

	BeforeAll(func() {
		By(fmt.Sprintf("Creating test namespace: %s", namespace))
		Expect(utils.CreateNamespace(namespace, testDir)).To(Succeed())
		fmt.Printf("Namespace %s created successfully\n", namespace)

		By("Applying Kueue resources setup")
		// Apply with namespace flag - cluster-scoped resources (ResourceFlavor, ClusterQueue) will be applied at cluster level,
		// and namespace-scoped resources (LocalQueue) will be applied in the specified namespace
		cmd := exec.Command("kubectl", "apply", "-f", kueueResourcesFile, "-n", namespace)
		output, err := utils.Run(cmd, testDir)
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf("Failed to apply Kueue resources: %v\nOutput: %s", err, output))
		fmt.Printf("Kueue resources applied successfully\n")
	})

	AfterAll(func() {
		By("Deleting Kueue resources")
		// Delete with namespace flag - will delete namespace-scoped resources from the namespace
		// and cluster-scoped resources from the cluster
		cmd := exec.Command("kubectl", "delete", "-f", kueueResourcesFile, "-n", namespace, "--ignore-not-found=true")
		_, _ = utils.Run(cmd, testDir)
		fmt.Printf("Kueue resources cleanup completed\n")

		By(fmt.Sprintf("Deleting test namespace: %s", namespace))
		Expect(utils.DeleteNamespace(namespace, testDir)).To(Succeed())
		fmt.Printf("Namespace %s deleted successfully\n", namespace)
	})

	Context("Feast Jupyter Notebook Test with Ray Offline store", func() {
		It("Should create and run a "+feastRayTest+" successfully", func() {
			// Create notebook with all setup steps
			// Pass empty string for feastProject to keep annotation empty
			utils.CreateNotebookTest(namespace, configMapName, notebookFile, "test/e2e_rhoai/resources/feature_repo", pvcFile, rolebindingName, notebookPVC, notebookName, testDir, "")

			// Monitor notebook execution
			utils.MonitorNotebookTest(namespace, notebookName)
		})
	})
})
