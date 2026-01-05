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
// of executing a Feast Jupyter notebook within a fully configured OpenShift namespace
package e2erhoai

import (
	"fmt"

	utils "github.com/feast-dev/feast/infra/feast-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Feast Jupyter Notebook Testing", Ordered, func() {
	const (
		namespace       = "test-ns-feast-wb"
		configMapName   = "feast-wb-cm"
		rolebindingName = "rb-feast-test"
		notebookFile    = "test/e2e_rhoai/resources/feast-wb-milvus-test.ipynb"
		pvcFile         = "test/e2e_rhoai/resources/pvc.yaml"
		notebookPVC     = "jupyterhub-nb-kube-3aadmin-pvc"
		testDir         = "/test/e2e_rhoai"
		notebookName    = "feast-wb-milvus-test.ipynb"
		feastMilvusTest = "TestFeastMilvusNotebook"
	)

	BeforeAll(func() {
		By(fmt.Sprintf("Creating test namespace: %s", namespace))
		Expect(utils.CreateNamespace(namespace, testDir)).To(Succeed())
		fmt.Printf("Namespace %s created successfully\n", namespace)
	})

	AfterAll(func() {
		By(fmt.Sprintf("Deleting test namespace: %s", namespace))
		Expect(utils.DeleteNamespace(namespace, testDir)).To(Succeed())
		fmt.Printf("Namespace %s deleted successfully\n", namespace)
	})

	Context("Feast Jupyter Notebook Test", func() {
		It("Should create and run a "+feastMilvusTest+" successfully", func() {
			// Create notebook with all setup steps
			// Pass empty string for feastProject to keep annotation empty
			utils.CreateNotebookTest(namespace, configMapName, notebookFile, "test/e2e_rhoai/resources/feature_repo", pvcFile, rolebindingName, notebookPVC, notebookName, testDir, "")

			// Monitor notebook execution
			utils.MonitorNotebookTest(namespace, notebookName)
		})
	})
})
