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
	"os"
	"os/exec"
	"strings"

	utils "github.com/feast-dev/feast/infra/feast-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Feast Jupyter Notebook Testing", Ordered, func() {
	const (
		namespace       = "test-ns-feast-wb"
		configMapName   = "feast-wb-cm"
		rolebindingName = "rb-feast-test"
		notebookFile    = "test/e2e_rhoai/resources/feast-test.ipynb"
		pvcFile         = "test/e2e_rhoai/resources/pvc.yaml"
		notebookPVC     = "jupyterhub-nb-kube-3aadmin-pvc"
		testDir         = "/test/e2e_rhoai"
		notebookName    = "feast-test.ipynb"
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

	runNotebookTest := func() {
		env := func(key string) string {
			val, _ := os.LookupEnv(key)
			return val
		}

		username := utils.GetOCUser(testDir)

		// set namespace context
		By(fmt.Sprintf("Setting namespace context to : %s", namespace))
		cmd := exec.Command("kubectl", "config", "set-context", "--current", "--namespace", namespace)
		output, err := utils.Run(cmd, "/test/e2e_rhoai")
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
			"Failed to set namespace context to %s.\nError: %v\nOutput: %s\n",
			namespace, err, output,
		))
		fmt.Printf("Successfully set namespace context to: %s\n", namespace)

		// create config map
		By(fmt.Sprintf("Creating Config map: %s", configMapName))
		cmd = exec.Command("kubectl", "create", "configmap", configMapName, "--from-file="+notebookFile, "--from-file=test/e2e_rhoai/resources/feature_repo")
		output, err = utils.Run(cmd, "/test/e2e_rhoai")
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
			"Failed to create ConfigMap %s.\nError: %v\nOutput: %s\n",
			configMapName, err, output,
		))
		fmt.Printf("ConfigMap %s created successfully\n", configMapName)

		// create pvc
		By(fmt.Sprintf("Creating Persistent volume claim: %s", notebookPVC))
		cmd = exec.Command("kubectl", "apply", "-f", "test/e2e_rhoai/resources/pvc.yaml")
		_, err = utils.Run(cmd, "/test/e2e_rhoai")
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		fmt.Printf("Persistent Volume Claim %s created successfully", notebookPVC)

		// create rolebinding
		By(fmt.Sprintf("Creating rolebinding %s for the user", rolebindingName))
		cmd = exec.Command("kubectl", "create", "rolebinding", rolebindingName, "-n", namespace, "--role=admin", "--user="+username)
		_, err = utils.Run(cmd, "/test/e2e_rhoai")
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		fmt.Printf("Created rolebinding %s successfully\n", rolebindingName)

		// configure papermill notebook command execution
		command := []string{
			"/bin/sh",
			"-c",
			fmt.Sprintf(
				"pip install papermill && "+
					"mkdir -p /opt/app-root/src/feature_repo && "+
					"cp -rL /opt/app-root/notebooks/* /opt/app-root/src/feature_repo/ && "+
					"oc login --token=%s --server=%s --insecure-skip-tls-verify=true && "+
					"(papermill /opt/app-root/notebooks/%s /opt/app-root/src/output.ipynb --kernel python3 && "+
					"echo '✅ Notebook executed successfully' || "+
					"(echo '❌ Notebook execution failed' && "+
					"cp /opt/app-root/src/output.ipynb /opt/app-root/src/failed_output.ipynb && "+
					"echo '📄 Copied failed notebook to failed_output.ipynb')) && "+
					"jupyter nbconvert --to notebook --stdout /opt/app-root/src/output.ipynb || echo '⚠️ nbconvert failed' && "+
					"sleep 100; exit 0",
				utils.GetOCToken("test/e2e_rhoai"),
				utils.GetOCServer("test/e2e_rhoai"),
				"feast-test.ipynb",
			),
		}

		// Defining notebook parameters
		nbParams := utils.NotebookTemplateParams{
			Namespace:             namespace,
			IngressDomain:         utils.GetIngressDomain(testDir),
			OpenDataHubNamespace:  env("APPLICATIONS_NAMESPACE"),
			NotebookImage:         env("NOTEBOOK_IMAGE"),
			NotebookConfigMapName: configMapName,
			NotebookPVC:           notebookPVC,
			Username:              username,
			OC_TOKEN:              utils.GetOCToken(testDir),
			OC_SERVER:             utils.GetOCServer(testDir),
			NotebookFile:          notebookName,
			Command:               "[\"" + strings.Join(command, "\",\"") + "\"]",
			PipIndexUrl:           env("PIP_INDEX_URL"),
			PipTrustedHost:        env("PIP_TRUSTED_HOST"),
			FeastVerison:          env("FEAST_VERSION"),
			OpenAIAPIKey:          env("OPENAI_API_KEY"),
		}

		By("Creating Jupyter Notebook")
		Expect(utils.CreateNotebook(nbParams)).To(Succeed(), "Failed to create notebook")

		By("Monitoring notebook logs")
		Expect(utils.MonitorNotebookPod(namespace, "jupyter-nb-", notebookName)).To(Succeed(), "Notebook execution failed")
	}

	Context("Feast Jupyter Notebook Test", func() {
		It("Should create and run a "+feastMilvusTest+" successfully", runNotebookTest)
	})
})
