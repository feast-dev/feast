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

package e2e

import (
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/feast-dev/feast/infra/feast-operator/test/utils"
)

const feastControllerNamespace = "feast-operator-system"

var _ = Describe("controller", Ordered, func() {
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", feastControllerNamespace)
		_, _ = utils.Run(cmd)
	})

	AfterAll(func() {
		//Add any post clean up code here.
	})

	Context("Operator", func() {
		It("Should be able to deploy and run a default feature store CR successfully", func() {
			//var controllerPodName string
			var err error

			// projectimage stores the name of the image used in the example
			var projectimage = "localhost/feast-operator:v0.0.1"

			By("building the manager(Operator) image")
			cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectimage))
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("loading the the manager(Operator) image on Kind")
			err = utils.LoadImageToKindClusterWithName(projectimage)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("building the feast image")
			cmd = exec.Command("make", "feast-ci-dev-docker-img")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
			// this image will be built in above make target.
			var feastImage = "feastdev/feature-server:dev"
			var feastLocalImage = "localhost/feastdev/feature-server:dev"

			By("Tag the local feast image for the integration tests")
			cmd = exec.Command("docker", "image", "tag", feastImage, feastLocalImage)
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("loading the the feast image on Kind cluster")
			err = utils.LoadImageToKindClusterWithName(feastLocalImage)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("installing CRDs")
			cmd = exec.Command("make", "install")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("deploying the controller-manager")
			cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectimage))
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			timeout := 2 * time.Minute

			controllerDeploymentName := "feast-operator-controller-manager"
			By("Validating that the controller-manager deployment is in available state")
			err = checkIfDeploymentExistsAndAvailable(feastControllerNamespace, controllerDeploymentName, timeout)
			Expect(err).To(BeNil(), fmt.Sprintf(
				"Deployment %s is not available but expected to be available. \nError: %v\n",
				controllerDeploymentName, err,
			))
			fmt.Printf("Feast Control Manager Deployment %s is available\n", controllerDeploymentName)

			By("deploying the Simple Feast Custom Resource to Kubernetes")
			cmd = exec.Command("kubectl", "apply", "-f",
				"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml")
			_, cmdOutputerr := utils.Run(cmd)
			ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())

			namespace := "default"

			deploymentNames := [3]string{"feast-simple-feast-setup-registry", "feast-simple-feast-setup-online",
				"feast-simple-feast-setup-offline"}
			for _, deploymentName := range deploymentNames {
				By(fmt.Sprintf("validate the feast deployment: %s is up and in availability state.", deploymentName))
				err = checkIfDeploymentExistsAndAvailable(namespace, deploymentName, timeout)
				Expect(err).To(BeNil(), fmt.Sprintf(
					"Deployment %s is not available but expected to be available. \nError: %v\n",
					deploymentName, err,
				))
				fmt.Printf("Feast Deployment %s is available\n", deploymentName)
			}

			By("Check if the feast client - kubernetes config map exists.")
			configMapName := "feast-simple-feast-setup-client"
			err = checkIfConfigMapExists(namespace, configMapName)
			Expect(err).To(BeNil(), fmt.Sprintf(
				"config map %s is not available but expected to be available. \nError: %v\n",
				configMapName, err,
			))
			fmt.Printf("Feast Deployment %s is available\n", configMapName)

			serviceAccountNames := [3]string{"feast-simple-feast-setup-registry", "feast-simple-feast-setup-online",
				"feast-simple-feast-setup-offline"}
			for _, serviceAccountName := range serviceAccountNames {
				By(fmt.Sprintf("validate the feast service account: %s is available.", serviceAccountName))
				err = checkIfServiceAccountExists(namespace, serviceAccountName)
				Expect(err).To(BeNil(), fmt.Sprintf(
					"Service account %s does not exist in namespace %s. Error: %v",
					serviceAccountName, namespace, err,
				))
				fmt.Printf("Service account %s exists in namespace %s\n", serviceAccountName, namespace)
			}

			serviceNames := [3]string{"feast-simple-feast-setup-registry", "feast-simple-feast-setup-online",
				"feast-simple-feast-setup-offline"}
			for _, serviceName := range serviceNames {
				By(fmt.Sprintf("validate the kubernetes service name: %s is available.", serviceName))
				err = checkIfKubernetesServiceExists(namespace, serviceName)
				Expect(err).To(BeNil(), fmt.Sprintf(
					"kubernetes service %s is not available but expected to be available. \nError: %v\n",
					serviceName, err,
				))
				fmt.Printf("kubernetes service %s is available\n", serviceName)
			}

			By(fmt.Sprintf("Checking FeatureStore customer resource: %s is in Ready Status.", "simple-feast-setup"))
			err = checkIfFeatureStoreCustomResourceConditionsInReady("simple-feast-setup", namespace)
			Expect(err).To(BeNil(), fmt.Sprintf(
				"FeatureStore custom resource %s all conditions are not in ready state. \nError: %v\n",
				"simple-feast-setup", err,
			))
			fmt.Printf("FeatureStore customer resource %s conditions are in Ready State\n", "simple-feast-setup")

			By("deleting the feast deployment")
			cmd = exec.Command("kubectl", "delete", "-f",
				"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml")
			_, cmdOutputerr = utils.Run(cmd)
			ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())

			By("Uninstalling the feast CRD")
			cmd = exec.Command("kubectl", "delete", "deployment", controllerDeploymentName, "-n", feastControllerNamespace)
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

		})
	})
})
