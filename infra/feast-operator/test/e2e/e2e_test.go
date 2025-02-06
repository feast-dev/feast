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
	"os"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/feast-dev/feast/infra/feast-operator/test/utils"
)

const (
	feastControllerNamespace = "feast-operator-system"
	timeout                  = 2 * time.Minute
	controllerDeploymentName = "feast-operator-controller-manager"
	feastPrefix              = "feast-"
)

var _ = Describe("controller", Ordered, func() {
	BeforeAll(func() {
		_, isRunOnOpenShiftCI := os.LookupEnv("RUN_ON_OPENSHIFT_CI")
		if !isRunOnOpenShiftCI {
			By("creating manager namespace")
			cmd := exec.Command("kubectl", "create", "ns", feastControllerNamespace)
			_, _ = utils.Run(cmd)

			var err error
			// projectimage stores the name of the image used in the example
			var projectimage = "localhost/feast-operator:v0.0.1"

			By("building the manager(Operator) image")
			cmd = exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectimage))
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("loading the the manager(Operator) image on Kind")
			err = utils.LoadImageToKindClusterWithName(projectimage)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			// this image will be built in above make target.
			var feastImage = "feastdev/feature-server:dev"
			var feastLocalImage = "localhost/feastdev/feature-server:dev"

			By("building the feast image")
			cmd = exec.Command("make", "feast-ci-dev-docker-img")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

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
			cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectimage), fmt.Sprintf("FS_IMG=%s", feastLocalImage))
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("Validating that the controller-manager deployment is in available state")
			err = checkIfDeploymentExistsAndAvailable(feastControllerNamespace, controllerDeploymentName, timeout)
			Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
				"Deployment %s is not available but expected to be available. \nError: %v\n",
				controllerDeploymentName, err,
			))
			fmt.Printf("Feast Control Manager Deployment %s is available\n", controllerDeploymentName)
		}
	})

	AfterAll(func() {
		// Add any post clean up code here.
		_, isRunOnOpenShiftCI := os.LookupEnv("RUN_ON_OPENSHIFT_CI")
		if !isRunOnOpenShiftCI {
			By("Uninstalling the feast CRD")
			cmd := exec.Command("kubectl", "delete", "deployment", controllerDeploymentName, "-n", feastControllerNamespace)
			_, err := utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
		}
	})

	Context("Operator E2E Tests", func() {
		It("Should be able to deploy and run a default feature store CR successfully", func() {
			By("deploying the Simple Feast Custom Resource to Kubernetes")
			namespace := "default"

			cmd := exec.Command("kubectl", "apply", "-f",
				"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml", "-n", namespace)
			_, cmdOutputerr := utils.Run(cmd)
			ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())

			featureStoreName := "simple-feast-setup"
			validateTheFeatureStoreCustomResource(namespace, featureStoreName, timeout)

			By("deleting the feast deployment")
			cmd = exec.Command("kubectl", "delete", "-f",
				"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml")
			_, cmdOutputerr = utils.Run(cmd)
			ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())
		})

		It("Should be able to deploy and run a feature store with remote registry CR successfully", func() {
			By("deploying the Simple Feast Custom Resource to Kubernetes")
			namespace := "default"
			cmd := exec.Command("kubectl", "apply", "-f",
				"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml", "-n", namespace)
			_, cmdOutputErr := utils.Run(cmd)
			ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())

			featureStoreName := "simple-feast-setup"
			validateTheFeatureStoreCustomResource(namespace, featureStoreName, timeout)

			var remoteRegistryNs = "remote-registry"
			By(fmt.Sprintf("Creating the remote registry namespace=%s", remoteRegistryNs))
			cmd = exec.Command("kubectl", "create", "ns", remoteRegistryNs)
			_, _ = utils.Run(cmd)

			By("deploying the Simple Feast remote registry Custom Resource on Kubernetes")
			cmd = exec.Command("kubectl", "apply", "-f",
				"test/testdata/feast_integration_test_crs/v1alpha1_remote_registry_featurestore.yaml", "-n", remoteRegistryNs)
			_, cmdOutputErr = utils.Run(cmd)
			ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())

			remoteFeatureStoreName := "simple-feast-remote-setup"

			validateTheFeatureStoreCustomResource(remoteRegistryNs, remoteFeatureStoreName, timeout)

			By("deleting the feast remote registry deployment")
			cmd = exec.Command("kubectl", "delete", "-f",
				"test/testdata/feast_integration_test_crs/v1alpha1_remote_registry_featurestore.yaml", "-n", remoteRegistryNs)
			_, cmdOutputErr = utils.Run(cmd)
			ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())

			By("deleting the feast deployment")
			cmd = exec.Command("kubectl", "delete", "-f",
				"test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml", "-n", namespace)
			_, cmdOutputErr = utils.Run(cmd)
			ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())
		})
	})
})

func validateTheFeatureStoreCustomResource(namespace string, featureStoreName string, timeout time.Duration) {
	hasRemoteRegistry, err := isFeatureStoreHavingRemoteRegistry(namespace, featureStoreName)
	Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
		"Error occurred while checking FeatureStore %s is having remote registry or not. \nError: %v\n",
		featureStoreName, err))

	feastResourceName := feastPrefix + featureStoreName
	k8sResourceNames := []string{feastResourceName}
	feastK8sResourceNames := []string{
		feastResourceName + "-online",
		feastResourceName + "-offline",
		feastResourceName + "-ui",
	}

	if !hasRemoteRegistry {
		feastK8sResourceNames = append(feastK8sResourceNames, feastResourceName+"-registry")
	}

	for _, deploymentName := range k8sResourceNames {
		By(fmt.Sprintf("validate the feast deployment: %s is up and in availability state.", deploymentName))
		err = checkIfDeploymentExistsAndAvailable(namespace, deploymentName, timeout)
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
			"Deployment %s is not available but expected to be available. \nError: %v\n",
			deploymentName, err,
		))
		fmt.Printf("Feast Deployment %s is available\n", deploymentName)
	}

	By("Check if the feast client - kubernetes config map exists.")
	configMapName := feastResourceName + "-client"
	err = checkIfConfigMapExists(namespace, configMapName)
	Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
		"config map %s is not available but expected to be available. \nError: %v\n",
		configMapName, err,
	))
	fmt.Printf("Feast Deployment client config map %s is available\n", configMapName)

	for _, serviceAccountName := range k8sResourceNames {
		By(fmt.Sprintf("validate the feast service account: %s is available.", serviceAccountName))
		err = checkIfServiceAccountExists(namespace, serviceAccountName)
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
			"Service account %s does not exist in namespace %s. Error: %v",
			serviceAccountName, namespace, err,
		))
		fmt.Printf("Service account %s exists in namespace %s\n", serviceAccountName, namespace)
	}

	for _, serviceName := range feastK8sResourceNames {
		By(fmt.Sprintf("validate the kubernetes service name: %s is available.", serviceName))
		err = checkIfKubernetesServiceExists(namespace, serviceName)
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
			"kubernetes service %s is not available but expected to be available. \nError: %v\n",
			serviceName, err,
		))
		fmt.Printf("kubernetes service %s is available\n", serviceName)
	}

	By(fmt.Sprintf("Checking FeatureStore customer resource: %s is in Ready Status.", featureStoreName))
	err = checkIfFeatureStoreCustomResourceConditionsInReady(featureStoreName, namespace)
	Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
		"FeatureStore custom resource %s all conditions are not in ready state. \nError: %v\n",
		featureStoreName, err,
	))
	fmt.Printf("FeatureStore custom resource %s conditions are in Ready State\n", featureStoreName)
}
