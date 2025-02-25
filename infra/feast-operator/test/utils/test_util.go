package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	"github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
)

const (
	FeastControllerNamespace = "feast-operator-system"
	Timeout                  = 3 * time.Minute
	ControllerDeploymentName = "feast-operator-controller-manager"
	FeastPrefix              = "feast-"
	FeatureStoreName         = "simple-feast-setup"
	FeastResourceName        = FeastPrefix + FeatureStoreName
)

// dynamically checks if all conditions of custom resource featurestore are in "Ready" state.
func checkIfFeatureStoreCustomResourceConditionsInReady(featureStoreName, namespace string) error {
	// Wait 10 seconds to lets the feature store status update
	time.Sleep(1 * time.Minute)

	cmd := exec.Command("kubectl", "get", "featurestore", featureStoreName, "-n", namespace, "-o", "json")

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to get resource %s in namespace %s. Error: %v. Stderr: %s",
			featureStoreName, namespace, err, stderr.String())
	}

	// Parse the JSON into FeatureStore
	var resource v1alpha1.FeatureStore
	if err := json.Unmarshal(out.Bytes(), &resource); err != nil {
		return fmt.Errorf("failed to parse the resource JSON. Error: %v", err)
	}

	// Validate all conditions
	for _, condition := range resource.Status.Conditions {
		if condition.Status != "True" {
			return fmt.Errorf(" FeatureStore=%s condition '%s' is not in 'Ready' state. Status: %s",
				featureStoreName, condition.Type, condition.Status)
		}
	}

	return nil
}

// CheckIfDeploymentExistsAndAvailable - validates if a deployment exists and also in the availability state as True.
func CheckIfDeploymentExistsAndAvailable(namespace string, deploymentName string, timeout time.Duration) error {
	var output, errOutput bytes.Buffer

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	timeoutChan := time.After(timeout)

	for {
		select {
		case <-timeoutChan:
			return fmt.Errorf("timed out waiting for deployment %s to become available", deploymentName)
		case <-ticker.C:
			// Run kubectl command
			cmd := exec.Command("kubectl", "get", "deployment", deploymentName, "-n", namespace, "-o", "json")
			cmd.Stdout = &output
			cmd.Stderr = &errOutput

			if err := cmd.Run(); err != nil {
				// Log error and retry
				fmt.Printf("Deployment not yet found, we may try again to find the updated status: %s\n", errOutput.String())
				continue
			}

			// Parse the JSON output into Deployment
			var result appsv1.Deployment
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				return fmt.Errorf("failed to parse deployment JSON: %v", err)
			}

			// Check for Available condition
			for _, condition := range result.Status.Conditions {
				if condition.Type == "Available" && condition.Status == "True" {
					return nil // Deployment is available
				}
			}

			// Reset buffers for the next loop iteration
			output.Reset()
			errOutput.Reset()
		}
	}
}

// validates if a service account exists using the kubectl CLI.
func checkIfServiceAccountExists(namespace, saName string) error {
	cmd := exec.Command("kubectl", "get", "sa", saName, "-n", namespace)

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to find service account %s in namespace %s. Error: %v. Stderr: %s",
			saName, namespace, err, stderr.String())
	}

	// Check the output to confirm presence
	if !strings.Contains(out.String(), saName) {
		return fmt.Errorf("service account %s not found in namespace %s", saName, namespace)
	}

	return nil
}

// validates if a config map exists using the kubectl CLI.
func checkIfConfigMapExists(namespace, configMapName string) error {
	cmd := exec.Command("kubectl", "get", "cm", configMapName, "-n", namespace)

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to find config map %s in namespace %s. Error: %v. Stderr: %s",
			configMapName, namespace, err, stderr.String())
	}

	// Check the output to confirm presence
	if !strings.Contains(out.String(), configMapName) {
		return fmt.Errorf("config map %s not found in namespace %s", configMapName, namespace)
	}

	return nil
}

// validates if a kubernetes service exists using the kubectl CLI.
func checkIfKubernetesServiceExists(namespace, serviceName string) error {
	cmd := exec.Command("kubectl", "get", "service", serviceName, "-n", namespace)

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to find kubernetes service %s in namespace %s. Error: %v. Stderr: %s",
			serviceName, namespace, err, stderr.String())
	}

	// Check the output to confirm presence
	if !strings.Contains(out.String(), serviceName) {
		return fmt.Errorf("kubernetes service %s not found in namespace %s", serviceName, namespace)
	}

	return nil
}

func isFeatureStoreHavingRemoteRegistry(namespace, featureStoreName string) (bool, error) {
	timeout := time.Second * 30
	interval := time.Second * 2 // Poll every 2 seconds
	startTime := time.Now()

	for time.Since(startTime) < timeout {
		cmd := exec.Command("kubectl", "get", "featurestore", featureStoreName, "-n", namespace,
			"-o=jsonpath='{.status.applied.services.registry}'")

		output, err := cmd.Output()
		if err != nil {
			// Retry only on transient errors
			if _, ok := err.(*exec.ExitError); ok {
				time.Sleep(interval)
				continue
			}
			return false, err // Return immediately on non-transient errors
		}

		// Convert output to string and trim any extra spaces
		result := strings.TrimSpace(string(output))

		// Remove single quotes if present
		if strings.HasPrefix(result, "'") && strings.HasSuffix(result, "'") {
			result = strings.Trim(result, "'")
		}

		if result == "" {
			time.Sleep(interval) // Retry if result is empty
			continue
		}

		// Parse the JSON into a map
		var registryConfig v1alpha1.Registry
		if err := json.Unmarshal([]byte(result), &registryConfig); err != nil {
			return false, err // Return false on JSON parsing failure
		}

		if registryConfig.Remote == nil {
			return false, nil
		}

		hasHostname := registryConfig.Remote.Hostname != nil
		hasValidFeastRef := registryConfig.Remote.FeastRef != nil &&
			registryConfig.Remote.FeastRef.Name != ""

		return hasHostname || hasValidFeastRef, nil
	}

	return false, errors.New("timeout waiting for featurestore registry status to be ready")
}

func validateTheFeatureStoreCustomResource(namespace string, featureStoreName string, feastResourceName string, feastK8sResourceNames []string, timeout time.Duration) {
	hasRemoteRegistry, err := isFeatureStoreHavingRemoteRegistry(namespace, featureStoreName)
	Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
		"Error occurred while checking FeatureStore %s is having remote registry or not. \nError: %v\n",
		featureStoreName, err))

	k8sResourceNames := []string{feastResourceName}

	if !hasRemoteRegistry {
		feastK8sResourceNames = append(feastK8sResourceNames, feastResourceName+"-registry")
	}

	for _, deploymentName := range k8sResourceNames {
		By(fmt.Sprintf("validate the feast deployment: %s is up and in availability state.", deploymentName))
		err = CheckIfDeploymentExistsAndAvailable(namespace, deploymentName, timeout)
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

// GetTestDeploySimpleCRFunc - returns a simple CR deployment function
func GetTestDeploySimpleCRFunc(testDir string, crYaml string, featureStoreName string, feastResourceName string, feastK8sResourceNames []string) func() {
	return func() {
		By("deploying the Simple Feast Custom Resource to Kubernetes")
		namespace := "default"

		cmd := exec.Command("kubectl", "apply", "-f", crYaml, "-n", namespace)
		_, cmdOutputerr := Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())

		validateTheFeatureStoreCustomResource(namespace, featureStoreName, feastResourceName, feastK8sResourceNames, Timeout)

		By("deleting the feast deployment")
		cmd = exec.Command("kubectl", "delete", "-f", crYaml)
		_, cmdOutputerr = Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())
	}
}

// GetTestWithRemoteRegistryFunc - returns a CR deployment with a remote registry function
func GetTestWithRemoteRegistryFunc(testDir string, crYaml string, remoteRegistryCRYaml string, featureStoreName string, feastResourceName string, feastK8sResourceNames []string) func() {
	return func() {
		By("deploying the Simple Feast Custom Resource to Kubernetes")
		namespace := "default"
		cmd := exec.Command("kubectl", "apply", "-f", crYaml, "-n", namespace)
		_, cmdOutputErr := Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())

		validateTheFeatureStoreCustomResource(namespace, featureStoreName, feastResourceName, feastK8sResourceNames, Timeout)

		var remoteRegistryNs = "remote-registry"
		By(fmt.Sprintf("Creating the remote registry namespace=%s", remoteRegistryNs))
		cmd = exec.Command("kubectl", "create", "ns", remoteRegistryNs)
		_, _ = Run(cmd, testDir)

		By("deploying the Simple Feast remote registry Custom Resource on Kubernetes")
		cmd = exec.Command("kubectl", "apply", "-f", remoteRegistryCRYaml, "-n", remoteRegistryNs)
		_, cmdOutputErr = Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())

		remoteFeatureStoreName := "simple-feast-remote-setup"
		remoteFeastResourceName := FeastPrefix + remoteFeatureStoreName
		fixRemoteFeastK8sResourceNames(feastK8sResourceNames, remoteFeastResourceName)
		validateTheFeatureStoreCustomResource(remoteRegistryNs, remoteFeatureStoreName, remoteFeastResourceName, feastK8sResourceNames, Timeout)

		By("deleting the feast remote registry deployment")
		cmd = exec.Command("kubectl", "delete", "-f", remoteRegistryCRYaml, "-n", remoteRegistryNs)
		_, cmdOutputErr = Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())

		By("deleting the feast deployment")
		cmd = exec.Command("kubectl", "delete", "-f", crYaml, "-n", namespace)
		_, cmdOutputErr = Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())
	}
}

func fixRemoteFeastK8sResourceNames(feastK8sResourceNames []string, remoteFeastResourceName string) {
	for i, feastK8sResourceName := range feastK8sResourceNames {
		if index := strings.LastIndex(feastK8sResourceName, "-"); index != -1 {
			feastK8sResourceNames[i] = remoteFeastResourceName + feastK8sResourceName[index:]
		}
	}
}

// DeployOperatorFromCode - Creates the images for the operator and deploys it
func DeployOperatorFromCode(testDir string) {
	_, isRunOnOpenShiftCI := os.LookupEnv("RUN_ON_OPENSHIFT_CI")
	if !isRunOnOpenShiftCI {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", FeastControllerNamespace)
		_, _ = Run(cmd, testDir)

		var err error
		// projectimage stores the name of the image used in the example
		var projectimage = "localhost/feast-operator:v0.0.1"

		By("building the manager(Operator) image")
		cmd = exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectimage))
		_, err = Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("loading the the manager(Operator) image on Kind")
		err = LoadImageToKindClusterWithName(projectimage, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		// this image will be built in above make target.
		var feastImage = "feastdev/feature-server:dev"
		var feastLocalImage = "localhost/feastdev/feature-server:dev"

		By("building the feast image")
		cmd = exec.Command("make", "feast-ci-dev-docker-img")
		_, err = Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("Tag the local feast image for the integration tests")
		cmd = exec.Command("docker", "image", "tag", feastImage, feastLocalImage)
		_, err = Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("loading the the feast image on Kind cluster")
		err = LoadImageToKindClusterWithName(feastLocalImage, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectimage), fmt.Sprintf("FS_IMG=%s", feastLocalImage))
		_, err = Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("Validating that the controller-manager deployment is in available state")
		err = CheckIfDeploymentExistsAndAvailable(FeastControllerNamespace, ControllerDeploymentName, Timeout)
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
			"Deployment %s is not available but expected to be available. \nError: %v\n",
			ControllerDeploymentName, err,
		))
		fmt.Printf("Feast Control Manager Deployment %s is available\n", ControllerDeploymentName)
	}
}

// DeleteOperatorDeployment - Deletes the operator deployment
func DeleteOperatorDeployment(testDir string) {
	_, isRunOnOpenShiftCI := os.LookupEnv("RUN_ON_OPENSHIFT_CI")
	if !isRunOnOpenShiftCI {
		By("Uninstalling the feast CRD")
		cmd := exec.Command("kubectl", "delete", "deployment", ControllerDeploymentName, "-n", FeastControllerNamespace)
		_, err := Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
	}
}

// DeployPreviousVersionOperator - Deploys the previous version of the operator
func DeployPreviousVersionOperator() {
	_, isRunOnOpenShiftCI := os.LookupEnv("RUN_ON_OPENSHIFT_CI")
	if !isRunOnOpenShiftCI {
		var err error

		cmd := exec.Command("kubectl", "apply", "-f", fmt.Sprintf("https://raw.githubusercontent.com/feast-dev/feast/refs/tags/v%s/infra/feast-operator/dist/install.yaml", feastversion.FeastVersion))
		_, err = Run(cmd, "/test/upgrade")
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		err = CheckIfDeploymentExistsAndAvailable(FeastControllerNamespace, ControllerDeploymentName, Timeout)
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
			"Deployment %s is not available but expected to be available. \nError: %v\n",
			ControllerDeploymentName, err,
		))
		fmt.Printf("Feast Control Manager Deployment %s is available\n", ControllerDeploymentName)
	}
}

// GetSimplePreviousVerCR - Get The previous version simple CR for tests
func GetSimplePreviousVerCR() string {
	return fmt.Sprintf("https://raw.githubusercontent.com/feast-dev/feast/refs/tags/v%s/infra/feast-operator/test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml", feastversion.FeastVersion)
}

// GetRemoteRegistryPreviousVerCR - Get The previous version remote registry CR for tests
func GetRemoteRegistryPreviousVerCR() string {
	return fmt.Sprintf("https://raw.githubusercontent.com/feast-dev/feast/refs/tags/v%s/infra/feast-operator/test/testdata/feast_integration_test_crs/v1alpha1_remote_registry_featurestore.yaml", feastversion.FeastVersion)
}
