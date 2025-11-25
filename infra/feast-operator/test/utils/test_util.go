package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"

	"github.com/feast-dev/feast/infra/feast-operator/api/feastversion"
	v1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
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
	var resource v1.FeatureStore
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
		var registryConfig v1.Registry
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
func GetTestDeploySimpleCRFunc(testDir string, crYaml string, featureStoreName string, feastResourceName string, feastK8sResourceNames []string, namespace string) func() {
	return func() {
		By("deploying the Simple Feast Custom Resource to Kubernetes")

		cmd := exec.Command("kubectl", "apply", "-f", crYaml, "-n", namespace)
		_, cmdOutputerr := Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())

		validateTheFeatureStoreCustomResource(namespace, featureStoreName, feastResourceName, feastK8sResourceNames, Timeout)

		By("deleting the feast deployment")
		cmd = exec.Command("kubectl", "delete", "-f", crYaml, "-n", namespace)
		_, cmdOutputerr = Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())
	}
}

// GetTestWithRemoteRegistryFunc - returns a CR deployment with a remote registry function
func GetTestWithRemoteRegistryFunc(testDir string, crYaml string, remoteRegistryCRYaml string, featureStoreName string, feastResourceName string, feastK8sResourceNames []string, namespace string) func() {
	return func() {
		By("deploying the Simple Feast Custom Resource to Kubernetes")
		cmd := exec.Command("kubectl", "apply", "-f", crYaml, "-n", namespace)
		_, cmdOutputErr := Run(cmd, testDir)
		ExpectWithOffset(1, cmdOutputErr).NotTo(HaveOccurred())

		validateTheFeatureStoreCustomResource(namespace, featureStoreName, feastResourceName, feastK8sResourceNames, Timeout)

		var remoteRegistryNs = "test-ns-remote-registry"
		err := CreateNamespace(remoteRegistryNs, "/test/e2e")
		Expect(err).ToNot(HaveOccurred(), fmt.Sprintf("failed to create namespace %s", remoteRegistryNs))

		DeferCleanup(func() {
			By(fmt.Sprintf("Deleting remote registry namespace: %s", remoteRegistryNs))
			err := DeleteNamespace(remoteRegistryNs, testDir)
			Expect(err).ToNot(HaveOccurred(), fmt.Sprintf("failed to delete namespace %s", remoteRegistryNs))
		})

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
func DeployOperatorFromCode(testDir string, skipBuilds bool) {
	_, isRunOnOpenShiftCI := os.LookupEnv("RUN_ON_OPENSHIFT_CI")
	if !isRunOnOpenShiftCI {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", FeastControllerNamespace)
		_, _ = Run(cmd, testDir)

		var err error
		// projectimage stores the name of the image used in the example
		var projectimage = "localhost/feast-operator:v0.0.1"

		// this image will be built in above make target.
		var feastImage = "feastdev/feature-server:dev"
		var feastLocalImage = "localhost/feastdev/feature-server:dev"

		if !skipBuilds {
			By("building the manager(Operator) image")
			cmd = exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectimage))
			_, err = Run(cmd, testDir)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

			By("loading the the manager(Operator) image on Kind")
			err = LoadImageToKindClusterWithName(projectimage, testDir)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())

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
		}

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectimage), fmt.Sprintf("FS_IMG=%s", feastLocalImage))
		_, err = Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
	}

	By("Validating that the controller-manager deployment is in available state")
	err := CheckIfDeploymentExistsAndAvailable(FeastControllerNamespace, ControllerDeploymentName, Timeout)
	Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
		"Deployment %s is not available but expected to be available. \nError: %v\n",
		ControllerDeploymentName, err,
	))
	fmt.Printf("Feast Control Manager Deployment %s is available\n", ControllerDeploymentName)
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
	var err error

	// Clean up existing CRD first to avoid version conflicts
	// This is necessary because the existing CRD might have versions in storedVersions
	// that the previous version's CRD doesn't support
	By("Cleaning up existing CRD to avoid version conflicts")
	cmd := exec.Command("kubectl", "delete", "crd", "featurestores.feast.dev", "--ignore-not-found=true")
	_, err = Run(cmd, "/test/upgrade")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	// Wait a moment for CRD deletion to complete
	time.Sleep(2 * time.Second)

	cmd = exec.Command("kubectl", "apply", "-f", fmt.Sprintf("https://raw.githubusercontent.com/feast-dev/feast/refs/tags/v%s/infra/feast-operator/dist/install.yaml", feastversion.FeastVersion))
	_, err = Run(cmd, "/test/upgrade")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	err = CheckIfDeploymentExistsAndAvailable(FeastControllerNamespace, ControllerDeploymentName, Timeout)
	Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
		"Deployment %s is not available but expected to be available. \nError: %v\n",
		ControllerDeploymentName, err,
	))
	fmt.Printf("Feast Control Manager Deployment %s is available\n", ControllerDeploymentName)
}

// GetSimplePreviousVerCR - Get The previous version simple CR for tests
func GetSimplePreviousVerCR() string {
	return fmt.Sprintf("https://raw.githubusercontent.com/feast-dev/feast/refs/tags/v%s/infra/feast-operator/test/testdata/feast_integration_test_crs/v1alpha1_default_featurestore.yaml", feastversion.FeastVersion)
}

// GetRemoteRegistryPreviousVerCR - Get The previous version remote registry CR for tests
// Note: Previous version (v0.57.0) uses v1alpha1 API, so we use v1alpha1_* file names
func GetRemoteRegistryPreviousVerCR() string {
	return fmt.Sprintf("https://raw.githubusercontent.com/feast-dev/feast/refs/tags/v%s/infra/feast-operator/test/testdata/feast_integration_test_crs/v1alpha1_remote_registry_featurestore.yaml", feastversion.FeastVersion)
}

// CreateNamespace - create the namespace for tests
func CreateNamespace(namespace string, testDir string) error {
	cmd := exec.Command("kubectl", "create", "ns", namespace)
	output, err := Run(cmd, testDir)
	if err != nil {
		return fmt.Errorf("failed to create namespace %s: %v\nOutput: %s", namespace, err, output)
	}
	return nil
}

// DeleteNamespace - Delete the namespace for tests
func DeleteNamespace(namespace string, testDir string) error {
	cmd := exec.Command("kubectl", "delete", "ns", namespace, "--timeout=180s")
	output, err := Run(cmd, testDir)
	if err != nil {
		return fmt.Errorf("failed to delete namespace %s: %v\nOutput: %s", namespace, err, output)
	}
	return nil
}

// Test real-time credit scoring demo by applying feature store configs and verifying Feast definitions, materializing data.
func RunTestApplyAndMaterializeFunc(testDir string, namespace string, feastCRName string, feastDeploymentName string) func() {
	return func() {
		ApplyFeastInfraManifestsAndVerify(namespace, testDir)
		ApplyFeastYamlAndVerify(namespace, testDir, feastDeploymentName, feastCRName, "test/testdata/feast_integration_test_crs/feast.yaml")
		VerifyApplyFeatureStoreDefinitions(namespace, feastCRName, feastDeploymentName)
		VerifyFeastMethods(namespace, feastDeploymentName, testDir)
	}
}

// applies the manifests for Redis and Postgres and checks whether the deployments become available
func ApplyFeastInfraManifestsAndVerify(namespace string, testDir string) {
	By("Applying postgres.yaml and redis.yaml manifests")
	cmd := exec.Command("kubectl", "apply", "-n", namespace, "-f", "test/testdata/feast_integration_test_crs/postgres.yaml", "-f", "test/testdata/feast_integration_test_crs/redis.yaml")
	_, cmdOutputerr := Run(cmd, testDir)
	ExpectWithOffset(1, cmdOutputerr).NotTo(HaveOccurred())
	checkDeployment(namespace, "postgres")
	checkDeployment(namespace, "redis")
}

// validates the `feast apply` and `feast materialize-incremental commands were configured in the FeatureStore CR's CronJob config.
func VerifyApplyFeatureStoreDefinitions(namespace string, feastCRName string, feastDeploymentName string) {
	By("Verify CronJob commands in FeatureStore CR")
	cmd := exec.Command("kubectl", "get", "-n", namespace, "feast/"+feastCRName, "-o", "jsonpath={.status.applied.cronJob.containerConfigs.commands}")
	output, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to fetch CronJob commands:\n%s", output))
	commands := string(output)
	fmt.Println("CronJob commands:", commands)
	Expect(commands).To(ContainSubstring(`feast apply`))
	Expect(commands).To(ContainSubstring(`feast materialize-incremental $(date -u +'%Y-%m-%dT%H:%M:%S')`))

	CreateAndVerifyJobFromCron(namespace, feastDeploymentName, "feast-test-apply", "", []string{
		"No project found in the repository",
		"Applying changes for project credit_scoring_local",
		"Deploying infrastructure for credit_history",
		"Deploying infrastructure for zipcode_features",
		"Materializing 2 feature views to",
		"into the redis online store",
		"credit_history from",
		"zipcode_features from",
	})

}

// checks for the presence of expected entities, features, feature views, data sources, etc.
func VerifyFeastMethods(namespace string, feastDeploymentName string, testDir string) {
	type feastCheck struct {
		command   []string
		expected  []string
		logPrefix string
	}
	checks := []feastCheck{
		{
			command:   []string{"feast", "projects", "list"},
			expected:  []string{"credit_scoring_local"},
			logPrefix: "Projects List",
		},
		{
			command:   []string{"feast", "feature-views", "list"},
			expected:  []string{"credit_history", "zipcode_features", "total_debt_calc"},
			logPrefix: "Feature Views List",
		},
		{
			command:   []string{"feast", "entities", "list"},
			expected:  []string{"zipcode", "dob_ssn"},
			logPrefix: "Entities List",
		},
		{
			command:   []string{"feast", "data-sources", "list"},
			expected:  []string{"Zipcode source", "Credit history", "application_data"},
			logPrefix: "Data Sources List",
		},
		{
			command: []string{"feast", "features", "list"},
			expected: []string{
				"credit_card_due", "mortgage_due", "student_loan_due", "vehicle_loan_due",
				"hard_pulls", "missed_payments_2y", "missed_payments_1y", "missed_payments_6m",
				"bankruptcies", "city", "state", "location_type", "tax_returns_filed",
				"population", "total_wages", "total_debt_due",
			},
			logPrefix: "Features List",
		},
	}

	for _, check := range checks {
		cmd := exec.Command("kubectl", "exec", "deploy/"+feastDeploymentName, "-n", namespace, "-c", "online", "--")
		cmd.Args = append(cmd.Args, check.command...)
		output, err := Run(cmd, testDir)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		fmt.Printf("%s:\n%s\n", check.logPrefix, string(output))
		VerifyOutputContains(output, check.expected)
	}
}

// asserts that all expected substrings are present in the given output.
func VerifyOutputContains(output []byte, expectedSubstrings []string) {
	outputStr := string(output)
	for _, expected := range expectedSubstrings {
		Expect(outputStr).To(ContainSubstring(expected), fmt.Sprintf("Expected output to contain: %s", expected))
	}
}

// Create a Job and verifies its logs contain expected substrings
func CreateAndVerifyJobFromCron(namespace, cronName, jobName, testDir string, expectedLogSubstrings []string) {
	By(fmt.Sprintf("Creating Job %s from CronJob %s", jobName, cronName))
	cmd := exec.Command("kubectl", "create", "job", "--from=cronjob/"+cronName, jobName, "-n", namespace)
	_, err := Run(cmd, testDir)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	By("Waiting for Job completion")
	cmd = exec.Command("kubectl", "wait", "--for=condition=complete", "--timeout=5m", "job/"+jobName, "-n", namespace)
	_, err = Run(cmd, testDir)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	By("Checking logs of completed job")
	cmd = exec.Command("kubectl", "logs", "job/"+jobName, "-n", namespace, "--all-containers=true")
	output, err := Run(cmd, testDir)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	outputStr := string(output)
	ansi := regexp.MustCompile(`\x1b\[[0-9;]*m`)
	outputStr = ansi.ReplaceAllString(outputStr, "")
	for _, expected := range expectedLogSubstrings {
		Expect(outputStr).To(ContainSubstring(expected))
	}
	fmt.Printf("created Job %s and Verified expected Logs ", jobName)
}

// verifies the specified deployment exists and is in the "Available" state.
func checkDeployment(namespace, name string) {
	By(fmt.Sprintf("Waiting for %s deployment to become available", name))
	err := CheckIfDeploymentExistsAndAvailable(namespace, name, 2*Timeout)
	Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
		"Deployment %s is not available but expected to be.\nError: %v", name, err,
	))
	fmt.Printf("Deployment %s is available\n", name)
}

// validate that the status of the FeatureStore CR is "Ready".
func ValidateFeatureStoreCRStatus(namespace, crName string) {
	Eventually(func() string {
		cmd := exec.Command("kubectl", "get", "feast", crName, "-n", namespace, "-o", "jsonpath={.status.phase}")
		output, err := cmd.Output()
		if err != nil {
			return ""
		}
		return string(output)
	}, "2m", "5s").Should(Equal("Ready"), "Feature Store CR did not reach 'Ready' state in time")

	fmt.Printf("✅ Feature Store CR %s/%s is in Ready state\n", namespace, crName)
}

// validate the feature store yaml
func validateFeatureStoreYaml(namespace, deployment string) {
	cmd := exec.Command("kubectl", "exec", "deploy/"+deployment, "-n", namespace, "-c", "online", "--", "cat", "feature_store.yaml")
	output, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "Failed to read feature_store.yaml")

	content := string(output)
	Expect(content).To(ContainSubstring("offline_store:\n    type: duckdb"))
	Expect(content).To(ContainSubstring("online_store:\n    type: redis"))
	Expect(content).To(ContainSubstring("registry_type: sql"))
}

// apply and verifies the Feast deployment becomes available, the CR status is "Ready
func ApplyFeastYamlAndVerify(namespace string, testDir string, feastDeploymentName string, feastCRName string, feastYAMLFilePath string) {
	By("Applying Feast yaml for secrets and Feature store CR")
	cmd := exec.Command("kubectl", "apply", "-n", namespace,
		"-f", feastYAMLFilePath)
	_, err := Run(cmd, testDir)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	checkDeployment(namespace, feastDeploymentName)

	By("Verify Feature Store CR is in Ready state")
	ValidateFeatureStoreCRStatus(namespace, feastCRName)

	By("Verifying that the Postgres DB contains the expected Feast tables")
	cmd = exec.Command("kubectl", "exec", "deploy/postgres", "-n", namespace, "--", "psql", "-h", "localhost", "-U", "feast", "feast", "-c", `\dt`)
	output, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to get tables from Postgres. Output:\n%s", output))
	outputStr := string(output)
	fmt.Println("Postgres Tables:\n", outputStr)
	// List of expected tables
	expectedTables := []string{
		"data_sources", "entities", "feast_metadata", "feature_services", "feature_views",
		"managed_infra", "on_demand_feature_views", "permissions", "projects",
		"saved_datasets", "stream_feature_views", "validation_references",
	}
	for _, table := range expectedTables {
		Expect(outputStr).To(ContainSubstring(table), fmt.Sprintf("Expected table %q not found in output:\n%s", table, outputStr))
	}

	By("Verifying that the Feast repo was successfully cloned by the init container")
	cmd = exec.Command("kubectl", "logs", "-f", "-n", namespace, "deploy/"+feastDeploymentName, "-c", "feast-init")
	output, err = cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to get logs from init container. Output:\n%s", output))
	outputStr = string(output)
	fmt.Println("Init Container Logs:\n", outputStr)
	// Assert that the logs contain success indicators
	Expect(outputStr).To(ContainSubstring("Feast repo creation complete"), "Expected Feast repo creation message not found")

	By("Verifying client feature_store.yaml for expected store types")
	validateFeatureStoreYaml(namespace, feastDeploymentName)
}

// ReplaceNamespaceInYaml reads a YAML file, replaces all existingNamespace with the actual namespace
func ReplaceNamespaceInYamlFilesInPlace(filePaths []string, existingNamespace string, actualNamespace string) error {
	for _, filePath := range filePaths {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read YAML file %s: %w", filePath, err)
		}
		updated := strings.ReplaceAll(string(data), existingNamespace, actualNamespace)

		err = os.WriteFile(filePath, []byte(updated), 0644)
		if err != nil {
			return fmt.Errorf("failed to write updated YAML file %s: %w", filePath, err)
		}
	}
	return nil
}

func ApplyFeastPermissions(fileName string, registryFilePath string, namespace string, podNamePrefix string) {
	By("Applying Feast permissions to the Feast registry pod")

	// 1. Get the pod by prefix
	By(fmt.Sprintf("Finding pod with prefix %q in namespace %q", podNamePrefix, namespace))
	pod, err := getPodByPrefix(namespace, podNamePrefix)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, pod).NotTo(BeNil())

	podName := pod.Name
	fmt.Printf("Found pod: %s\n", podName)

	cmd := exec.Command(
		"oc", "cp",
		fileName, // local source file
		fmt.Sprintf("%s/%s:%s", namespace, podName, registryFilePath), // remote destination
		"-c", "registry",
	)

	_, err = Run(cmd, "/test/e2e_rhoai")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	fmt.Printf("Successfully copied file to pod: %s\n", podName)

	// Run `feast apply` inside the pod to apply updated permissions
	By("Running feast apply inside the Feast registry pod")
	cmd = exec.Command(
		"oc", "exec", podName,
		"-n", namespace,
		"-c", "registry",
		"--",
		"bash", "-c",
		"cd /feast-data/credit_scoring_local/feature_repo && feast apply",
	)
	_, err = Run(cmd, "/test/e2e_rhoai")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	fmt.Println("Feast permissions apply executed successfully")

	By("Validating that Feast permission has been applied")

	cmd = exec.Command(
		"oc", "exec", podName,
		"-n", namespace,
		"-c", "registry",
		"--",
		"feast", "permissions", "list",
	)

	output, err := Run(cmd, "/test/e2e_rhoai")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	// Change "feast-auth" if your permission name is different
	ExpectWithOffset(1, output).To(ContainSubstring("feast-auth"), "Expected permission 'feast-auth' to exist")

	fmt.Println("Verified: Feast permission 'feast-auth' exists")
}
