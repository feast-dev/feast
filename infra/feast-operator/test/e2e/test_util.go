package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// validates if a deployment exists and also in the availability state as True.
func checkIfDeploymentExistsAndAvailable(namespace string, deploymentName string, timeout time.Duration) error {
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
				fmt.Printf("Error running command: %s\n", errOutput.String())
				continue
			}

			// Parse the JSON output into a map
			var result map[string]interface{}
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				return fmt.Errorf("failed to parse deployment JSON: %v", err)
			}

			// Navigate to status.conditions
			status, ok := result["status"].(map[string]interface{})
			if !ok {
				return fmt.Errorf("failed to get status field from deployment JSON")
			}

			conditions, ok := status["conditions"].([]interface{})
			if !ok {
				return fmt.Errorf("failed to get conditions field from deployment JSON")
			}

			// Check for Available condition
			for _, condition := range conditions {
				cond, ok := condition.(map[string]interface{})
				if !ok {
					continue
				}
				if cond["type"] == "Available" && cond["status"] == "True" {
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
		return fmt.Errorf("failed to find service account %s in namespace %s. Error: %v. Stderr: %s", saName, namespace, err, stderr.String())
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
		return fmt.Errorf("failed to find config map %s in namespace %s. Error: %v. Stderr: %s", configMapName, namespace, err, stderr.String())
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
		return fmt.Errorf("failed to find kubernetes service %s in namespace %s. Error: %v. Stderr: %s", serviceName, namespace, err, stderr.String())
	}

	// Check the output to confirm presence
	if !strings.Contains(out.String(), serviceName) {
		return fmt.Errorf("kubernetes service %s not found in namespace %s", serviceName, namespace)
	}

	return nil
}
