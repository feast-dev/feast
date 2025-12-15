package utils

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/template"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type NotebookTemplateParams struct {
	Namespace             string
	IngressDomain         string
	OpenDataHubNamespace  string
	NotebookImage         string
	NotebookConfigMapName string
	NotebookPVC           string
	Username              string
	OC_TOKEN              string
	OC_SERVER             string
	NotebookFile          string
	Command               string
	PipIndexUrl           string
	PipTrustedHost        string
	FeastVerison          string
	OpenAIAPIKey          string
}

// CreateNotebook renders a notebook manifest from a template and applies it using kubectl.
func CreateNotebook(params NotebookTemplateParams) error {
	content, err := os.ReadFile("test/e2e_rhoai/resources/custom-nb.yaml")
	if err != nil {
		return fmt.Errorf("failed to read template file: %w", err)
	}

	tmpl, err := template.New("notebook").Parse(string(content))
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, params); err != nil {
		return fmt.Errorf("failed to substitute template: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "notebook-*.yaml")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Defer cleanup of temp file
	defer func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			fmt.Printf("warning: failed to remove temp file %s: %v", tmpFile.Name(), err)
		}
	}()

	if _, err := tmpFile.Write(rendered.Bytes()); err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// fmt.Println("Notebook manifest applied successfully")
	cmd := exec.Command("kubectl", "apply", "-f", tmpFile.Name(), "-n", params.Namespace)
	output, err := Run(cmd, "/test/e2e_rhoai")
	Expect(err).ToNot(HaveOccurred(), fmt.Sprintf(
		"Failed to create Notebook %s.\nError: %v\nOutput: %s\n",
		tmpFile.Name(), err, output,
	))
	fmt.Printf("Notebook %s created successfully\n", tmpFile.Name())
	return nil
}

// MonitorNotebookPod waits for a notebook pod to reach Running state and verifies execution logs.
func MonitorNotebookPod(namespace, podPrefix string, notebookName string) error {
	const successMarker = "Notebook executed successfully"
	const failureMarker = "Notebook execution failed"
	const pollInterval = 5 * time.Second
	var pod *PodInfo

	fmt.Println("🔄 Waiting for notebook pod to reach Running & Ready state...")

	foundRunningReady := false
	for i := 0; i < 36; i++ {
		var err error
		pod, err = getPodByPrefix(namespace, podPrefix)
		if err != nil {
			fmt.Printf("⏳ Pod not created yet: %v\n", err)
			time.Sleep(pollInterval)
			continue
		}
		if pod.Status == "Running" {
			fmt.Printf("✅ Pod %s is Running and Ready.\n", pod.Name)
			foundRunningReady = true
			break
		}
		fmt.Printf("⏳ Pod %s not ready yet. Phase: %s\n", pod.Name, pod.Status)
		time.Sleep(pollInterval)
	}

	if !foundRunningReady {
		return fmt.Errorf("❌ Pod %s did not reach Running & Ready state within 3 minutes", podPrefix)
	}

	// Start monitoring notebook logs
	fmt.Printf("⏳ Monitoring  Notebook pod %s Logs for Jupyter Notebook %s execution status\n", pod.Name, notebookName)

	for i := 0; i < 60; i++ {
		logs, err := getPodLogs(namespace, pod.Name)
		if err != nil {
			fmt.Printf("⏳ Failed to get logs for pod %s: %v\n", pod.Name, err)
			time.Sleep(pollInterval)
			continue
		}

		if strings.Contains(logs, successMarker) {
			Expect(logs).To(ContainSubstring(successMarker))
			fmt.Printf("✅ Jupyter Notebook pod %s executed successfully.\n", pod.Name)
			return nil
		}

		if strings.Contains(logs, failureMarker) {
			fmt.Printf("❌ Notebook pod %s failed: failure marker found.\n", pod.Name)
			return fmt.Errorf("Notebook failed in execution. Logs:\n%s", logs)
		}

		time.Sleep(pollInterval)
	}

	return fmt.Errorf("❌ Timed out waiting for notebook pod %s to complete", podPrefix)
}

type PodInfo struct {
	Name   string
	Status string
}

// returns the first pod matching a name prefix in the given namespace.
func getPodByPrefix(namespace, prefix string) (*PodInfo, error) {
	cmd := exec.Command(
		"kubectl", "get", "pods", "-n", namespace,
		"-o", "jsonpath={range .items[*]}{.metadata.name} {.status.phase}{\"\\n\"}{end}",
	)
	output, err := Run(cmd, "/test/e2e_rhoai")
	if err != nil {
		return nil, fmt.Errorf("failed to get pods: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		name := parts[0]
		status := parts[1]

		if strings.HasPrefix(name, prefix) {
			return &PodInfo{
				Name:   name,
				Status: status,
			}, nil
		}
	}

	return nil, fmt.Errorf("no pod found with prefix %q in namespace %q", prefix, namespace)
}

// retrieves the logs of a specified pod in the given namespace.
func getPodLogs(namespace, podName string) (string, error) {
	cmd := exec.Command("kubectl", "logs", "-n", namespace, podName)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("error getting pod logs: %v - %s", err, stderr.String())
	}

	return out.String(), nil
}

// returns the OpenShift cluster ingress domain.
func GetIngressDomain(testDir string) string {
	cmd := exec.Command("oc", "get", "ingresses.config.openshift.io", "cluster", "-o", "jsonpath={.spec.domain}")
	output, _ := Run(cmd, testDir)
	return string(output)
}

// returns the current OpenShift user authentication token.
func GetOCToken(testDir string) string {
	cmd := exec.Command("oc", "whoami", "--show-token")
	output, _ := Run(cmd, testDir)
	return string(output)
}

// returns the OpenShift API server URL for the current user.
func GetOCServer(testDir string) string {
	cmd := exec.Command("oc", "whoami", "--show-server")
	output, _ := Run(cmd, testDir)
	return string(output)
}

// returns the OpenShift cluster logged in Username
func GetOCUser(testDir string) string {
	cmd := exec.Command("oc", "whoami")
	output, _ := Run(cmd, testDir)
	return strings.TrimSpace(string(output))
}

// SetNamespaceContext sets the kubectl namespace context to the specified namespace
func SetNamespaceContext(namespace, testDir string) error {
	cmd := exec.Command("kubectl", "config", "set-context", "--current", "--namespace", namespace)
	output, err := Run(cmd, testDir)
	if err != nil {
		return fmt.Errorf("failed to set namespace context to %s: %w\nOutput: %s", namespace, err, output)
	}
	return nil
}

// CreateNotebookConfigMap creates a ConfigMap containing the notebook file and feature repo
func CreateNotebookConfigMap(namespace, configMapName, notebookFile, featureRepoPath, testDir string) error {
	cmd := exec.Command("kubectl", "create", "configmap", configMapName,
		"--from-file="+notebookFile,
		"--from-file="+featureRepoPath)
	output, err := Run(cmd, testDir)
	if err != nil {
		return fmt.Errorf("failed to create ConfigMap %s: %w\nOutput: %s", configMapName, err, output)
	}
	return nil
}

// CreateNotebookPVC creates a PersistentVolumeClaim for the notebook
func CreateNotebookPVC(pvcFile, testDir string) error {
	cmd := exec.Command("kubectl", "apply", "-f", pvcFile)
	_, err := Run(cmd, testDir)
	if err != nil {
		return fmt.Errorf("failed to create PVC from %s: %w", pvcFile, err)
	}
	return nil
}

// CreateNotebookRoleBinding creates a rolebinding for the user in the specified namespace
func CreateNotebookRoleBinding(namespace, rolebindingName, username, testDir string) error {
	cmd := exec.Command("kubectl", "create", "rolebinding", rolebindingName,
		"-n", namespace,
		"--role=admin",
		"--user="+username)
	_, err := Run(cmd, testDir)
	if err != nil {
		return fmt.Errorf("failed to create rolebinding %s: %w", rolebindingName, err)
	}
	return nil
}

// BuildNotebookCommand builds the command array for executing a notebook with papermill
func BuildNotebookCommand(notebookName, testDir string) []string {
	return []string{
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
			GetOCToken(testDir),
			GetOCServer(testDir),
			notebookName,
		),
	}
}

// GetNotebookParams builds and returns NotebookTemplateParams from environment variables and configuration
func GetNotebookParams(namespace, configMapName, notebookPVC, notebookName, testDir string) NotebookTemplateParams {
	username := GetOCUser(testDir)
	command := BuildNotebookCommand(notebookName, testDir)

	getEnv := func(key string) string {
		val, _ := os.LookupEnv(key)
		return val
	}

	return NotebookTemplateParams{
		Namespace:             namespace,
		IngressDomain:         GetIngressDomain(testDir),
		OpenDataHubNamespace:  getEnv("APPLICATIONS_NAMESPACE"),
		NotebookImage:         getEnv("NOTEBOOK_IMAGE"),
		NotebookConfigMapName: configMapName,
		NotebookPVC:           notebookPVC,
		Username:              username,
		OC_TOKEN:              GetOCToken(testDir),
		OC_SERVER:             GetOCServer(testDir),
		NotebookFile:          notebookName,
		Command:               "[\"" + strings.Join(command, "\",\"") + "\"]",
		PipIndexUrl:           getEnv("PIP_INDEX_URL"),
		PipTrustedHost:        getEnv("PIP_TRUSTED_HOST"),
		FeastVerison:          getEnv("FEAST_VERSION"),
		OpenAIAPIKey:          getEnv("OPENAI_API_KEY"),
	}
}

// SetupNotebookEnvironment performs all the setup steps required for notebook testing
func SetupNotebookEnvironment(namespace, configMapName, notebookFile, featureRepoPath, pvcFile, rolebindingName, testDir string) error {
	// Set namespace context
	if err := SetNamespaceContext(namespace, testDir); err != nil {
		return fmt.Errorf("failed to set namespace context: %w", err)
	}

	// Create config map
	if err := CreateNotebookConfigMap(namespace, configMapName, notebookFile, featureRepoPath, testDir); err != nil {
		return fmt.Errorf("failed to create config map: %w", err)
	}

	// Create PVC
	if err := CreateNotebookPVC(pvcFile, testDir); err != nil {
		return fmt.Errorf("failed to create PVC: %w", err)
	}

	// Create rolebinding
	username := GetOCUser(testDir)
	if err := CreateNotebookRoleBinding(namespace, rolebindingName, username, testDir); err != nil {
		return fmt.Errorf("failed to create rolebinding: %w", err)
	}

	return nil
}

// RunNotebookTest performs all the setup steps, creates a notebook, and monitors its execution.
// This function encapsulates the common notebook test workflow used across multiple test files.
func RunNotebookTest(namespace, configMapName, notebookFile, featureRepoPath, pvcFile, rolebindingName, notebookPVC, notebookName, testDir string) {
	// Execute common setup steps
	By(fmt.Sprintf("Setting namespace context to : %s", namespace))
	Expect(SetNamespaceContext(namespace, testDir)).To(Succeed())
	fmt.Printf("Successfully set namespace context to: %s\n", namespace)

	By(fmt.Sprintf("Creating Config map: %s", configMapName))
	Expect(CreateNotebookConfigMap(namespace, configMapName, notebookFile, featureRepoPath, testDir)).To(Succeed())
	fmt.Printf("ConfigMap %s created successfully\n", configMapName)

	By(fmt.Sprintf("Creating Persistent volume claim: %s", notebookPVC))
	Expect(CreateNotebookPVC(pvcFile, testDir)).To(Succeed())
	fmt.Printf("Persistent Volume Claim %s created successfully\n", notebookPVC)

	By(fmt.Sprintf("Creating rolebinding %s for the user", rolebindingName))
	Expect(CreateNotebookRoleBinding(namespace, rolebindingName, GetOCUser(testDir), testDir)).To(Succeed())
	fmt.Printf("Created rolebinding %s successfully\n", rolebindingName)

	// Build notebook parameters and create notebook
	nbParams := GetNotebookParams(namespace, configMapName, notebookPVC, notebookName, testDir)
	By("Creating Jupyter Notebook")
	Expect(CreateNotebook(nbParams)).To(Succeed(), "Failed to create notebook")

	By("Monitoring notebook logs")
	Expect(MonitorNotebookPod(namespace, "jupyter-nb-", notebookName)).To(Succeed(), "Notebook execution failed")
}
