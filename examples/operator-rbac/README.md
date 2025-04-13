# Running the Feast RBAC example on Kubernetes using the Feast Operator.

1. [1-setup-operator-rbac.ipynb](1-setup-operator-rbac.ipynb) will guide you through how to setup Role-Based Access Control (RBAC) for Feast using the [Feast Operator](../../infra/feast-operator/)  and Kubernetes Authentication. This Feast Admin Step requires you to setup the operator and Feast RBAC on K8s. 
2. [2-client-rbac-test-pod.ipynb](2-client-rbac-test-pod.ipynb) notebook from within a Kubernetes pod to validate RBAC.
3. [3-client-rbac-test-local.ipynb](3-client-rbac-test-local.ipynb) Validate the RBAC with the client example using different test cases using a service account token locally.
4. [04-uninstall.ipynb](04-uninstall.ipynb) Clear the installed deployments and K8s Objects.

