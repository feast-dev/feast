# Feast Operator RBAC with TLS (OpenShift)

This directory contains examples and configurations for using Feast with Role-Based Access Control (RBAC) in an OpenShift environment with TLS authentication.

## Contents

- `1-setup-operator-rbac.ipynb`: Jupyter notebook for setting up RBAC with TLS in OpenShift
- `2-client-rbac-test-pod.ipynb`: Jupyter notebook demonstrating RBAC testing with TLS in OpenShift
- `3-uninstall.ipynb`: Jupyter notebook for cleaning up the RBAC setup
- `permissions_apply.py`: Python script for applying RBAC permissions with TLS configuration
- `client/`: Directory containing client configurations
  - `readonly_user_deployment_tls.yaml`: Deployment configuration for readonly users with TLS
  - `admin_user_deployment_tls.yaml`: Deployment configuration for admin users with TLS
  - `unauthorized_user_deployment_tls.yaml`: Deployment configuration for unauthorized users with TLS
  - `feature_repo/`: Feature repository configurations
    - `feature_store.yaml`: Feature store configuration with TLS settings
    - `test.py`: Contents numerous tests for validation of permissions while accessing feast objects

## Key Features

- TLS certificate configuration for secure communication
- OpenShift service CA certificate integration
- RBAC with service account authentication
- HTTPS endpoints (port 443)
- Separate configurations for admin, readonly, and unauthorized users

## Usage

1. Set up RBAC with TLS:
   - Option 1: Run `1-setup-operator-rbac.ipynb` notebook
   - Option 2: Run `python permissions_apply.py` script

2. Apply the appropriate deployment configurations

3. Test RBAC functionality with TLS authentication using `2-client-rbac-test-pod.ipynb`

4. Clean up resources using `3-uninstall.ipynb` when done

For more details, refer to the [Feast documentation](https://docs.feast.dev/master/getting-started/components/authz_manager#kubernetes-rbac-authorization). 