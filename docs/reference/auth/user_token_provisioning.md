# User Token Provisioning Guide

This document explains how users can provide Kubernetes user tokens from CLI, API, and SDK from the client side for Feast authentication.

## Overview

Feast supports multiple methods for providing user tokens to authenticate with Kubernetes-based feature stores. This guide covers all the available approaches for different use cases and environments.

## Table of Contents

- [SDK Configuration (Python)](#sdk-configuration-python)
- [CLI Usage](#cli-usage)
- [API Usage (REST/gRPC)](#api-usage-restgrpc)
- [Programmatic SDK Usage](#programmatic-sdk-usage)
- [Configuration Priority](#configuration-priority)
- [Security Best Practices](#security-best-practices)
- [Complete Examples](#complete-examples)
- [Troubleshooting](#troubleshooting)

## SDK Configuration (Python)

### Method 1: Direct Configuration in FeatureStore

```python
from feast import FeatureStore
from feast.permissions.auth_model import KubernetesAuthConfig

# Create auth config with user token
auth_config = KubernetesAuthConfig(
    type="kubernetes",
    user_token="your-kubernetes-user-token-here"
)

# Initialize FeatureStore with auth config
fs = FeatureStore(
    repo_path="path/to/feature_repo",
    auth_config=auth_config
)
```

### Method 2: Environment Variable

```python
import os
from feast import FeatureStore

# Set environment variable
os.environ["LOCAL_K8S_TOKEN"] = "your-kubernetes-user-token-here"

# FeatureStore will automatically use the token
fs = FeatureStore("path/to/feature_repo")
```

### Method 3: Configuration File

Create or update your `feature_store.yaml`:

```yaml
project: my-project
auth:
  type: kubernetes
  user_token: "your-kubernetes-user-token-here"
```
Feature Store will read the token from config YAML
```python
fs = FeatureStore("path/to/feature_repo")
```

**Note**: The `KubernetesAuthConfig` class is configured to allow extra fields, so the `user_token` field will be properly recognized when loaded from YAML files.

Then use in Python:

```python
from feast import FeatureStore

# FeatureStore will read auth config from feature_store.yaml
fs = FeatureStore("path/to/feature_repo")
```

## CLI Usage

### Method 1: Environment Variable

```bash
# Set the token as environment variable
export LOCAL_K8S_TOKEN="your-kubernetes-user-token-here"

# Use Feast CLI commands
feast apply
feast materialize
feast get-online-features \
  --features feature1,feature2 \
  --entity-rows '{"entity_id": "123"}'
```

### Method 2: Configuration File

Create or update your `feature_store.yaml`:

```yaml
project: my-project
auth:
  type: kubernetes
  user_token: "your-kubernetes-user-token-here"
```

Then use CLI commands:

```bash
feast apply
feast materialize
feast get-online-features \
  --features feature1,feature2 \
  --entity-rows '{"entity_id": "123"}'
```

## API Usage (REST/gRPC)

### REST API

#### Method 1: Authorization Header

```python
import requests

# For REST API
headers = {
    "Authorization": "Bearer your-kubernetes-user-token-here",
    "Content-Type": "application/json"
}

# Get features
response = requests.get(
    "http://feast-server/features",
    headers=headers
)

# Get online features
response = requests.post(
    "http://feast-server/get-online-features",
    headers=headers,
    json={
        "features": ["feature1", "feature2"],
        "entity_rows": [{"entity_id": "123"}]
    }
)
```

#### Method 2: Using requests Session

```python
import requests
from requests.auth import HTTPBearerAuth

# Create session with auth
session = requests.Session()
session.auth = HTTPBearerAuth("your-kubernetes-user-token-here")

# Make requests
response = session.get("http://feast-server/features")
```

### gRPC API

#### Method 1: gRPC Metadata

```python
import grpc
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest

# Create gRPC channel
channel = grpc.insecure_channel('feast-server:6565')
stub = ServingServiceStub(channel)

# Create metadata with auth token
metadata = [('authorization', 'Bearer your-kubernetes-user-token-here')]

# Create request
request = GetOnlineFeaturesRequest(
    features=["feature1", "feature2"],
    entity_rows=[{"entity_id": "123"}]
)

# Make gRPC call
response = stub.GetOnlineFeatures(request, metadata=metadata)
```

#### Method 2: gRPC Interceptor

```python
import grpc
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub

class AuthInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, token):
        self.token = token
    
    def intercept_unary_unary(self, continuation, client_call_details, request):
        # Add auth metadata
        metadata = list(client_call_details.metadata or [])
        metadata.append(('authorization', f'Bearer {self.token}'))
        
        # Update call details
        client_call_details = client_call_details._replace(metadata=metadata)
        return continuation(client_call_details, request)

# Create channel with interceptor
channel = grpc.insecure_channel('feast-server:6565')
interceptor = AuthInterceptor("your-kubernetes-user-token-here")
channel = grpc.intercept_channel(channel, interceptor)

# Use the channel
stub = ServingServiceStub(channel)
```

## Programmatic SDK Usage

### Token from Kubernetes Config

```python
from feast import FeatureStore
from feast.permissions.auth_model import KubernetesAuthConfig
from kubernetes import client, config

# Load kubeconfig and get token
config.load_kube_config()
v1 = client.CoreV1Api()

# Get token from Kubernetes API or secure storage
def get_token_from_k8s():
    # Example: Get token from secret
    secret = v1.read_namespaced_secret(
        name="user-token",
        namespace="default"
    )
    return secret.data["token"].decode("utf-8")

user_token = get_token_from_k8s()

auth_config = KubernetesAuthConfig(
    type="kubernetes",
    user_token=user_token
)

fs = FeatureStore(
    repo_path="path/to/feature_repo",
    auth_config=auth_config
)
```

## Configuration Priority

The system checks for tokens in this order:

1. **Intra-communication**: `INTRA_COMMUNICATION_BASE64` (for service-to-service)
2. **Direct configuration**: `user_token` in `KubernetesAuthConfig` or in `feature_store.yaml`
3. **Service account token**: `/var/run/secrets/kubernetes.io/serviceaccount/token` (for pods)
4. **Environment variable**: `LOCAL_K8S_TOKEN`


## Troubleshooting

### Common Issues

1. **Token Not Found**
   ```
   Error: Missing authentication token
   ```
   **Solution**: Ensure the token is provided through one of the supported methods.

2. **Invalid Token**
   ```
   Error: Invalid or expired access token
   ```
   **Solution**: Verify the token is valid and not expired.

3. **Permission Denied**
   ```
   Error: User is not added into the permitted groups / Namespaces
   ```
   **Solution**: Check that the user has the required groups/namespaces access.

## Related Documentation

- [Groups and Namespaces Authentication](./groups_namespaces_auth.md)
- [Authorization Manager](../../getting-started/components/authz_manager.md)
- [Permission Model](../../getting-started/concepts/permission.md)
- [RBAC Architecture](../../getting-started/architecture/rbac.md)
