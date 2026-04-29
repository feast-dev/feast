# ADR-0006: Kubernetes Operator

## Status

Accepted

## Context

As the Feast project grew, deploying a fully functional Feature Store in a production-like manner became increasingly difficult. Existing installers required many manual operations that led to configuration errors. Users needed a simpler way to install and maintain Feature Store environments, especially with features like RBAC.

The existing Helm-based operator had limitations in handling complex installation requirements. A more capable operator was needed to manage the full lifecycle of Feast deployments on Kubernetes.

## Decision

Build a **Go Operator** using the `operator-sdk` framework with a cluster-scoped controller and a namespaced `FeatureStore` Custom Resource Definition (CRD).

### FeatureStore CRD

The operator manages Feast through a single CRD that defines the entire feature store deployment:

```yaml
apiVersion: feast.dev/v1alpha1
kind: FeatureStore
metadata:
  name: example
  namespace: feast
spec:
  feastProject: my-project
  auth:
    kubernetes:
      roles: [reader, writer]
  services:
    registry:
      replicas: 1
      persistence:
        file:
          pvc:
            capacity: 5Gi
    onlineStore:
      replicas: 2
      persistence:
        postgresql:
          secretRef: online-store-creds
    offlineStore:
      replicas: 3
  feastApplyJob:
    configMapRef: feast-definitions
```

### Architecture

- **Operator deploys Feast services** (Registry, Online Store, Offline Store) as defined in the CR.
- **Operator generates `feature_store.yaml`** from the CR spec, including only relevant sections for each server type.
- **Client ConfigMap** is created automatically for remote connectivity.
- **`feast apply` Job** can be triggered from a ConfigMap or Git repo to initialize the registry.
- **CR status.applied** is the single source of truth for the deployed state.

### Key Decisions

- **Go over Python**: Go is better suited for Kubernetes operators. Python is great for ML work but not for cloud-native infrastructure management.
- **Single CRD** (`FeatureStore`) instead of separate CRDs per service type. All services are part of a functioning Feature Store and should be managed together.
- **Operator manages `feature_store.yaml`** entirely to ensure consistency and validation (e.g., `remote` types are only used where appropriate).
- **Data store deployments are out of scope**: The operator assumes data stores are pre-provisioned and accessible.
- **Deprecation of Helm-based operator**: The existing Helm-based operator is deprecated in favor of the Go operator.

### Persistence Options

- **Default**: Ephemeral file-based stores.
- **File with PVC**: For clusters supporting persistent volumes.
- **PostgreSQL**: Via referenced Kubernetes secrets for credentials.

## Consequences

### Positive

- Simplified, standardized deployment of Feast on Kubernetes.
- Full lifecycle management including RBAC, metrics, and feature store initialization.
- Supports multiple Feature Store deployments in a single cluster without conflict.
- Proper validation and consistency enforcement through the operator reconciliation loop.
- Deployable with kustomize; compatible with OLM and OperatorHub.

### Negative

- Requires Kubernetes as the deployment platform.
- Data store management is left to users (intentionally out of scope).
- Initial release supports limited persistence backends; additional stores added incrementally.

## References

- Original RFC: [Feast RFC-042: Operator](https://docs.google.com/document/d/1vGKMizf3_14IyiF_W_Ik7CR03joFkQfzbKT0jH4PZJM/edit)
- GitHub Issue: [#4561](https://github.com/feast-dev/feast/issues/4561)
- Implementation: `infra/feast-operator/`
