# Notebook ConfigMap Integration

## Overview

The Feast operator can watch OpenDataHub Notebook custom resources and automatically create/update a `notebook-feast-config` ConfigMap in each notebook's namespace. This ConfigMap contains Feast client configuration for all projects referenced by the notebook's annotation.

## Architecture

The integration works bidirectionally:

1. **Notebook → ConfigMap**: When a Notebook's annotation changes, the operator creates/updates the `notebook-feast-config` with Feast client configs for all referenced projects.

2. **FeatureStore → ConfigMap**: When a FeatureStore's client ConfigMap changes, the operator updates all `notebook-feast-config` that reference that project.

## Label Format

Notebooks must have the following label:

```yaml
labels:
  opendatahub.io/feast-integration: "true"
```
And the following annotation:

```yaml
annotations:
  opendatahub.io/feast-config: "project1,project2,project3"
```

The annotation value is a comma-separated list of Feast project names. The operator will:
- Fetch the client ConfigMap for each project from the FeatureStore
- Create/update `notebook-feast-config` in the notebook's namespace
- Use project names as keys and client ConfigMap YAML content as values

## ConfigMap Structure

The `notebook-feast-config` ConfigMap is created in the same namespace as the Notebook and has the following structure:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: <notebook_name>-feast-config
  namespace: <notebook-namespace>
  labels:
    managed-by: feast-operator
  ownerReferences:
    - apiVersion: kubeflow.org/v1
      kind: Notebook
      name: <notebook_name>
      uid: <notebook-uid>
data:
  <project_name>: |
    <project_yaml_content>
```

Each key in `data` is a Feast project name, and the value is the YAML content from that project's client ConfigMap. The ConfigMap is owned by the Notebook and will be deleted when the Notebook is deleted.

## How It Works

### 1. Notebook Reconciliation

When a Notebook is created or updated:

1. Operator checks for `opendatahub.io/feast-integration` label is set to `true`
2. Parses comma-separated project names from `opendatahub.io/feast-config` annotation
3. Creates/updates `notebook-feast-config` ConfigMap with project names as keys and YAML content from the project's client ConfigMap as values in the notebook's namespace

### 2. FeatureStore Reconciliation

When a FeatureStore's client ConfigMap changes:

1. Operator identifies the project name from the FeatureStore's `spec.feastProject`
2. Lists all Notebooks across all namespaces
3. For each Notebook with the project in its `opendatahub.io/feast-config` annotation:
   - Triggers reconciliation of that Notebook's `notebook-feast-config` ConfigMap
4. Updates the `notebook-feast-config` ConfigMap with the new client config

### 3. Cleanup

When a Notebook is deleted:
- The `notebook-feast-config` ConfigMap is automatically deleted (via owner reference)

When a project is removed from a Notebook's `opendatahub.io/feast-config` annotation:
- The corresponding entry is removed from the ConfigMap

## Example Usage

### 1. Create a FeatureStore

```yaml
apiVersion: feast.dev/v1alpha1
kind: FeatureStore
metadata:
  name: my-feast-store
  namespace: feast-system
spec:
  feastProject: my-project
  services:
    # ... service configuration
```

This creates a client ConfigMap (e.g., `my-feast-store-client`) with the Feast configuration.

### 2. Create a Notebook with Feast Projects

```yaml
apiVersion: kubeflow.org/v1
kind: Notebook
metadata:
  name: my-notebook
  namespace: user-namespace
  labels:
    opendatahub.io/feast-integration: "true"
  annotations:
    opendatahub.io/feast-config: "my-project,another-project"
spec:
  # ... notebook configuration
```

The operator automatically creates `my-notebook-feast-config` ConfigMap in `user-namespace`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-notebook-feast-config
  namespace: user-namespace
  labels:
    managed-by: feast-operator
  ownerReferences:
    - apiVersion: kubeflow.org/v1
      kind: Notebook
      name: my-notebook
      uid: <notebook-uid>
data:
  my-project: |
    # YAML content from my-project's client ConfigMap
  another-project: |
    # YAML content from another-project's client ConfigMap
```

### 3. Update Notebook Annotation

If you update the Notebook annotation:

```yaml
annotations:
  opendatahub.io/feast-config: "my-project"  # Removed another-project
```

The operator automatically removes `another-project` from the ConfigMap.

### 4. Update FeatureStore

If you update the FeatureStore configuration, the operator automatically updates all `my-notebook-feast-config` ConfigMaps that reference that project.

## Configuration

The Notebook GVK is configured in `cmd/main.go`:

```go
notebookGVK := schema.GroupVersionKind{
    Group:   "kubeflow.org",
    Version: "v1",
    Kind:    "Notebook",
}
```

To use a different Notebook CRD, modify this GVK accordingly.

## RBAC Permissions

The operator requires the following permissions:

```yaml
- apiGroups:
  - kubeflow.org
  resources:
  - notebooks
  verbs:
  - get
  - list
  - watch
- apiGroups:
  - feast.dev
  resources:
  - featurestores
  verbs:
  - get
  - list
  - watch
- apiGroups:
  - ""
  resources:
  - configmaps
  verbs:
  - get
  - list
  - watch
  - create
  - update
  - patch
  - delete
```

## Troubleshooting

### ConfigMap Not Created

- Verify the Notebook has the `opendatahub.io/feast-integration` label set to `true`
- Check that the annotation `opendatahub.io/feast-config` contains valid project names
- Ensure FeatureStores exist for all referenced projects
- Check operator logs for errors
### Project Not Found
- Ensure a FeatureStore exists with `spec.feastProject` matching the project name
- Verify the FeatureStore is in Ready state
- Check that the FeatureStore has a client ConfigMap created

