# Feast Namespace Registry

## Overview

The Feast Namespace Registry is a feature that automatically creates and maintains a centralized ConfigMap containing information about all Feast feature store instances deployed by the operator. This enables dashboard applications and other tools to discover and connect to Feast instances across different namespaces.

## Implementation Details

1. **ConfigMap Creation**: The operator creates a ConfigMap in the appropriate namespace:
   - **OpenShift AI**: `redhat-ods-applications` namespace (or DSCi configured namespace)
   - **Kubernetes**: the namespace where the operator is installed, unless `FEAST_OPERATOR_NAMESPACE` is set

2. **Access Control**: A RoleBinding is created to allow `system:authenticated` users to read the ConfigMap

3. **Automatic Registration & Cleanup**: When a new feature store instance is created, it automatically registers its namespace and client configuration in the ConfigMap. When deleted, it automatically removes its entry from the ConfigMap

4. **Data Structure**: The ConfigMap contains a JSON structure with namespace names as keys and lists of client configuration names as values

### Upgrade Compatibility

For Kubernetes upgrades, if the legacy `feast-configs-registry` ConfigMap already exists in
`feast-operator-system`, the operator continues updating and cleaning up its FeatureStore entries
there as well as in its current namespace. The same compatibility behavior applies to an existing
`feast-openlineage-config` ConfigMap. This keeps clients that still read the legacy location current.
This compatibility lookup only recognizes the legacy `feast-operator-system` namespace; ConfigMaps
in other historical custom namespaces are not discovered automatically.

These ConfigMaps are shared and are not deleted by the operator. Migrate readers before manually
retiring a legacy ConfigMap, and verify that no other operator instance still uses it.

### ConfigMap Structure

The namespace registry ConfigMap (`feast-configs-registry`) contains the following data:

```json
{
  "namespaces": {
    "namespace-1": ["client-config-1", "client-config-2"],
    "namespace-2": ["client-config-3"]
  }
}
```

### Usage

The namespace registry is automatically deployed when any Feast feature store instance is created. No additional configuration is required.

#### For External Applications

External applications can discover Feast instances by:

1. Reading the ConfigMap from the appropriate namespace:
   ```bash
   # For OpenShift
   kubectl get configmap feast-configs-registry -n redhat-ods-applications -o jsonpath='{.data.namespaces}'
   
   # For Kubernetes, use the namespace where the operator is installed
   kubectl get configmap feast-configs-registry -n <operator-namespace> -o jsonpath='{.data.namespaces}'
   ```

   During an upgrade, clients may continue reading `feast-operator-system` while its legacy
   ConfigMap exists; the operator keeps that ConfigMap's FeatureStore entries current.

### Lifecycle Management

The namespace registry automatically manages the lifecycle of feature store instances:

1. **Creation**: When a feature store is deployed, it registers itself in the ConfigMap
2. **Updates**: If a feature store is updated, its entry remains in the ConfigMap
3. **Deletion**: When a feature store is deleted, its entry is automatically removed from the ConfigMap
4. **Namespace Cleanup**: If all feature stores in a namespace are deleted, the namespace entry is also removed
