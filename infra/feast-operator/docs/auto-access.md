# Feast Auto Access Enablement

## Overview

The Feast Operator automatically enables discovery and view access for users and groups defined in Feast permissions. Dashboard applications use user credentials to list Feast namespaces and client ConfigMaps, with no dependency on a central registry or service account.

## Namespace Labeling

The operator adds the label `opendatahub.io/feast: "true"` to namespaces that contain a deployed FeatureStore. This enables dashboards to discover Feast namespaces cluster-wide by listing namespaces with this label selector.

- **Label**: `opendatahub.io/feast=true`
- **When added**: After a FeatureStore is successfully deployed
- **When removed**: When the last FeatureStore in the namespace is deleted

## Auto-Access RBAC

When the registry REST API is enabled and permissions are defined in `permissions.py`, the operator creates RBAC so users and groups from Feast permissions get view access:

1. **GroupBasedPolicy**: All users in the listed groups
2. **NamespaceBasedPolicy**: All users/groups with any RoleBinding in the Data Science Project (K8s namespace)
3. **CombinedGroupNamespacePolicy**: Both groups and namespace-based subjects

### RBAC Resources Created

- **ClusterRole** `feast-discover-namespaces`: Allows `get`, `list`, `watch` on namespaces (for cluster-wide discovery)
- **ClusterRoleBinding** (per FeatureStore): Binds the ClusterRole to subjects from permissions
- **Role** (namespace-scoped): Allows `get`, `list`, `watch` on the client ConfigMap
- **RoleBinding** (namespace-scoped): Binds the Role to the same subjects

### View Access

View access grants:
- List namespaces cluster-wide (with label `opendatahub.io/feast=true`)
- Get/list/watch the Feature Store client ConfigMap in each accessible namespace

## Dashboard Contract

Dashboards should:

1. List namespaces with label selector `opendatahub.io/feast=true` using the user's token
2. For each namespace the user can access, list ConfigMaps to find client ConfigMaps (e.g. `feast-<name>-client`)

## Prerequisites

- Registry REST API must be enabled (`spec.services.registry.local.server.restAPI: true`)
- Permissions must be applied via `feast apply` (from `permissions.py` in the feature repo)
- The operator reconciles permissions periodically (every 30 seconds) to pick up changes
