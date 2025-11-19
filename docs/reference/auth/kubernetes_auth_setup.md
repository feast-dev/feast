# Setting up the kubernetes Auth

This document describes the authentication and authorization capabilities in Feast that support groups, namespaces and roles extraction from Kubernetes tokens.

## Overview

Feast supports extracting user groups, namespaces and roles of both Service Account and User from Kubernetes authentication tokens. This allows for more granular access control based on:

- **Groups**: User groups associated directly with User/SA and from associated namespace 
- **Namespaces**: Kubernetes namespaces associated with User/SA
- **Roles**: Kubernetes roles associated with User/SA

## Key Features

### Setting Up Kubernetes RBAC for Feast

#### Role based auth setup

To ensure the Kubernetes RBAC environment aligns with Feast's RBAC configuration, follow these guidelines:
* The roles defined in Feast `Permission` instances must have corresponding Kubernetes RBAC `Role` names. 
* The Kubernetes RBAC `Role` must reside in the same namespace as the Feast service.
* The client application can run in a different namespace, using its own dedicated `ServiceAccount`.
* Finally, the `RoleBinding` that links the client `ServiceAccount` to the RBAC `Role` must be defined in the namespace of the Feast service.

#### Group and Namespace based auth setup

To ensure the Kubernetes RBAC environment aligns with Feast's RBAC configuration, follow these guidelines:
* The groups and namespaces defined in Feast `Permission` instances must have corresponding Kubernetes `Group` and `Namespace` names.
* The user or service account must reside in the group or namespace defined in the Feast `Permission` instances.
* The client application can run in a different namespace, using its own dedicated `ServiceAccount` or user.
* Finally, the feast service grants access based on the group and namespace association defined in the Feast `Permission` instances.

## Policy Types

### RoleBasedPolicy
Grants access based on user role membership.

```python
from feast.permissions.policy import RoleBasedPolicy

policy = RoleBasedPolicy(roles=["data-team", "ml-engineers"])
```

### GroupBasedPolicy
Grants access based on user group membership.

```python
from feast.permissions.policy import GroupBasedPolicy

policy = GroupBasedPolicy(groups=["data-team", "ml-engineers"])
```

#### NamespaceBasedPolicy
Grants access based on user namespace association.

```python
from feast.permissions.policy import NamespaceBasedPolicy

policy = NamespaceBasedPolicy(namespaces=["production", "staging"])
```

#### CombinedGroupNamespacePolicy
Grants access only when user is added into either permitted groups OR namespaces.

```python
from feast.permissions.policy import CombinedGroupNamespacePolicy

policy = CombinedGroupNamespacePolicy(
    groups=["data-team"],
    namespaces=["production"]
)
```

## Configuration

### Server Configuration

The server automatically extracts groups, namespaces and roles when using Kubernetes authentication. No additional configuration is required beyond the existing Kubernetes auth setup.

### Client Configuration

For external users (not service accounts), you can provide a user token in the configuration:

Refer examples of providing the token are described in doc [User Token Provisioning](./user_token_provisioning.md)

## Usage Examples

### Basic Permission Setup

```python
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.permissions.action import READ, AuthzedAction, ALL_ACTIONS
from feast.permissions.permission import Permission
from feast.permissions.policy import (
    RoleBasedPolicy,
    GroupBasedPolicy,
    NamespaceBasedPolicy,
    CombinedGroupNamespacePolicy
)

# Role-based permission
role_perm = Permission(
    name="role_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=["reader-role"]),
    actions=[AuthzedAction.DESCRIBE] + READ
)

# Group-based permission (new)
data_team_perm = Permission(
    name="data_team_permission",
    types=ALL_RESOURCE_TYPES,
    policy=GroupBasedPolicy(groups=["data-team", "ml-engineers"]),
    actions=[AuthzedAction.DESCRIBE] + READ
)

# Namespace-based permission (new)
prod_perm = Permission(
    name="production_permission",
    types=ALL_RESOURCE_TYPES,
    policy=NamespaceBasedPolicy(namespaces=["production"]),
    actions=[AuthzedAction.DESCRIBE] + READ
)

# Combined permission (new)
dev_staging_perm = Permission(
    name="dev_staging_permission",
    types=ALL_RESOURCE_TYPES,
    policy=CombinedGroupNamespacePolicy(
        groups=["dev-team"],
        namespaces=["staging"]
    ),
    actions=ALL_ACTIONS
)
```

### Applying Permissions

Run `feast apply` from CLI/API/SDK on server or from client(if permitted) to apply the permissions.

## Troubleshooting

### Common Issues

1. **Token Access Review Fails**
   - Check that the Feast server has the required RBAC permissions
   - Verify the token is valid and not expired
   - Check server logs for detailed error messages in debug mode

2. **Groups/Namespaces Not Extracted**
   - Verify the token contains the expected claims
   - Check that the user is properly configured in Kubernetes/ODH/RHOAI

3. **Permission Denied**
   - Verify the user is added to required groups/namespaces Or has the required role assigned
   - Check that the policy is correctly configured
   - Review the permission evaluation logs

## Migration Guide

### From Role-Based to Group/Namespace-Based

1. **Identify User Groups**: Determine which groups your users belong to
2. **Map Namespaces**: Identify which namespaces users should have access to
3. **Create New Policies**: Define group-based and namespace-based policies
4. **Test Gradually**: Start with read-only permissions and gradually expand
5. **Monitor**: Watch logs to ensure proper authentication and authorization


## Best Practices

1. **Principle of Least Privilege**: Grant only the minimum required permissions
2. **Group Organization**: Organize users into logical groups based on their responsibilities
3. **Namespace Isolation**: Use namespaces to isolate different environments or teams
4. **Regular Audits**: Periodically review and audit permissions

## Related Documentation

- [Authorization Manager](./authz_manager.md)
- [Permission Model](../concepts/permission.md)
- [RBAC Architecture](../architecture/rbac.md)
- [Kubernetes RBAC Authorization](./authz_manager.md#kubernetes-rbac-authorization)
