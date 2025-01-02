# Permission

## Overview

The Feast permissions model allows to configure granular permission policies to all the resources defined in a feature store.

The configured permissions are stored in the Feast registry and accessible through the CLI and the registry APIs.

The permission authorization enforcement is performed when requests are executed through one of the Feast (Python) servers
- The online feature server (REST)
- The offline feature server (Arrow Flight)
- The registry server (gRPC)

Note that there is no permission enforcement when accessing the Feast API with a local provider.

## Concepts

The permission model is based on the following components:
- A `resource` is a Feast object that we want to secure against unauthorized access.
  - We assume that the resource has a `name` attribute and optional dictionary of associated key-value `tags`.
- An `action` is a logical operation executed on the secured resource, like:
  - `create`: Create an instance.
  - `describe`: Access the instance state.
  - `update`: Update the instance state.
  - `delete`: Delete an instance.
  - `read`:  Read both online and offline stores.
  - `read_online`:  Read the online store.
  - `read_offline`:  Read the offline store.
  - `write`:  Write on any store.
  - `write_online`:  Write to the online store.
  - `write_offline`:  Write to the offline store.
- A `policy` identifies the rule for enforcing authorization decisions on secured resources, based on the current user.
  - A default implementation is provided for role-based policies, using the user roles to grant or deny access to the requested actions
  on the secured resources.

The `Permission` class identifies a single permission configured on the feature store and is identified by these attributes:
- `name`: The permission name.
- `types`: The list of protected resource  types. Defaults to all managed types, e.g. the `ALL_RESOURCE_TYPES` alias. All sub-classes are included in the resource match.
- `name_patterns`: A list of regex patterns to match resource names. If any regex matches, the `Permission` policy is applied. Defaults to `[]`, meaning no name filtering is applied.
- `required_tags`: Dictionary of key-value pairs that must match the resource tags. Defaults to `None`, meaning that no tags filtering is applied.
- `actions`: The actions authorized by this permission. Defaults to `ALL_VALUES`, an alias defined in the `action` module.
- `policy`: The policy to be applied to validate a client request.

To simplify configuration, several constants are defined to streamline the permissions setup:
- In module `feast.feast_object`:
  - `ALL_RESOURCE_TYPES` is the list of all the `FeastObject` types.
  - `ALL_FEATURE_VIEW_TYPES` is the list of all the feature view types, including those not inheriting from `FeatureView` type like 
  `OnDemandFeatureView`.
- In module `feast.permissions.action`:
  - `ALL_ACTIONS` is the list of all managed actions.
  - `READ` includes all the read actions for online and offline store.
  - `WRITE` includes all the write actions for online and offline store.
  - `CRUD` includes all the state management actions to create, describe, update or delete a Feast resource.

Given the above definitions, the feature store can be configured with granular control over each resource, enabling partitioned access by 
teams to meet organizational requirements for service and data sharing, and protection of sensitive information.

The `feast` CLI includes a new `permissions` command to list the registered permissions, with options to identify the matching resources for each configured permission and the existing resources that are not covered by any permission.

{% hint style="info" %}
**Note**: Feast resources that do not match any of the configured permissions are not secured by any authorization policy, meaning any user can execute any action on such resources.
{% endhint %}

## Definition examples
This permission definition grants access to the resource state and the ability to read all of the stores for any feature view or
feature service to all users with the role `super-reader`:
```py
Permission(
    name="feature-reader",
    types=[FeatureView, FeatureService],
    policy=RoleBasedPolicy(roles=["super-reader"]),
    actions=[AuthzedAction.DESCRIBE, *READ],
)
```

This example grants permission to write on all the data sources with `risk_level` tag set to `high` only to users with role `admin` or `data_team`:
```py
Permission(
    name="ds-writer",
    types=[DataSource],
    required_tags={"risk_level": "high"},
    policy=RoleBasedPolicy(roles=["admin", "data_team"]),
    actions=[AuthzedAction.WRITE],
)
```

{% hint style="info" %}
**Note**: When using multiple roles in a role-based policy, the user must be granted at least one of the specified roles.
{% endhint %}


The following permission grants authorization to read the offline store of all the feature views including `risky` in the name, to users with role `trusted`:

```py
Permission(
    name="reader",
    types=[FeatureView],
    name_patterns=".*risky.*", # Accepts both `str` or `list[str]` types
    policy=RoleBasedPolicy(roles=["trusted"]),
    actions=[AuthzedAction.READ_OFFLINE],
)
```

## Authorization configuration
In order to leverage the permission functionality, the `auth` section is needed in the `feature_store.yaml` configuration.
Currently, Feast supports OIDC and Kubernetes RBAC authorization protocols.

The default configuration, if you don't specify the `auth` configuration section, is `no_auth`, indicating that no permission
enforcement is applied.

The `auth` section includes a `type` field specifying the actual authorization protocol, and protocol-specific fields that
are specified in [Authorization Manager](../components/authz_manager.md).
