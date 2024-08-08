# Permission

## Overview

The Feast permissions model allows to configure granular permission policies to all the resources defined in a feature store.

The configured permissions are stored in the Feast registry and accessible through the CLI and the registry APIs.

The permission authorization enforcement is performed when requests are executed through one of the Feast (Python) servers
- The online feature server (REST)
- The offline feature server (Arrow Flight)
- The registry server (gRPC)

On the contrary, there is no permission enforcement when accessing the Feast API with a local provider.

## Concepts

The permission model is based on the following components:
- A `resource` is a Feast object that we want to secure against unauthorized access.
  - We assume that the resource has a `name` attribute and optional dictionary of associated key-value `tags`.
- An `action` is a logical operation executed on the secured resource, like:
  - `create`: Create an instance.
  - `read`: Access the instance state.
  - `update`: Update the instance state.
  - `delete`: Delete an instance.
  - `query`:  Query both online and offline stores.
  - `query_online`:  Query the online store.
  - `query_offline`:  Query the offline store.
  - `write`:  Query on any store.
  - `write_online`:  Write to the online store.
  - `write_offline`:  Write to the offline store.
- A `policy` identifies the rule for enforcing authorization decisions on secured resources, based on the current user.
  - A default implementation is provided for role-based policies, using the user roles to grant or deny access to the requested actions
  on the secured resources.

The `Permission` class identifies a single permission configured on the feature store and is identified by these attributes:
- `name`: The permission name.
- `types`: The list of protected resource  types. Defaults to all managed types, e.g. the `ALL_RESOURCE_TYPES` alias
- `with_subclasses`: Specify if sub-classes are included in the resource match or not. Defaults to `True`.
- `name_pattern`: A regex to match the resource name. Defaults to `None`, meaning that no name filtering is applied
- `required_tags`: Dictionary of key-value pairs that must match the resource tags. Defaults to `None`, meaning that no tags filtering is applied.
- `actions`: The actions authorized by this permission. Defaults to `ALL_VALUES`, an alias defined in the `action` module.
- `policy`: The policy to be applied to validate a client request.

Given the above definitions, the feature store can be configured with granular control over each resource, enabling partitioned access by 
teams to meet organizational requirements for service and data sharing, and protection of sensitive information.

The `feast` CLI includes a new `permissions` command to list the registered permissions, with options to identify the matching resources for each configured permission and the existing resources that are not covered by any permission.

{% hint style="info" %}
**Note**: Feast resources that do not match any of the configured permissions are not secured by any authorization policy, meaning any user can execute any action on such resources.
{% endhint %}

## Configuration examples
This permission configuration allows access to the resource state and the ability to query all the stores for any feature view or feature service
to all users with role `super-reader`:
```py
Permission(
    name="feature-reader",
    types=[FeatureView, FeatureService],
    policy=RoleBasedPolicy(roles=["super-reader"]),
    actions=[AuthzedAction.READ, QUERY],
)
```
Please note that all sub-classes of `FeatureView` are also included since the default for the `with_subclasses` parameter is `True`.

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


The following permission grants authorization to query the offline store of all the feature views including `risky` in the name, to users with role `trusted`:
```py
Permission(
    name="reader",
    types=[FeatureView],
    with_subclasses=False, # exclude sub-classes
    name_pattern=".*risky.*",
    policy=RoleBasedPolicy(roles=["trusted"]),
    actions=[AuthzedAction.QUERY_OFFLINE],
)
```

## Authorizing Feast clients
If you would like to leverage the permissions' functionality then `auth` config block should be added to feature_store.yaml. Currently, feast supports oidc and kubernetes rbac authentication/authorization. 

The default configuration if you don't mention the auth configuration is no_auth configuration.

* No auth configuration example:
```yaml
project: foo
registry: "registry.db"
provider: local
online_store:
    path: foo
entity_key_serialization_version: 2
auth:
  type: no_auth
```

* Kubernetes rbac authentication/authorization example:
{% hint style="info" %}
**NOTE**: This configuration will only work if you deploy feast on Openshift or a Kubernetes platform.
{% endhint %}
```yaml
project: foo
registry: "registry.db"
provider: local
online_store:
  path: foo
entity_key_serialization_version: 2
auth:
  type: kubernetes
```
* OIDC authorization: Below configuration is an example for OIDC authorization example. 
```yaml
project: foo
auth:
  type: oidc
  client_id: test_client_id
  client_secret: test_client_secret
  username: test_user_name
  password: test_password
  realm: master
  auth_discovery_url: http://localhost:8080/realms/master/.well-known/openid-configuration
registry: "registry.db"
provider: local
online_store:
  path: foo
entity_key_serialization_version: 2
```
Few of the key models, classes to understand the authorization implementation from the clients can be found [here](./../../../sdk/python/feast/permissions/client).