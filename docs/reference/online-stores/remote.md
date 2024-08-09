# Remote online store

## Description

This remote online store will let you interact with remote feature server. At this moment this only supports the read operation. You can use this online store and able retrieve online features `store.get_online_features`  from remote feature server. 

## Examples

The registry is pointing to registry of remote feature store. If it is not accessible then should be configured to use remote registry.

{% code title="feature_store.yaml" %}
```yaml
project: my-local-project
registry: /remote/data/registry.db
provider: local
online_store:
  path: http://localhost:6566
  type: remote
entity_key_serialization_version: 2
auth:
  type: no_auth
```
{% endcode %}

## How to configure Authentication and Authorization
Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.

