# Azure (contrib)

## Description

* Offline Store: Uses the **MsSql** offline store by default. Also supports File as the offline store.
* Online Store: Uses the **Redis** online store by default. Also supports Sqlite as an online store.

## Disclaimer

The Azure provider does not achieve full test coverage.
Please do not assume complete stability.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[azure]'`. You can get started by then following this [tutorial](https://github.com/feast-dev/feast/blob/master/docs/tutorials/azure/README.md).

## Example

{% code title="feature_store.yaml" %}
```yaml
registry:
  registry_store_type: AzureRegistryStore
  path: ${REGISTRY_PATH} # Environment Variable
project: production
provider: azure
online_store:
    type: redis
    connection_string: ${REDIS_CONN} # Environment Variable
```
{% endcode %}