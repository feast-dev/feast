# Remote Offline Store

## Description

The Remote Offline Store is an Arrow Flight client for the offline store that implements the `RemoteOfflineStore` class using the existing `OfflineStore` interface.
The client implements various methods, including `get_historical_features`, `pull_latest_from_table_or_query`, `write_logged_features`, and `offline_write_batch`.

## How to configure the client

User needs to create client side `feature_store.yaml` file and set the `offline_store` type `remote` and provide the server connection configuration 
including adding the host and specifying the port (default is 8815) required by the Arrow Flight client to connect with the Arrow Flight server.

{% code title="feature_store.yaml" %}
```yaml
offline_store:
  type: remote
  host: localhost
  port: 8815
```
{% endcode %}

## Client Example

The complete example can be find under [remote-offline-store-example](../../../examples/remote-offline-store)

## How to configure the server

Please see the detail how to configure offline feature server [offline-feature-server.md](../feature-servers/offline-feature-server.md)

## How to configure Authentication and Authorization
Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.
