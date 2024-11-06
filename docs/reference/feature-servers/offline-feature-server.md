# Offline feature server

## Description

The Offline feature server is an Apache Arrow Flight Server that uses the gRPC communication protocol to exchange data.
This server wraps calls to existing offline store implementations and exposes interfaces as Arrow Flight endpoints.

## How to configure the server

## CLI

There is a CLI command that starts the Offline feature server: `feast serve_offline`. By default, remote offline server uses port 8815, the port can be overridden with a `--port` flag.

## Deploying as a service on Kubernetes

The Offline feature server can be deployed using helm chart see this [helm chart](https://github.com/feast-dev/feast/blob/master/infra/charts/feast-feature-server).

User need to set `feast_mode=offline`, when installing Offline feature server as shown in the helm command below:

```
helm install feast-offline-server feast-charts/feast-feature-server --set feast_mode=offline  --set feature_store_yaml_base64=$(base64 > feature_store.yaml)
```

## Server Example

The complete example can be found under [remote-offline-store-example](../../../examples/remote-offline-store)

## How to configure the client

Please see the detail how to configure offline store client [remote-offline-store.md](../offline-stores/remote-offline-store.md)

## Functionality Matrix

The set of functionalities supported by remote offline stores is the same as those supported by offline stores with the SDK, which are described in detail [here](../offline-stores/overview.md#functionality).

# Offline Feature Server Permissions and Access Control

## API Endpoints and Permissions

| Endpoint                              | Resource Type    | Permission    | Description                                       |
| ------------------------------------- |------------------|---------------|---------------------------------------------------|
| offline_write_batch                   | FeatureView      | Write Offline | Write a batch of data to the offline store        |
| write_logged_features                 | FeatureService   | Write Offline | Write logged features to the offline store        |
| persist                               | DataSource       | Write Offline | Persist the result of a read in the offline store |
| get_historical_features               | FeatureView      | Read Offline  | Retrieve historical features                      |
| pull_all_from_table_or_query          | DataSource       | Read Offline  | Pull all data from a table or read it             |
| pull_latest_from_table_or_query       | DataSource       | Read Offline  | Pull the latest data from a table or read it      |


## How to configure Authentication and Authorization ?

Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.