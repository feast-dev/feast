# Remote Offline Store

## Description

The Python offline server is an Apache Arrow Flight Server that uses the gRPC communication protocol to exchange data.
This server wraps calls to existing offline store implementations and exposes interfaces as Arrow Flight endpoints.


## CLI

There is a CLI command that starts the remote offline server: `feast serve_offline`. By default, remote offline server uses port 8815, the port can be overridden with a `--port` flag.

## Deploying as a service on Kubernetes

The remote offline server can be deployed using helm chart see this [helm chart](https://github.com/feast-dev/feast/blob/master/infra/charts/feast-feature-server).

User need to set `feast_mode=offline`, when installing offline server as shown in the helm command below:

```
helm install feast-offline-server feast-charts/feast-feature-server --set feast_mode=offline  --set feature_store_yaml_base64=$(base64 > feature_store.yaml)
```

## Client Example

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

The complete example can be find under [remote-offline-store-example](../../../examples/remote-offline-store)

## Functionality Matrix

The set of functionalities supported by remote offline stores is the same as those supported by offline stores with the SDK, which are described in detail [here](../offline-stores/overview.md#functionality).

