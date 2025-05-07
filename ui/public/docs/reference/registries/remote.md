# Remote Registry

## Description

The Remote Registry is a gRPC client for the registry that implements the `RemoteRegistry` class using the existing `BaseRegistry` interface.

## How to configure the client

User needs to create a client side `feature_store.yaml` file, set the `registry_type` to `remote` and provide the server connection configuration.
The `path` parameter is a URL with a port (default is 6570) used by the client to connect with the Remote Registry server.

{% code title="feature_store.yaml" %}
```yaml
registry:
  registry_type: remote
  path: localhost:6570
```
{% endcode %}

The optional `cert` parameter can be configured as well, it should point to the public certificate path when the Registry Server starts in SSL mode. This may be needed if the Registry Server is started with a self-signed certificate, typically this file ends with *.crt, *.cer, or *.pem.
More info about the `cert` parameter can be found in [feast-client-connecting-to-remote-registry-sever-started-in-tls-mode](../../how-to-guides/starting-feast-servers-tls-mode.md#feast-client-connecting-to-remote-registry-sever-started-in-tls-mode)

## How to configure the server

Please see the detail how to configure registry server [registry-server.md](../feature-servers/registry-server.md)

## How to configure Authentication and Authorization
Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.
