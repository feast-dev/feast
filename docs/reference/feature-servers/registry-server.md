# Registry server

## Description

The Registry server supports both gRPC and REST interfaces for interacting with feature metadata. While gRPC remains the default protocol—enabling clients in any language with gRPC support—the REST API allows users to interact with the registry over standard HTTP using any REST-capable tool or language.

Feast supports running the Registry Server in three distinct modes:

| Mode        | Command Example                             | Description                            |
| ----------- | ------------------------------------------- | -------------------------------------- |
| gRPC only   | `feast serve_registry`                      | Default behavior for SDK and clients   |
| REST + gRPC | `feast serve_registry --rest-api`           | Enables both interfaces                |
| REST only   | `feast serve_registry --rest-api --no-grpc` | Used for REST-only clients like the UI |


## How to configure the server

## CLI

There is a CLI command that starts the Registry server: `feast serve_registry`. By default, remote Registry Server uses port 6570, the port can be overridden with a `--port` flag.
To start the Registry Server in TLS mode, you need to provide the private and public keys using the `--key` and `--cert` arguments.
More info about TLS mode can be found in [feast-client-connecting-to-remote-registry-sever-started-in-tls-mode](../../how-to-guides/starting-feast-servers-tls-mode.md#starting-feast-registry-server-in-tls-mode)

To enable REST API support along with gRPC, start the registry server with REST mode enabled : 

`feast serve_registry --rest-api`

This launches both the gRPC and REST servers concurrently. The REST server listens on port 6572 by default.

To run a REST-only server (no gRPC):

`feast serve_registry --rest-api --no-grpc`


## How to configure the client

Please see the detail how to configure Remote Registry client [remote.md](../registries/remote.md)

# Registry Server Permissions and Access Control

Please refer the [page](./../registry/registry-permissions.md) for more details on API Endpoints and Permissions.

## How to configure Authentication and Authorization ?

Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.