# Registry server

## Description

The Registry server uses the gRPC communication protocol to exchange data.
This enables users to communicate with the server using any programming language that can make gRPC requests.

## How to configure the server

## CLI

There is a CLI command that starts the Registry server: `feast serve_registry`. By default, remote Registry Server uses port 6570, the port can be overridden with a `--port` flag.
To start the Registry Server in TLS mode, you need to provide the private and public keys using the `--key` and `--cert` arguments.
More info about TLS mode can be found in [feast-client-connecting-to-remote-registry-sever-started-in-tls-mode](../../how-to-guides/starting-feast-servers-tls-mode.md#starting-feast-registry-server-in-tls-mode)

## How to configure the client

Please see the detail how to configure Remote Registry client [remote.md](../registries/remote.md)

# Registry Server Permissions and Access Control

Please refer the [page](./../registry/registry-permissions.md) for more details on API Endpoints and Permissions.

## How to configure Authentication and Authorization ?

Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.