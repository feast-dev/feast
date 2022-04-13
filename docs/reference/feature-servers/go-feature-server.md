# Go feature server

## Overview

The Go feature server is a Go implementation of the core feature serving logic, embedded in the Python SDK. It supports retrieval of feature references, feature services, and on demand feature views, and can be used either through the Python SDK or the [Python feature server](local-feature-server.md).

Currently, the Go feature server only supports online serving and does not have an offline component including APIs to create feast feature repositories or apply configuration to the registry to facilitate online materialization. It also does not expose its own dedicated cli to perform feast actions. Currently, the Go feature server is only meant to expose an online serving API that can be called through the python SDK to facilitate faster online feature retrieval.

The Go feature server currently only supports Redis and Sqlite as online stores; support for other online stores will be added soon. Initial benchmarks indicate that the Go feature server is significantly faster than the Python feature server. We plan to release a more comprehensive set of benchmarks. For more details, see the [RFC](https://docs.google.com/document/d/1Lgqv6eWYFJgQ7LA_jNeTh8NzOPhqI9kGTeyESRpNHnE).

## Usage

To enable the Go feature server, set `go_feature_server: True` in your `feature_store.yaml`.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
go_feature_server: True
```
{% endcode %}

## Future/Current Work

The Go feature server online feature logging for Data Quality Monitoring is currently in development. More information can be found [here](https://docs.google.com/document/d/110F72d4NTv80p35wDSONxhhPBqWRwbZXG4f9mNEMd98/edit#heading=h.9gaqqtox9jg6).

We also plan on adding support for the Java feature server (e.g. the capability to call into the Go feature server and execute Java UDFs).

