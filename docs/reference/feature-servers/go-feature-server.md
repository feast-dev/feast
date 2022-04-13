# Go feature server

## Overview

The Go feature server is a Go implementation of the core feature serving logic, embedded in the Python SDK. It supports retrieval of feature references, feature services, and on demand feature views, and can be used either through the Python SDK or the [Python feature server](local-feature-server.md).

The Go feature server currently only supports Redis as an online store; support for other online stores will be added soon. We also plan on adding support for the Java feature server (e.g. the capability to call into the Go feature server and execute Java UDFs). Initial benchmarks indicate that the Go feature server is significantly faster than the Python feature server. We plan to release a more comprehensive set of benchmarks. For more details, see the [RFC](https://docs.google.com/document/d/1Lgqv6eWYFJgQ7LA_jNeTh8NzOPhqI9kGTeyESRpNHnE).

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
