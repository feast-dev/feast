# Go feature server

## Overview

The Go feature server is an HTTP/gRPC endpoint that serves features.
It is written in Go, and is therefore significantly faster than the Python feature server.
See this [blog post](https://feast.dev/blog/go-feature-server-benchmarks/) for more details on the comparison between Python and Go.
In general, we recommend the Go feature server for all production use cases that require extremely low-latency feature serving.
Currently only the Redis and SQLite online stores are supported.

## CLI

By default, the Go feature server is turned off.
To turn it on you can add `go_feature_serving: True` to your `feature_store.yaml`:

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
go_feature_serving: True
```
{% endcode %}

Then the `feast serve` CLI command will start the Go feature server.
As with Python, the Go feature server uses port 6566 by default; the port be overridden with a `--port` flag.
Moreover, the server uses HTTP by default, but can be set to use gRPC with `--type=grpc`.

Alternatively, if you wish to experiment with the Go feature server instead of permanently turning it on, you can just run `feast serve --go`.

## Installation

The Go component comes pre-compiled when you install Feast with Python versions 3.8-3.10 on macOS or Linux (on x86).
In order to install the additional Python dependencies, you should install Feast with
```
pip install feast[go]
```
You must also install the Apache Arrow C++ libraries.
This is because the Go feature server uses the cgo memory allocator from the Apache Arrow C++ library for interoperability between Go and Python, to prevent memory from being accidentally garbage collected when executing on-demand feature views.
You can read more about the usage of the cgo memory allocator in these [docs](https://pkg.go.dev/github.com/apache/arrow/go/arrow@v0.0.0-20211112161151-bc219186db40/cdata#ExportArrowRecordBatch).

For macOS, run `brew install apache-arrow`.
For linux users, you have to install `libarrow-dev`.
```
sudo apt update
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev # For C++
```
For developers, if you want to build from source, run `make compile-go-lib` to build and compile the go server. In order to build the go binaries, you will need to install the `apache-arrow` c++ libraries.

## Alpha features

### Feature logging

The Go feature server can log all requested entities and served features to a configured destination inside an offline store.
This allows users to create new datasets from features served online. Those datasets could be used for future trainings or for
feature validations. To enable feature logging we need to edit `feature_store.yaml`:
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
go_feature_serving: True
feature_server:
  feature_logging:
    enabled: True
```

Feature logging configuration in `feature_store.yaml` also allows to tweak some low-level parameters to achieve the best performance:
```yaml
feature_server:
  feature_logging:
    enabled: True
    flush_interval_secs: 300
    write_to_disk_interval_secs: 30
    emit_timeout_micro_secs: 10000
    queue_capacity: 10000
```
All these parameters are optional.

### Python SDK retrieval

The logic for the Go feature server can also be used to retrieve features during a Python `get_online_features` call.
To enable this behavior, you must add `go_feature_retrieval: True` to your `feature_store.yaml`.
You must also have all the dependencies installed as detailed above.
