# Go-based Feature Retrieval

## Overview

The Go Feature Retrieval component is a Go implementation of the core feature serving logic, embedded in the Python SDK. It supports retrieval of feature references, feature services, and on demand feature views, and can be used either through the Python SDK or the [Python feature server](python-feature-server.md).

Currently, this component only supports online serving and does not have an offline component including APIs to create feast feature repositories or apply configuration to the registry to facilitate online materialization. It also does not expose its own dedicated cli to perform feast actions. Furthermore, this component is only meant to expose an online serving API that can be called through the python SDK to facilitate faster online feature retrieval.

The Go Feature Retrieval component currently only supports Redis and Sqlite as online stores; support for other online stores will be added soon. Initial benchmarks indicate that it is significantly faster than the Python feature server for online feature retrieval. We plan to release a more comprehensive set of benchmarks. For more details, see the [RFC](https://docs.google.com/document/d/1Lgqv6eWYFJgQ7LA_jNeTh8NzOPhqI9kGTeyESRpNHnE).

## Installation

As long as you are running macOS or linux, on x86, with python version 3.7-3.10, the go component comes pre-compiled when you install feast.

However, some additional dependencies are required for Go <-> Python interoperability. To install these dependencies run the following command in your console:
```
pip install feast[go]
```
You will also have to install the apache-arrow c++ libraries, since we use the cgo memory allocator to prevent memory from being incorrectly garbage collected, detailed in these [docs](https://pkg.go.dev/github.com/apache/arrow/go/arrow@v0.0.0-20211112161151-bc219186db40/cdata#ExportArrowRecordBatch).

For macos, run `brew install apache-arrow`.
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

## Usage

To enable the Go online feature retrieval component, set `go_feature_retrieval: True` in your `feature_store.yaml`. This will direct all online feature retrieval to Go instead of Python. This flag will be enabled by default in the future.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
go_feature_retrieval: True
```
{% endcode %}

## Feature logging

Go feature server can log all requested entities and served features to a configured destination inside an offline store.
This allows users to create new datasets from features served online. Those datasets could be used for future trainings or for
feature validations. To enable feature logging we need to edit `feature_store.yaml`:
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
go_feature_retrieval: True
feature_server:
  feature_logging:
    enable: True
```

Feature logging configuration in `feature_store.yaml` also allows to tweak some low-level parameters to achieve the best performance:
```yaml
feature_server:
  feature_logging:
    enable: True
    flush_interval_secs: 300
    write_to_disk_interval_secs: 30
    emit_timeout_micro_secs: 10000
    queue_capacity: 10000
```
All these parameters are optional.

## Future/Current Work

The Go feature retrieval online feature logging for Data Quality Monitoring is currently in development. More information can be found [here](https://docs.google.com/document/d/110F72d4NTv80p35wDSONxhhPBqWRwbZXG4f9mNEMd98/edit#heading=h.9gaqqtox9jg6).

We also plan on adding support for the Java feature server (e.g. the capability to call into the Go component and execute Java UDFs).

