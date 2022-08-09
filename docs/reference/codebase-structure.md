# Codebase structure

Let's examine the Feast codebase.

```
$ tree -L 1 -d
.
├── docs
├── examples
├── go
├── infra
├── java
├── protos
├── sdk
└── ui
```

## Python SDK

The Python SDK lives in `sdk/python/feast`.

```
$ cd sdk/python/feast
$ tree -L 1 
.
├── __init__.py
├── aggregation.py
├── base_feature_view.py
├── batch_feature_view.py
├── cli.py
├── constants.py
├── data_format.py
├── data_source.py
├── diff
├── dqm
├── driver_test_data.py
├── embedded_go
├── entity.py
├── errors.py
├── feast_object.py
├── feature.py
├── feature_logging.py
├── feature_server.py
├── feature_service.py
├── feature_store.py
├── feature_view.py
├── feature_view_projection.py
├── field.py
├── flags_helper.py
├── importer.py
├── inference.py
├── infra
├── loaders
├── names.py
├── on_demand_feature_view.py
├── online_response.py
├── project_metadata.py
├── proto_json.py
├── py.typed
├── registry.py
├── registry_store.py
├── repo_config.py
├── repo_contents.py
├── repo_operations.py
├── repo_upgrade.py
├── request_feature_view.py
├── saved_dataset.py
├── stream_feature_view.py
├── templates
├── transformation_server.py
├── type_map.py
├── types.py
├── ui
├── ui_server.py
├── usage.py
├── utils.py
├── value_type.py
├── version.py
└── wait.py
```

The majority of Feast logic lives in these Python files:
* The core Feast objects ([entities](../getting-started/concepts/entity.md), [feature views](../getting-started/concepts/feature-view.md), [data sources](../getting-started/concepts/dataset.md), etc.) are defined in their respective Python files, such as `entity.py`, `feature_view.py`, and `data_source.py`.
* The feature store object is defined in `feature_store.py` and the associated configuration objects are defined in `repo_config.py`.
* The registry is defined in `registry.py`.
* The CLI and other core feature store logic are defined in `cli.py` and `repo_operations.py`.
* The type system that is used to manage conversion between Feast types and external typing systems is managed in `type_map.py`.
* The Python feature server is defined in `feature_server.py`.

There are also several important submodules:
* `infra/` contains all the infrastructure components, such as offline and online stores.
* `dqm/` covers data quality monitoring.
* `diff/` covers the logic for creating plans.
* `embedded_go/` covers the Go feature server.
* `ui/` contains the embedded Web UI, to be launched on the `feast ui` command.

Of these submodules, `infra/` is the most important.
It contains the interfaces for the [provider](getting-started/architecture-and-components/provider.md), [offline store](getting-started/architecture-and-components/offline-store.md), [online store](getting-started/architecture-and-components/online-store.md), and [batch materialization engine](getting-started/architecture-and-components/batch-materialization-engine.md), as well as all of their individual implementations.

```
$ tree -L 1 infra
.
├── __init__.py
├── __pycache__
├── aws.py
├── contrib
├── feature_servers
├── gcp.py
├── infra_object.py
├── key_encoding_utils.py
├── local.py
├── materialization
├── offline_stores
├── online_stores
├── passthrough_provider.py
├── provider.py
├── registry_stores
├── transformation_servers
└── utils
```

The tests for the Python SDK are contained in `sdk/python/tests`.
For more details, see this [overview](../how-to-guides/adding-or-reusing-tests.md#test-suite-overview) of the test suite.

## Java SDK

The `java/` directory contains the Java serving component.
See [here](https://github.com/feast-dev/feast/blob/master/java/CONTRIBUTING.md) for more details on how the repo is structured.

## Go feature server

The `go/` directory contains the Go feature server.
Within `go/`, the `internal/feast/` directory contains most of the core logic:
* `onlineserving/` covers the core serving logic.
* `model/` contains the implementations of the Feast objects (entity, feature view, etc.).
* `registry/` covers the registry.
* `onlinestore/` covers the online stores (currently only Redis and SQLite are supported).

## Protobufs

Feast uses [protobuf](https://github.com/protocolbuffers/protobuf) to store serialized versions of the core Feast objects.
The protobuf definitions are stored in `protos/feast`.

## Web UI

The `ui/` directory contains the Web UI.
See [here](https://github.com/feast-dev/feast/blob/master/ui/CONTRIBUTING.md) for more details on the structure of the Web UI.
