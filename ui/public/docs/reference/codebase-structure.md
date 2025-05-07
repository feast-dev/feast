# Codebase structure

Let's examine the Feast codebase.
This analysis is accurate as of Feast 0.23.

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
The majority of Feast logic lives in these Python files:
* The core Feast objects ([entities](../getting-started/concepts/entity.md), [feature views](../getting-started/concepts/feature-view.md), [data sources](../getting-started/concepts/dataset.md), etc.) are defined in their respective Python files, such as `entity.py`, `feature_view.py`, and `data_source.py`.
* The `FeatureStore` class is defined in `feature_store.py` and the associated configuration object (the Python representation of the `feature_store.yaml` file) are defined in `repo_config.py`.
* The CLI and other core feature store logic are defined in `cli.py` and `repo_operations.py`.
* The type system that is used to manage conversion between Feast types and external typing systems is managed in `type_map.py`.
* The Python feature server (the server that is started through the `feast serve` command) is defined in `feature_server.py`.

There are also several important submodules:
* `infra/` contains all the infrastructure components, such as the provider, offline store, online store, batch materialization engine, and registry.
* `dqm/` covers data quality monitoring, such as the dataset profiler.
* `diff/` covers the logic for determining how to apply infrastructure changes upon feature repo changes (e.g. the output of `feast plan` and `feast apply`).
* `embedded_go/` covers the Go feature server.
* `ui/` contains the embedded Web UI, to be launched on the `feast ui` command.

Of these submodules, `infra/` is the most important.
It contains the interfaces for the [provider](getting-started/components/provider.md), [offline store](getting-started/components/offline-store.md), [online store](getting-started/components/online-store.md), [batch materialization engine](getting-started/components/batch-materialization-engine.md), and [registry](getting-started/components/registry.md), as well as all of their individual implementations.

```
$ tree --dirsfirst -L 1 infra   
infra
├── contrib
├── feature_servers
├── materialization
├── offline_stores
├── online_stores
├── registry
├── transformation_servers
├── utils
├── __init__.py
├── aws.py
├── gcp.py
├── infra_object.py
├── key_encoding_utils.py
├── local.py
├── passthrough_provider.py
└── provider.py
```

The tests for the Python SDK are contained in `sdk/python/tests`.
For more details, see this [overview](../how-to-guides/adding-or-reusing-tests.md#test-suite-overview) of the test suite.

### Example flow: `feast apply`

Let's walk through how `feast apply` works by tracking its execution across the codebase.
   
1. All CLI commands are in `cli.py`.
    Most of these commands are backed by methods in `repo_operations.py`.
    The `feast apply` command triggers `apply_total_command`, which then calls `apply_total` in `repo_operations.py`.
2. With a `FeatureStore` object (from `feature_store.py`) that is initialized based on the `feature_store.yaml` in the current working directory, `apply_total` first parses the feature repo with `parse_repo` and then calls either `FeatureStore.apply` or `FeatureStore._apply_diffs` to apply those changes to the feature store.
3. Let's examine `FeatureStore.apply`.
    It splits the objects based on class (e.g. `Entity`, `FeatureView`, etc.) and then calls the appropriate registry method to apply or delete the object.
    For example, it might call `self._registry.apply_entity` to apply an entity.
    If the default file-based registry is used, this logic can be found in `infra/registry/registry.py`.
4. Then the feature store must update its cloud infrastructure (e.g. online store tables) to match the new feature repo, so it calls `Provider.update_infra`, which can be found in `infra/provider.py`.
5. Assuming the provider is a built-in provider (e.g. one of the local, GCP, or AWS providers), it will call `PassthroughProvider.update_infra` in `infra/passthrough_provider.py`.
6. This delegates to the online store and batch materialization engine.
    For example, if the feature store is configured to use the Redis online store then the `update` method from `infra/online_stores/redis.py` will be called.
    And if the local materialization engine is configured then the `update` method from `infra/materialization/local_engine.py` will be called.

At this point, the `feast apply` command is complete.

### Example flow: `feast materialize`

Let's walk through how `feast materialize` works by tracking its execution across the codebase.

1. The `feast materialize` command triggers `materialize_command` in `cli.py`, which then calls `FeatureStore.materialize` from `feature_store.py`.
2. This then calls `Provider.materialize_single_feature_view`, which can be found in `infra/provider.py`.
3. As with `feast apply`, the provider is most likely backed by the passthrough provider, in which case `PassthroughProvider.materialize_single_feature_view` will be called.
4. This delegates to the underlying batch materialization engine.
    Assuming that the local engine has been configured, `LocalMaterializationEngine.materialize` from `infra/materialization/local_engine.py` will be called.
5. Since materialization involves reading features from the offline store and writing them to the online store, the local engine will delegate to both the offline store and online store.
    Specifically, it will call `OfflineStore.pull_latest_from_table_or_query` and `OnlineStore.online_write_batch`.
    These two calls will be routed to the offline store and online store that have been configured.

### Example flow: `get_historical_features`

Let's walk through how `get_historical_features` works by tracking its execution across the codebase.

1. We start with `FeatureStore.get_historical_features` in `feature_store.py`.
    This method does some internal preparation, and then delegates the actual execution to the underlying provider by calling `Provider.get_historical_features`, which can be found in `infra/provider.py`.
2. As with `feast apply`, the provider is most likely backed by the passthrough provider, in which case `PassthroughProvider.get_historical_features` will be called.
3. That call simply delegates to `OfflineStore.get_historical_features`.
    So if the feature store is configured to use Snowflake as the offline store, `SnowflakeOfflineStore.get_historical_features` will be executed.

## Java SDK

The `java/` directory contains the Java serving component.
See [here](https://github.com/feast-dev/feast/blob/master/java/CONTRIBUTING.md) for more details on how the repo is structured.

## Go feature server

The `go/` directory contains the Go feature server.
Most of the files here have logic to help with reading features from the online store.
Within `go/`, the `internal/feast/` directory contains most of the core logic:
* `onlineserving/` covers the core serving logic.
* `model/` contains the implementations of the Feast objects (entity, feature view, etc.).
  * For example, `entity.go` is the Go equivalent of `entity.py`. It contains a very simple Go implementation of the entity object.
* `registry/` covers the registry.
  * Currently only the file-based registry supported (the sql-based registry is unsupported). Additionally, the file-based registry only supports a file-based registry store, not the GCS or S3 registry stores.
* `onlinestore/` covers the online stores (currently only Redis and SQLite are supported).

## Protobufs

Feast uses [protobuf](https://github.com/protocolbuffers/protobuf) to store serialized versions of the core Feast objects.
The protobuf definitions are stored in `protos/feast`.

The [registry](../getting-started/concepts/registry.md) consists of the serialized representations of the Feast objects.

Typically, changes being made to the Feast objects require changes to their corresponding protobuf representations. 
The usual best practices for making changes to protobufs should be followed ensure backwards and forwards compatibility.

## Web UI

The `ui/` directory contains the Web UI.
See [here](https://github.com/feast-dev/feast/blob/master/ui/CONTRIBUTING.md) for more details on the structure of the Web UI.
