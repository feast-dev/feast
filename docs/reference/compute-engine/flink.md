# Apache Flink

## Description

The Apache Flink compute engine provides a distributed execution engine for
feature pipelines through the PyFlink Table API. It implements Feast's unified
`ComputeEngine` interface and can be used for batch materialization operations
(`materialize` and `materialize-incremental`) and historical retrieval
(`get_historical_features`).

The engine reads data through the configured Feast offline store and executes
the Feast DAG as PyFlink tables. Offline stores that expose a native
`to_flink_table(table_env)` retrieval job hand Flink tables directly to the
engine. Retrieval jobs that only expose the standard Arrow path are also
supported and are converted into Flink tables by the engine. The engine then
uses Flink Table/SQL operations for join, filter, aggregate, dedupe, and
projection steps, and writes materialization results to the configured online
and/or offline store.

## Configuration

Install the Flink extra from a Feast source checkout with `uv` before using the
engine:

```bash
uv sync --extra flink --no-dev
```

The `flink` extra installs PyFlink directly. PyFlink currently requires
`pyarrow<21`, while the default Feast install keeps `pyarrow>=21`; Feast's uv
lock resolves the Flink extra in a separate dependency fork so normal Feast
installs do not downgrade Arrow.

Configure the engine in `feature_store.yaml`:

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: file
online_store:
  type: sqlite
  path: data/online_store.db
batch_engine:
  type: flink.engine
  execution_mode: batch
  parallelism: 4
  table_config:
    pipeline.name: "Feast Flink Compute Engine"
  pandas_split_num: 4
```

## Configuration Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `type` | string | `flink.engine` | Must be `flink.engine`. |
| `execution_mode` | string | `batch` | PyFlink execution mode: `batch` or `streaming`. |
| `parallelism` | integer | `null` | Default Flink parallelism for jobs created by the engine. |
| `table_config` | map | `null` | Additional PyFlink table configuration entries. |
| `pandas_split_num` | integer | `1` | Number of PyFlink Arrow source splits when converting pandas entity DataFrames into Flink tables. |

## Flink Transformations

Use `mode="flink"` when a `BatchFeatureView` transformation should receive and
return PyFlink table objects:

```python
from feast import BatchFeatureView, Field
from feast.types import Float32


def double_rates(table):
    # In production this can use PyFlink Table API operations and return a table.
    return table


driver_stats = BatchFeatureView(
    name="driver_stats",
    entities=[driver],
    mode="flink",
    udf=double_rates,
    schema=[Field(name="conv_rate", dtype=Float32)],
    source=driver_stats_source,
    online=True,
)
```

Flink transformations must return PyFlink table objects. pandas-returning UDFs
are not accepted by the Flink compute engine.

## DAG Support

The Flink engine implements Feast's compute DAG with Flink-specific nodes:

- Source reads from Feast offline stores, preferring native Flink tables when a
  retrieval job supports `to_flink_table(table_env)` and otherwise converting
  Arrow results into Flink tables.
- Transform nodes pass PyFlink tables to `mode="flink"` UDFs and preserve native
  Flink table outputs.
- Join nodes use Flink SQL temporary views for feature joins and entity joins.
- Filter nodes apply point-in-time, TTL, and custom filter expressions in Flink
  SQL.
- Aggregate nodes support non-windowed Feast aggregations using Flink SQL
  aggregate functions.
- Dedupe nodes use `ROW_NUMBER()` over entity keys or internal entity-row ids so
  historical retrieval keeps one latest feature row per entity row.
- Validation nodes check required output columns. JSON value validation must be
  handled upstream in Flink SQL.
- Output nodes write only for materialization tasks; historical retrieval is
  read-only.
- Historical retrieval accepts pandas entity DataFrames and SQL-string entity
  DataFrames. SQL strings are interpreted as Flink SQL queries against the
  configured TableEnvironment/catalog and must select an `event_timestamp`
  column.

## Running on Kubernetes with the Flink Operator (`flink_application`)

The `flink.engine` above runs PyFlink inside the Feast process. To run the same
materialization **on Kubernetes**, the `flink_application` engine submits a
[`FlinkDeployment`](https://github.com/apache/flink-kubernetes-operator) custom
resource to the Apache Flink Kubernetes Operator for each materialize call,
rather than running PyFlink locally. Confluent Platform for Apache Flink ships a
distribution of the same operator, so this works against both.

How it works:

1. Feast writes a ConfigMap with the task list and a copy of `feature_store.yaml`
   (with `batch_engine` rewritten to `flink.engine`).
2. Feast creates a `FlinkDeployment` that runs `/opt/feast/main.py` as a PyFlink
   application (via the operator's `PythonDriver`) in `BATCH` mode.
3. The pod runs the in-process `flink.engine` on the operator-provisioned
   JobManager/TaskManager, reads the offline store, and writes the online store
   and registry.
4. Feast polls the `FlinkDeployment` to completion.

This mode supports `materialize` / `materialize-incremental` only (one
`FlinkDeployment` per call, multiple feature views batched);
`get_historical_features` is not yet supported.

### Requirements

- Apache Flink Kubernetes Operator installed and watching the target namespace.
- A container image with the Feast SDK, PyFlink, and clients for your stores (see
  the reference [Dockerfile](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/compute_engines/flink_application/Dockerfile)).
  The base Flink image, the `apache-flink` wheel from the `flink` extra (pinned
  `>=2.2.1,<3`), and `flink_version` / `python_jar_uri` must all reference the
  same Flink release (default **2.2**). `pemja` ships only linux/amd64 wheels —
  build for amd64.
- A service account for the Flink pods (`service_account`, default `flink`) with
  the standard Flink-operator RBAC, including `deployments/finalizers` (the
  JobManager sets owner references when creating TaskManager pods).
- **Network-accessible** online, offline, and registry stores. File-based
  backends (`sqlite`/`faiss` online, `dask`/`file`/`duckdb` offline, `file`
  registry) are rejected because Flink pods have an ephemeral filesystem.

### Example

```yaml
batch_engine:
  type: flink_application
  image: my-registry.example.com/feast-flink:latest
  namespace: feast
  service_account: flink
  flink_version: v2_2
  python_jar_uri: local:///opt/flink/opt/flink-python-2.2.1.jar
  parallelism: 1
  jobmanager_memory: "2048m"
  taskmanager_memory: "2048m"
```

### Configuration Options

In addition to the shared operator fields (`image_pull_secrets`, `env`,
`env_from`, `queue_name`, `job_timeout_seconds`, `poll_interval_seconds`,
`ttl_seconds_after_finished`, `max_retries`, `concurrency`, `labels`, `volumes`,
`volume_mounts`, `node_selector`, `tolerations`, `staging_location`):

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `type` | string | `flink_application` | Must be `flink_application`. |
| `image` | string | **required** | Container image for the JobManager/TaskManager. |
| `namespace` | string | `default` | Namespace for the `FlinkDeployment` and ConfigMap. |
| `service_account` | string | `flink` | Service account for the Flink pods. |
| `flink_version` | string | `v2_2` | `FlinkDeployment` `flinkVersion` enum; must match the image. |
| `python_jar_uri` | string | `local:///opt/flink/opt/flink-python-2.2.1.jar` | PythonDriver jar; version must match `flink_version`. |
| `parallelism` | integer | `1` | Default Flink job parallelism. |
| `task_slots` | integer | `1` | `taskmanager.numberOfTaskSlots`. |
| `jobmanager_cpu` / `jobmanager_memory` | float / string | `1.0` / `2048m` | JobManager resources. |
| `taskmanager_cpu` / `taskmanager_memory` | float / string | `1.0` / `2048m` | TaskManager resources. |
| `upgrade_mode` | string | `stateless` | `FlinkDeployment` job `upgradeMode`. |
| `flink_configuration` | map | `{}` | Extra entries merged into `flinkConfiguration`. |

## Current Limitations

- Windowed aggregations are not yet implemented in the Flink compute engine. Use
  non-windowed Feast aggregations or pre-window upstream in Flink.
- JSON value validation is not implemented inside the Flink compute engine
  because the engine does not collect intermediate data out of Flink for
  validation.
