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

## Current Limitations

- Windowed aggregations are not yet implemented in the Flink compute engine. Use
  non-windowed Feast aggregations or pre-window upstream in Flink.
- JSON value validation is not implemented inside the Flink compute engine
  because the engine does not collect intermediate data out of Flink for
  validation.
