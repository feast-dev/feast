# Chronon offline store (contrib)

## Description

The Chronon offline store provides support for reading [ChrononSources](../data-sources/chronon.md) from Chronon's Parquet materialization output.

Chronon remains the system of record for feature computation and materialization. Feast reads Chronon-produced data for historical retrieval, feature reuse, and registry-driven training workflows.

## Getting started

Chronon-backed feature repos use the Chronon offline store with Chronon sources:

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: chronon
offline_store:
  type: chronon
online_store:
  type: chronon
  path: http://localhost:8080
```
{% endcode %}

Example feature view:

```python
from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)
from feast.types import Float32

driver = Entity(name="driver", join_keys=["driver_id"])

driver_stats = FeatureView(
    name="driver_stats",
    entities=[driver],
    schema=[Field(name="rating", dtype=Float32)],
    source=ChrononSource(
        name="driver_stats_source",
        materialization_path="data/chronon/driver_stats",
        chronon_join="team/driver_stats.v1",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    ),
)
```

## Historical retrieval

`get_historical_features` performs point-in-time joins against Chronon's materialized Parquet data. For each entity row, Feast selects the latest Chronon row with an event timestamp at or before the entity timestamp. If `created_timestamp_column` is configured and duplicate event timestamps exist, the latest created row wins.

This is intended for training and validation workflows that want Feast's registry and retrieval APIs while using Chronon as the feature computation engine.

## Configuration reference

| Parameter | Required | Default | Description |
| :-------- | :------- | :------ | :---------- |
| `type`    | yes      | -       | Must be set to `chronon`. |
| `path`    | no       | -       | Reserved for future offline store configuration. Source-level `materialization_path` controls where data is read from. |

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Chronon offline store.

|                                                                    | Chronon |
| :----------------------------------------------------------------- | :------ |
| `get_historical_features` (point-in-time correct join)             | yes     |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes     |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes     |
| `offline_write_batch` (persist dataframes to offline store)        | no      |
| `write_logged_features` (persist logged features to offline store) | no      |

Below is a matrix indicating which functionality is supported by `ChrononRetrievalJob`.

|                                                       | Chronon |
| ----------------------------------------------------- | ------- |
| export to dataframe                                   | yes     |
| export to arrow table                                 | yes     |
| export to arrow batches                               | no      |
| export to SQL                                         | no      |
| export to data lake (S3, GCS, etc.)                   | no      |
| export to data warehouse                              | no      |
| export as Spark dataframe                             | no      |
| local execution of Python-based on-demand transforms  | no      |
| remote execution of Python-based on-demand transforms | no      |
| persist results in the offline store                  | no      |
| preview the query plan before execution               | no      |
| read partitioned data                                 | yes     |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
