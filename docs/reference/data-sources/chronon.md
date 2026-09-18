# Chronon source (contrib)

## Description

Chronon sources describe feature data produced by [Chronon](https://chronon.ai/) and consumed by Feast.
They point Feast at Chronon's offline materialization output and, when online reads are needed, identify the Chronon Join or GroupBy that should be queried from Chronon's online service.

Feast does not compute or materialize Chronon features. Chronon owns feature computation, backfills, consistency, and online serving. Feast uses the source metadata for registry, discovery, historical retrieval, and online lookup.

## Examples

Defining a Chronon source for a Chronon Join:

```python
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)

driver_stats_source = ChrononSource(
    name="driver_stats",
    materialization_path="data/chronon/driver_stats",
    chronon_join="team/driver_stats.v1",
    online_endpoint="http://localhost:8080",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)
```

Defining a Chronon source for a Chronon GroupBy:

```python
driver_profile_source = ChrononSource(
    name="driver_profile",
    materialization_path="data/chronon/driver_profile",
    chronon_group_by="team/driver_profile.v1",
    timestamp_field="event_timestamp",
)
```

## Configuration reference

| Parameter                  | Required | Description |
| :------------------------- | :------- | :---------- |
| `materialization_path`     | yes      | Local or repository-relative path to Chronon's Parquet materialization output. |
| `chronon_join`             | no       | Chronon Join name used for online reads, for example `team/training_set.v1`. |
| `chronon_group_by`         | no       | Chronon GroupBy name used for online reads, for example `team/user_features.v1`. |
| `online_endpoint`          | no       | Chronon online service base URL for this source. If omitted, Feast uses the Chronon online store `path`. |
| `timestamp_field`          | yes      | Event timestamp column in the materialized Chronon data. |
| `created_timestamp_column` | no       | Optional created timestamp column used to select the latest row when duplicate event timestamps exist. |
| `field_mapping`            | no       | Standard Feast field mapping applied before retrieval. |

Set at most one of `chronon_join` and `chronon_group_by`. Offline-only sources may omit both, but `online_endpoint` requires one of them so Feast can build the Chronon request URL.

## Supported Types

Chronon sources read Parquet data through PyArrow and use Feast's standard PyArrow type mapping.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
