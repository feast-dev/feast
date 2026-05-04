# feature_store.yaml

## Overview

`feature_store.yaml` is used to configure a feature store. The file must be located at the root of a [feature repository](./). An example `feature_store.yaml` is shown below:

{% code title="feature_store.yaml" %}
```yaml
project: loyal_spider
registry: data/registry.db
provider: local
online_store:
    type: sqlite
    path: data/online_store.db
```
{% endcode %}

## Options

The following top-level configuration options exist in the `feature_store.yaml` file.

* **provider**  — Configures the environment in which Feast will deploy and operate.
* **registry** — Configures the location of the feature registry.
* **online_store** — Configures the online store.
* **offline_store** — Configures the offline store.
* **project** — Defines a namespace for the entire feature store. Can be used to isolate multiple deployments in a single installation of Feast. Should only contain letters, numbers, and underscores.
* **engine** - Configures the batch materialization engine.
* **materialization** - Configures materialization behavior (write batching, feature pull strategy). See below.

Please see the [RepoConfig](https://rtd.feast.dev/en/latest/#feast.repo_config.RepoConfig) API reference for the full list of configuration options.

---

## `materialization` configuration

The `materialization` block controls how Feast reads from the offline store and writes to the online store during `feast materialize` / `feast materialize-incremental` runs.

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
materialization:
  online_write_batch_size: 10000   # write rows in chunks of 10 000
  pull_latest_features: false      # pull full time range (default)
```
{% endcode %}

### `online_write_batch_size`

| Field | Type | Default | Supported engines |
| --- | --- | --- | --- |
| `online_write_batch_size` | `int` (positive) | `null` | local, spark, ray |

Controls how many rows are converted to protobuf and written to the online store per batch during materialization.

**Default behaviour (`null`):** All rows fetched from the offline store are converted to protobuf in a single in-memory operation before writing. This is fast but can exhaust memory for large datasets — every row must be held as a Python proto object simultaneously.

**With `online_write_batch_size` set:** The Arrow table returned by the offline store is split into chunks of at most `online_write_batch_size` rows. Each chunk is converted and written independently, keeping peak memory proportional to the batch size rather than the full dataset size.

```yaml
# Recommended for datasets > a few million rows or memory-constrained workers
materialization:
  online_write_batch_size: 10000
```

**Choosing a value:**

| Dataset size | Worker memory | Recommended batch size |
| --- | --- | --- |
| < 1 M rows | Any | `null` (default — single batch is fine) |
| 1–10 M rows | ≥ 4 GB | `50000` |
| 10–100 M rows | ≥ 8 GB | `10000` |
| > 100 M rows | Any | `5000`–`10000` |

A smaller batch size reduces peak memory at the cost of more `online_write_batch` calls to the online store. For Redis, each call is a pipelined batch, so the overhead is low. For stores with higher per-call latency (e.g. DynamoDB), prefer larger batch sizes.

{% hint style="info" %}
`online_write_batch_size` is applied **per feature view** within a single materialization job. If you materialize five feature views in parallel, peak memory is `5 × batch_size × bytes_per_row`.
{% endhint %}

### `pull_latest_features`

| Field | Type | Default |
| --- | --- | --- |
| `pull_latest_features` | `bool` | `false` |

When `false` (default), the offline store retrieves **all** feature values within the requested time range for each entity.

When `true`, only the **latest** value per entity is retrieved. This reduces I/O and memory for feature views where historical values are not needed (e.g., slowly changing dimensions). It is equivalent to running a `GROUP BY entity, MAX(event_timestamp)` on the offline data before writing.
