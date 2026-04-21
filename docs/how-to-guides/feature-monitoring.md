# Feature Quality Monitoring

## Overview

Feast's data quality monitoring system computes, stores, and serves statistical metrics for every registered feature. It gives you visibility into feature health — distributions, null rates, percentiles, histograms — across batch data and feature serving logs.

This guide covers:

1. [Prerequisites](#1-prerequisites)
2. [Auto-baseline on registration](#2-auto-baseline-on-registration)
3. [Scheduled monitoring with the CLI](#3-scheduled-monitoring-with-the-cli)
4. [Monitoring feature serving logs](#4-monitoring-feature-serving-logs)
5. [Reading metrics via REST API](#5-reading-metrics-via-rest-api)
6. [On-demand exploration (transient compute)](#6-on-demand-exploration)
7. [Integrating with orchestrators](#7-integrating-with-orchestrators)
8. [Supported backends](#8-supported-backends)

## 1. Prerequisites

Monitoring works with any supported offline store backend. No additional infrastructure or configuration is needed — monitoring tables are created automatically on first use.

**Minimum setup:**

- A Feast project with at least one feature view and a configured offline store
- Feast SDK installed (`pip install feast`)

**For serving log monitoring:**

- At least one feature service with `logging_config` set (see [step 4](#4-monitoring-feature-serving-logs))

## 2. Auto-baseline on registration

When you run `feast apply` to register new features, Feast automatically queues baseline metric computation:

```bash
$ feast apply
Applying changes...
Created feature view 'driver_stats' with 3 features
  → Queued baseline metrics computation (DQM job: abc-123)
Done!
```

The baseline reads all available source data and stores the resulting statistics with `is_baseline=TRUE`. This serves as the reference distribution for future drift detection.

Baseline computation is:
- **Non-blocking** — `feast apply` returns immediately; computation runs asynchronously
- **Idempotent** — only features without existing baselines are computed; re-running `feast apply` won't recompute existing baselines

## 3. Scheduled monitoring with the CLI

### Auto mode (recommended for production)

Schedule a single daily job that computes all granularities automatically:

```bash
feast monitor run
```

This detects the latest event timestamp in the source data and computes metrics for 5 time windows:

| Granularity | Window |
|-------------|--------|
| `daily` | Last 1 day |
| `weekly` | Last 7 days |
| `biweekly` | Last 14 days |
| `monthly` | Last 30 days |
| `quarterly` | Last 90 days |

No date arguments needed. One scheduled job produces all granularities.

### Targeting a specific feature view

```bash
feast monitor run --feature-view driver_stats
```

### Explicit date range and granularity

```bash
feast monitor run \
  --feature-view driver_stats \
  --start-date 2025-01-01 \
  --end-date 2025-01-07 \
  --granularity weekly
```

### Setting a manual baseline

```bash
feast monitor run \
  --feature-view driver_stats \
  --start-date 2025-01-01 \
  --end-date 2025-03-31 \
  --granularity daily \
  --set-baseline
```

### CLI reference

```
Usage: feast monitor run [OPTIONS]

Options:
  -p, --project TEXT         Feast project name (defaults to feature_store.yaml)
  -v, --feature-view TEXT    Feature view name (omit for all)
  -f, --feature-name TEXT    Feature name(s), repeatable (omit for all)
  --start-date TEXT          Start date YYYY-MM-DD (omit for auto-detect)
  --end-date TEXT            End date YYYY-MM-DD (omit for auto-detect)
  -g, --granularity          One of: daily, weekly, biweekly, monthly, quarterly
  --set-baseline             Mark this computation as baseline
  --source-type              One of: batch, log, all (default: batch)
  --help                     Show this message and exit.
```

## 4. Monitoring feature serving logs

If your feature services have logging configured, you can compute metrics from the actual features served to models in production.

### Setting up feature service logging

In your feature definitions:

```python
from feast import FeatureService, LoggingConfig
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLLoggingDestination,
)

driver_service = FeatureService(
    name="driver_service",
    features=[driver_stats_fv],
    logging_config=LoggingConfig(
        destination=PostgreSQLLoggingDestination(table_name="feast_driver_logs"),
        sample_rate=1.0,
    ),
)
```

### Computing log metrics

**Auto mode (all feature services with logging):**

```bash
feast monitor run --source-type log
```

**Specific feature service:**

```bash
feast monitor run --source-type log --feature-view driver_service
```

**Both batch and log in one run:**

```bash
feast monitor run --source-type all
```

Log metrics are stored with `data_source_type="log"` alongside batch metrics in the same monitoring tables. Feature names from the log schema (e.g., `driver_stats__conv_rate`) are automatically normalized back to their original names (`conv_rate`) and associated with the correct feature view — enabling batch-vs-log comparison and drift detection.

### Via REST API

```bash
# Compute log metrics
POST /monitoring/compute/log
{
  "project": "my_project",
  "feature_service_name": "driver_service",
  "granularity": "daily"
}

# Auto-compute all log metrics
POST /monitoring/auto_compute/log
{
  "project": "my_project"
}
```

## 5. Reading metrics via REST API

All read endpoints support cascading filters: `project` → `feature_service_name` → `feature_view_name` → `feature_name` → `granularity` → `data_source_type`.

### Per-feature metrics

```
GET /monitoring/metrics/features?project=my_project&feature_view_name=driver_stats&granularity=daily
```

**Response:**

```json
[
  {
    "project_id": "my_project",
    "feature_view_name": "driver_stats",
    "feature_name": "conv_rate",
    "feature_type": "numeric",
    "metric_date": "2025-03-26",
    "granularity": "daily",
    "data_source_type": "batch",
    "row_count": 15000,
    "null_count": 12,
    "null_rate": 0.0008,
    "mean": 0.523,
    "stddev": 0.189,
    "min_val": 0.001,
    "max_val": 0.998,
    "p50": 0.51,
    "p75": 0.68,
    "p90": 0.82,
    "p95": 0.89,
    "p99": 0.96,
    "histogram": {
      "bins": [0.0, 0.05, 0.1, "..."],
      "counts": [120, 340, 560, "..."],
      "bin_width": 0.05
    }
  }
]
```

### Per-feature-view aggregates

```
GET /monitoring/metrics/feature_views?project=my_project&feature_view_name=driver_stats
```

### Per-feature-service aggregates

```
GET /monitoring/metrics/feature_services?project=my_project&feature_service_name=driver_service
```

### Baseline

```
GET /monitoring/metrics/baseline?project=my_project&feature_view_name=driver_stats
```

### Time-series (for trend charts)

```
GET /monitoring/metrics/timeseries?project=my_project&feature_name=conv_rate&granularity=daily&start_date=2025-01-01&end_date=2025-03-31
```

### Filtering batch vs. log metrics

Add `data_source_type=batch` or `data_source_type=log` to any read endpoint:

```
GET /monitoring/metrics/features?project=my_project&data_source_type=log
```

### Full endpoint reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/monitoring/compute` | Submit batch DQM job |
| `POST` | `/monitoring/auto_compute` | Auto-detect dates, all granularities |
| `POST` | `/monitoring/compute/transient` | On-demand compute (not stored) |
| `POST` | `/monitoring/compute/log` | Compute from serving logs |
| `POST` | `/monitoring/auto_compute/log` | Auto-detect log dates, all granularities |
| `GET` | `/monitoring/jobs/{job_id}` | DQM job status |
| `GET` | `/monitoring/metrics/features` | Per-feature metrics |
| `GET` | `/monitoring/metrics/feature_views` | Per-view aggregates |
| `GET` | `/monitoring/metrics/feature_services` | Per-service aggregates |
| `GET` | `/monitoring/metrics/baseline` | Baseline metrics |
| `GET` | `/monitoring/metrics/timeseries` | Time-series data |

## 6. On-demand exploration

When you need metrics for an arbitrary date range (e.g., "show me the distribution for Jan 5 to Jan 20"), use the transient compute endpoint. It reads source data for the exact range, computes fresh statistics, and returns them directly without storing.

```bash
POST /monitoring/compute/transient
{
  "project": "my_project",
  "feature_view_name": "driver_stats",
  "feature_names": ["conv_rate"],
  "start_date": "2025-01-05",
  "end_date": "2025-01-20"
}
```

This is necessary because pre-computed histograms from different date ranges have different bin edges and cannot be merged losslessly.

## 7. Integrating with orchestrators

### Airflow

```python
from airflow.operators.bash import BashOperator

monitor_task = BashOperator(
    task_id="feast_monitor",
    bash_command="feast monitor run",
    cwd="/path/to/feast/repo",
)
```

### Kubeflow Pipelines (KFP)

```python
from kfp import dsl

@dsl.component(base_image="feast-image:latest")
def monitor_features():
    import subprocess
    subprocess.run(["feast", "monitor", "run"], check=True, cwd="/feast/repo")
```

### Cron

```cron
# Daily at 2:00 AM UTC
0 2 * * * cd /path/to/feast/repo && feast monitor run >> /var/log/feast-monitor.log 2>&1
```

### Monitoring both batch and log in one job

```bash
feast monitor run --source-type all
```

## 8. Supported backends

Monitoring works natively with all offline stores that serve as compute engines for Feast materialization:

| Backend | Compute | Storage |
|---------|---------|---------|
| PostgreSQL | SQL push-down | `INSERT ON CONFLICT` |
| Snowflake | SQL push-down | `MERGE` with `VARIANT` JSON |
| BigQuery | SQL push-down | `MERGE` into BQ tables |
| Redshift | SQL push-down | `MERGE` via Data API |
| Spark | SparkSQL push-down | Parquet tables |
| Oracle | SQL via Ibis | `MERGE` from `DUAL` |
| DuckDB | In-memory SQL | Parquet files |
| Dask | PyArrow compute | Parquet files |

Backends not listed above fall back to Python-based computation — the offline store's `pull_all_from_table_or_query()` returns a PyArrow Table, and metrics are computed using `pyarrow.compute` and `numpy`.

## What metrics are computed

**Per-feature (full profile):**

| Metric | Numeric | Categorical |
|--------|:-------:|:-----------:|
| row_count, null_count, null_rate | Yes | Yes |
| mean, stddev, min, max | Yes | — |
| p50, p75, p90, p95, p99 | Yes | — |
| histogram (JSONB) | Binned distribution | Top-N values with counts |

**Per-feature-view and per-feature-service (aggregate summaries):**

| Metric | Description |
|--------|-------------|
| total_row_count | Total rows in the view |
| total_features | Number of features |
| features_with_nulls | Count of features with any nulls |
| avg_null_rate, max_null_rate | Aggregate null rate statistics |

## RBAC

Monitoring respects Feast's existing RBAC:

- **Compute operations** (`POST /monitoring/compute`, `/auto_compute`, `/compute/log`, `/auto_compute/log`) require `AuthzedAction.UPDATE`
- **Transient compute** (`POST /monitoring/compute/transient`) requires `AuthzedAction.DESCRIBE`
- **Read operations** (`GET /monitoring/metrics/*`) require `AuthzedAction.DESCRIBE`
