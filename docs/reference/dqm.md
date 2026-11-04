# Data Quality Monitoring

Feast's Data Quality Monitoring (DQM) system computes, stores, and serves statistical metrics for every registered feature. It gives you visibility into feature health — distributions, null rates, percentiles, histograms — across batch data and feature serving logs.

Its goal is to address several complex data problems:

* **Data consistency** — new training datasets can differ significantly from previous datasets, potentially requiring changes in model architecture.
* **Upstream pipeline bugs** — bugs in upstream pipelines can cause invalid values to overwrite existing valid values in an online store.
* **Training/serving skew** — distribution shift between training and serving data can decrease model performance.

### Overview

Feast's DQM system works natively with your configured offline store — no additional infrastructure or external dependencies are required. The workflow is:

1. **Register features** — run `feast apply` to register feature views. If `auto_baseline: true` is configured, baseline metrics are computed automatically.
2. **Schedule monitoring** — run `feast monitor run` on a schedule (daily recommended) to compute metrics across multiple time windows.
3. **Read metrics** — query metrics via the REST API or view them in the Feast UI.

### Configuration

Enable DQM in your `feature_store.yaml`:

```yaml
data_quality_monitoring:
  auto_baseline: true
```

### Computing Metrics

**Auto mode (recommended for production):**

```bash
feast monitor run
```

This detects the latest event timestamp in the source data and computes metrics for 5 time windows: daily, weekly, biweekly, monthly, and quarterly.

**Target a specific feature view:**

```bash
feast monitor run --feature-view driver_stats
```

**Explicit date range:**

```bash
feast monitor run \
  --feature-view driver_stats \
  --start-date 2025-01-01 \
  --end-date 2025-01-07 \
  --granularity weekly
```

**Set a manual baseline:**

```bash
feast monitor run \
  --feature-view driver_stats \
  --start-date 2025-01-01 \
  --end-date 2025-03-31 \
  --granularity daily \
  --set-baseline
```

### Monitoring Feature Serving Logs

If your feature services have logging configured, you can compute metrics from the actual features served to models in production:

```bash
feast monitor run --source-type log
```

### Reading Metrics

Metrics are accessible via the REST API:

```
GET /monitoring/metrics/features?project=my_project&feature_view_name=driver_stats&granularity=daily
```

See the [Feature Quality Monitoring guide](../how-to-guides/feature-monitoring.md) for full API reference, UI integration, and orchestrator examples.
