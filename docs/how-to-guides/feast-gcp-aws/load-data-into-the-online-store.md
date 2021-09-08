# Load data into the online store

Feast allows users to load their feature data into an online store in order to serve the latest features to models for online prediction.

## Materializing features

### 1. Register feature views

Before proceeding, please ensure that you have applied \(registered\) the feature views that should be materialized.

{% page-ref page="deploy-a-feature-store.md" %}

### 2.a Materialize

The materialize command allows users to materialize features over a specific historical time range into the online store.

```bash
feast materialize 2021-04-07T00:00:00 2021-04-08T00:00:00
```

The above command will query the batch sources for all feature views over the provided time range, and load the latest feature values into the configured online store.

It is also possible to materialize for specific feature views by using the `-v / --views` argument.

```text
feast materialize 2021-04-07T00:00:00 2021-04-08T00:00:00 \
--views driver_hourly_stats
```

The materialize command is completely stateless. It requires the user to provide the time ranges that will be loaded into the online store. This command is best used from a scheduler that tracks state, like Airflow.

### 2.b Materialize Incremental \(Alternative\)

For simplicity, Feast also provides a materialize command that will only ingest new data that has arrived in the offline store. Unlike `materialize`, `materialize-incremental` will track the state of previous ingestion runs inside of the feature registry.

The example command below will load only new data that has arrived for each feature view up to the end date and time \(`2021-04-08T00:00:00`\).

```text
feast materialize-incremental 2021-04-08T00:00:00
```

The `materialize-incremental` command functions similarly to `materialize` in that it loads data over a specific time range for all feature views \(or the selected feature views\) into the online store.

Unlike `materialize`, `materialize-incremental` automatically determines the start time from which to load features from batch sources of each feature view. The first time `materialize-incremental` is executed it will set the start time to the oldest timestamp of each data source, and the end time as the one provided by the user. For each run of `materialize-incremental`, the end timestamp will be tracked.

Subsequent runs of `materialize-incremental` will then set the start time to the end time of the previous run, thus only loading new data that has arrived into the online store. Note that the end time that is tracked for each run is at the feature view level, not globally for all feature views, i.e, different feature views may have different periods that have been materialized into the online store.

