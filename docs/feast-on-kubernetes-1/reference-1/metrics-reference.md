# Metrics Reference

{% hint style="warning" %}
This page applies to Feast 0.7. The content may be out of date for Feast 0.8+
{% endhint %}

Reference of the metrics that each Feast component exports:

* [Feast Core](metrics-reference.md#feast-core)
* [Feast Serving](metrics-reference.md#feast-serving)
* [Feast Ingestion Job](metrics-reference.md#feast-ingestion-job)

For how to configure Feast to export Metrics, see the [Metrics user guide.](../advanced-1/metrics.md)

## Feast Core

**Exported Metrics**

Feast Core exports the following metrics:

| Metrics | Description | Tags |
| :--- | :--- | :--- |
| `feast_core_request_latency_seconds` | Feast Core's latency in serving Requests in  Seconds. | `service`, `method`, `status_code` |
| `feast_core_feature_set_total` | No. of Feature Sets registered with Feast Core. | None |
| `feast_core_store_total` | No. of Stores registered with Feast Core. | None |
| `feast_core_max_memory_bytes` | Max amount of memory the Java virtual machine will attempt to use. | None |
| `feast_core_total_memory_bytes` | Total amount of memory in the Java virtual machine | None |
| `feast_core_free_memory_bytes` | Total amount of free memory in the Java virtual machine. | None |
| `feast_core_gc_collection_seconds` | Time spent in a given JVM garbage collector in seconds. | None |

**Metric Tags**

Exported Feast Core metrics may be filtered by the following tags/keys

| Tag | Description |
| :--- | :--- |
| `service` | Name of the Service that request is made to. Should be set to `CoreService` |
| `method` | Name of the Method that the request is calling. \(ie `ListFeatureSets`\) |
| `status_code` | Status code returned as a result of handling the requests \(ie `OK`\). Can be used to find request failures. |

## Feast Serving

**Exported Metrics**

Feast Serving exports the following metrics:

| Metric | Description | Tags |
| :--- | :--- | :--- |
| `feast_serving_request_latency_seconds` | Feast Serving's latency in serving Requests in  Seconds. | `method` |
| `feast_serving_request_feature_count` | No. of requests retrieving a Feature from Feast Serving. | `project`, `feature_name` |
| `feast_serving_not_found_feature_count` | No. of requests retrieving a Feature has resulted in a [`NOT_FOUND` field status.](../user-guide/getting-training-features.md#online-field-statuses)  | `project`, `feature_name` |
| `feast_serving_stale_feature_count` | No. of requests retrieving a Feature resulted in a [`OUTSIDE_MAX_AGE` field status.](../user-guide/getting-training-features.md#online-field-statuses) | `project`, `feature_name` |
| `feast_serving_grpc_request_count` | Total gRPC requests served. | `method` |

**Metric Tags**

Exported Feast Serving metrics may be filtered by the following tags/keys

| Tag | Description |
| :--- | :--- |
| `method` | Name of the Method that the request is calling. \(ie `ListFeatureSets`\) |
| `status_code` | Status code returned as a result of handling the requests \(ie `OK`\). Can be used to find request failures. |
| `project` | Name of the project that the FeatureSet of the Feature retrieved belongs to. |
| `feature_name` | Name of the Feature being retrieved. |

## Feast Ingestion Job

Feast Ingestion computes both metrics an statistics on [data ingestion.](../user-guide/define-and-ingest-features.md) Make sure you familar with data ingestion concepts before proceeding.

**Metrics Namespace**

Metrics are computed at  two stages of the Feature Row's/Feature Value's life cycle when being processed by the Ingestion Job:

* `Inflight`- Prior to writing data to stores, but after successful validation of data.
* `WriteToStoreSucess`-  After a successful store write.

Metrics processed by each staged will be tagged with `metrics_namespace`  to the stage where the metric was computed.

**Metrics Bucketing**

Metrics with a `{BUCKET}` are computed on a 60 second window/bucket. Suffix with the following to select the bucket to use:

* `min` - minimum value.
* `max` - maximum value.
* `mean`- mean value.
* `percentile_90`- 90 percentile.
* `percentile_95`- 95 percentile.
* `percentile_99`- 99 percentile.

**Exported Metrics**

<table>
  <thead>
    <tr>
      <th style="text-align:left">Metric</th>
      <th style="text-align:left">Description</th>
      <th style="text-align:left">Tags</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><code>feast_ingestion_feature_row_lag_ms_{BUCKET}</code>
      </td>
      <td style="text-align:left">Lag time in milliseconds between succeeding ingested Feature Rows.</td>
      <td
      style="text-align:left">
        <p><code>feast_store</code>, <code>feast_project_name</code>,<code>feast_featureSet_name</code>,<code>ingestion_job_name</code>,</p>
        <p><code>metrics_namespace</code>
        </p>
        </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>feast_ingestion_feature_value_lag_ms_{BUCKET}</code>
      </td>
      <td style="text-align:left">Lag time in milliseconds between succeeding ingested values for each Feature.</td>
      <td
      style="text-align:left">
        <p><code>feast_store</code>, <code>feast_project_name</code>,<code>feast_featureSet_name</code>,</p>
        <p> <code>feast_feature_name</code>,</p>
        <p><code>ingestion_job_name</code>,</p>
        <p><code>metrics_namespace</code>
        </p>
        </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>feast_ingestion_feature_value_{BUCKET}</code>
      </td>
      <td style="text-align:left">Last value feature for each Feature.</td>
      <td style="text-align:left"><code>feast_store</code>, <code>feature_project_name</code>, <code>feast_feature_name</code>,<code>feast_featureSet_name</code>, <code>ingest_job_name</code>, <code>metrics_namepace</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>feast_ingestion_feature_row_ingested_count</code>
      </td>
      <td style="text-align:left">No. of Ingested Feature Rows</td>
      <td style="text-align:left">
        <p><code>feast_store</code>, <code>feast_project_name</code>,<code>feast_featureSet_name</code>,<code>ingestion_job_name</code>,</p>
        <p><code>metrics_namespace</code>
        </p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>feast_ingestion_feature_value_missing_count</code>
      </td>
      <td style="text-align:left">No. of times a ingested Feature values did not provide a value for the
        Feature.</td>
      <td style="text-align:left">
        <p><code>feast_store</code>, <code>feast_project_name</code>,<code>feast_featureSet_name</code>,</p>
        <p> <code>feast_feature_name</code>,</p>
        <p><code>ingestion_job_name</code>,</p>
        <p><code>metrics_namespace</code>
        </p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>feast_ingestion_deadletter_row_count</code>
      </td>
      <td style="text-align:left">No. of Feature Rows that that the Ingestion Job did not successfully write
        to store.</td>
      <td style="text-align:left"><code>feast_store</code>, <code>feast_project_name</code>,<code>feast_featureSet_name</code>,<code>ingestion_job_name</code>
      </td>
    </tr>
  </tbody>
</table>

**Metric Tags**

Exported Feast Ingestion Job  metrics may be filtered by the following tags/keys

| Tag | Description |
| :--- | :--- |
| `feast_store` | Name of the target store the Ingestion Job is writing to. |
| `feast_project_name` | Name of the project that the ingested FeatureSet belongs to. |
| `feast_featureSet_name` | Name of the Feature Set being ingested. |
| `feast_feature_name` | Name of the Feature being ingested. |
| `ingestion_job_name` | Name of the Ingestion Job performing data ingestion. Typically this is set to the Id of the Ingestion Job. |
| `metrics_namespace` | Stage where metrics where computed. Either  `Inflight` or `WriteToStoreSuccess` |

