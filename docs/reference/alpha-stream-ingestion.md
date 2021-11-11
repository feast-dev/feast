# \[Alpha] Stream ingestion

**Warning**: This is an _experimental_ feature. It's intended for early testing and feedback, and could change without warnings in future releases.

{% hint style="info" %}
To enable this feature, run **`feast alpha enable direct_ingest_to_online_store`**
{% endhint %}

## Overview

Streaming data sources are important sources of feature values. A typical setup with streaming data looks like:

1. Raw events come in (stream 1)
2. Streaming transformations applied (e.g. `last_N_purchased_categories`) (stream 2)
3. Write stream 2 values to an offline store as a historical log for training
4. Write stream 2 values to an online store for low latency feature serving
5. Periodically materialize feature values from the offline store into the online store for improved correctness

Feast now allows users to push features previously registered in a feature view to the online store. This most commonly would be done from a stream processing job (e.g. a Beam or Spark Streaming job). Future versions of Feast will allow writing features directly to the offline store as well.

## Example

See [https://github.com/feast-dev/feast-demo](https://github.com/feast-dev/on-demand-feature-views-demo) for an example on how to ingest stream data into Feast.

We register a feature view as normal, and during stream processing (e.g. Kafka consumers), now we push a dataframe matching the feature view schema:

```python
event_df = pd.DataFrame.from_dict(
    {
        "driver_id": [1001],
        "event_timestamp": [
            datetime(2021, 5, 13, 10, 59, 42),
        ],
        "created": [
            datetime(2021, 5, 13, 10, 59, 42),
        ],
        "conv_rate": [1.0],
        "acc_rate": [1.0],
        "avg_daily_trips": [1000],
    }
)
store.write_to_online_store("driver_hourly_stats", event_df)
```

Feast will coordinate between pushed stream data and regular materialization jobs to ensure only the latest feature values are written to the online store. This ensures correctness in served features for model inference.
