# ADR-0005: Stream Transformations

## Status

Accepted

## Context

Feast supported batch features well but lacked in-house support for pull-based stream ingestion or registered stream transformations. While Kafka and Kinesis data sources could be registered, users had to either:

- Write a custom Provider to launch ingestion jobs outside the Feast environment.
- Manually push stream data into the online store via the Push API.

The stream transformation pipeline existed entirely outside of Feast, making it harder to track, version, and manage streaming features.

## Decision

Introduce a `StreamFeatureView` and a `StreamProcessor` interface to provide a standardized pipeline for ingesting and transforming stream data.

### Stream Feature View

```python
from feast import StreamFeatureView, Entity, Field, Aggregation
from feast.types import Float32

@stream_feature_view(
    entities=[entity],
    ttl=timedelta(days=30),
    owner="test@example.com",
    online=True,
    schema=[Field(name="dummy_field", dtype=Float32)],
    description="Stream feature view with aggregations",
    aggregations=[
        Aggregation(column="dummy_field", function="max", time_window=timedelta(days=1)),
        Aggregation(column="dummy_field2", function="count", time_window=timedelta(days=24)),
    ],
    timestamp_field="event_timestamp",
    mode="spark",
    source=stream_source,
)
def pandas_view(pandas_df):
    df = pandas_df.transform(lambda x: x + 10, axis=1)
    return df
```

### Stream Processor

The `StreamProcessor` is a pluggable interface for stream engines (Spark, Flink, etc.):

```python
class StreamProcessor(ABC):
    sfv: StreamFeatureView
    data_source: DataSource

    def ingest_stream_feature_view(self) -> None: ...
    def _ingest_stream_data(self) -> StreamTable: ...
    def _construct_transformation_plan(self, table: StreamTable) -> StreamTable: ...
    def _write_to_online_store(self, table: StreamTable) -> None: ...
```

### Unified Push API

A unified push API was introduced to allow pushing features to both online and offline stores, supporting the Kappa architectural approach to streaming.

### Aggregations

Built-in aggregation functions: `sum`, `count`, `mean`, `max`, `min`. Aggregations use full aggregation with RocksDB for the initial implementation, keeping it simple while reducing request-time latency.

### Key Decisions

- **Full aggregations** chosen over partial aggregations for simplicity and lower request-time latency, using RocksDB to handle memory pressure.
- **Single time window restriction** for initial release; aggregations across different time windows (stream joins) add significant complexity.
- **User-managed ingestion**: Users handle their own ingestion jobs using the StreamProcessor interface with their preferred streaming engine.

## Consequences

### Positive

- Streaming features can be registered and tracked in the Feast registry alongside batch features.
- UDFs for stream transformations are versioned with the feature view definition.
- The pluggable StreamProcessor interface supports multiple streaming engines.
- Unified Push API enables backfilling streaming features to the offline store.

### Negative

- Users must implement their own StreamProcessor for their streaming engine.
- Aggregation support is limited to basic functions in the initial release.
- Stream joins across different time windows are not supported.

## References

- Original RFC: [Feast RFC-036: Stream Transformations](https://docs.google.com/document/d/1Onjy-kiRlHt0USw5ggHu40hpezw1AV8KsgJAh46LoNY/edit)
- Implementation: `sdk/python/feast/stream_feature_view.py`
