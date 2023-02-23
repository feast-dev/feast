# Hazelcast source

**Warning**: This is an _experimental_ feature. It's intended for early testing and feedback, and could change without warnings in future releases.

## Description

## Stream sources

## Example

### Defining a Kafka source

```python
from datetime import timedelta

from feast import Field, FileSource, KafkaSource, stream_feature_view
from feast.data_format import JsonFormat
from feast.types import Float32

driver_stats_batch_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
)
```

### Using the Hazelcast source in a stream feature view
The Hazelcast source can be used in a stream feature view.
```python
# TODO: Add sample code
```

### Ingesting data
