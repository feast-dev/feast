# Copy from aws template
# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast import MaxcomputeSource

driver_stats_source = MaxcomputeSource(
    # The maxcompute table where features can be found
    table_ref = "feast_driver_stats_source",
    # The event timestamp is used for point-in-time joins
    event_timestamp_column="event_timestamp",
    # The create timestamp, recommand as a partition column
    created_timestamp_column="created",
)

driver = Entity(name="driver_id", join_key="driver_id", value_type=ValueType.INT64, description="driver id",)

# Feature views are a grouping based on how features are stored in either the
# online or offline store.
test_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 15),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_stats_source,
    tags={},
)
