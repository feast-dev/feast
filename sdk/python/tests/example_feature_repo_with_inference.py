from google.protobuf.duration_pb2 import Duration

from feast import Entity, FeatureView
from feast.data_source import FileSource

driver_hourly_stats = FileSource(
    path="%PARQUET_PATH%",  # placeholder to be replaced by the test
    created_timestamp_column="created",
)

driver = Entity(name="driver_id", description="driver id",)

# features are inferred from columns of data source
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 1),
    online=True,
    input=driver_hourly_stats,
    tags={},
)
