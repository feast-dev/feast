# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from datetime import datetime
from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService, FeatureStore

print("Running materialize.py")

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
file_path = "driver_stats.parquet"
driver_hourly_stats = FileSource(
    path=file_path,
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 365),
    features=[
        Feature(name="conv_rate", dtype=ValueType.DOUBLE),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)

fs = FeatureStore("")
fs.apply([driver_hourly_stats_view, driver])

now = datetime.now()
fs.materialize_incremental(now)
