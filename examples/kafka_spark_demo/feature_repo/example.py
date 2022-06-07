# This is an example feature definition file

from datetime import timedelta

from feast import Entity, FeatureService, StreamFeatureView, Field, FileSource, KafkaSource, PushSource, FeatureView
from feast.types import Float32, Int64, Float64, UnixTimestamp
from feast.data_format import AvroFormat, JsonFormat

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_stats_batch_source = FileSource(
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

normal_stats_view = FeatureView(
    name="normal_stats",
    description="Hourly features",
    entities=["driver"],
    ttl=timedelta(seconds=8640000000),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="miles_driven", dtype=Float32),
    ],
    online=True,
    source=driver_stats_batch_source,
    tags={"production": "True"},
    owner="test2@gmail.com",
)

# A push source is useful if you have upstream systems that transform features (e.g. stream processing jobs)
driver_stats_push_source = PushSource(
    name="driver_stats_push_source", batch_source=driver_stats_batch_source,
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver", join_keys=["driver_id"])

driver_stats_stream_source = KafkaSource(
    name="driver_stats_stream",
    bootstrap_servers="localhost:9092",
    topic="drivers",
    timestamp_field="event_timestamp",
    batch_source=driver_stats_batch_source,
    message_format=JsonFormat(
        schema_json="driver_id integer, miles_driven double, event_timestamp timestamp, conv_rate double, acc_rate double"
    )
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = StreamFeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    mode="spark",
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="miles_driven", dtype=Float32)
    ],
    online=True,
    source=driver_stats_stream_source,
    tags={},
)

driver_stats_fs = FeatureService(
    name="driver_activity", features=[driver_hourly_stats_view]
)
