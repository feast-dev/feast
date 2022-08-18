# This is an example feature definition file

from datetime import timedelta

from feast import Entity, Feature, FeatureView, Field, FileSource, FeatureService
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.types import Float32, Int64

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = FileSource(
    path="driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
driver = Entity(name="driver_id", description="driver id")

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(seconds=86400 * 365 * 10),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
)

driver_stats_fs = FeatureService(
    name="test_service",
    features=[driver_hourly_stats_view],
    logging_config=LoggingConfig(destination=FileLoggingDestination(path=""))
)