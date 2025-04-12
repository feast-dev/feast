from datetime import timedelta

from pyspark.sql import DataFrame

from feast import BatchFeatureView, Entity, Field, FileSource
from feast.types import Float32, Int32, Int64

driver_hourly_stats = FileSource(
    path="%PARQUET_PATH%",  # placeholder to be replaced by the test
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver = Entity(
    name="driver_id",
    description="driver id",
)

schema = [
    Field(name="conv_rate", dtype=Float32),
    Field(name="acc_rate", dtype=Float32),
    Field(name="avg_daily_trips", dtype=Int64),
    Field(name="driver_id", dtype=Int32),
]


def transform_feature(df: DataFrame) -> DataFrame:
    df = df.withColumn("conv_rate", df["conv_rate"] * 2)
    df = df.withColumn("acc_rate", df["acc_rate"] * 2)
    return df


driver_hourly_stats_view = BatchFeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    mode="python",
    udf=transform_feature,
    udf_string="transform_feature",
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="driver_id", dtype=Int32),
    ],
    online=True,
    offline=True,
    source=driver_hourly_stats,
    tags={},
)

global_daily_stats = FileSource(
    path="%PARQUET_PATH_GLOBAL%",  # placeholder to be replaced by the test
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

global_stats_feature_view = BatchFeatureView(
    name="global_daily_stats",
    entities=None,
    mode="python",
    udf=lambda x: x,
    ttl=timedelta(days=1),
    schema=[
        Field(name="num_rides", dtype=Int32),
        Field(name="avg_ride_length", dtype=Float32),
    ],
    online=True,
    offline=True,
    source=global_daily_stats,
    tags={},
)
