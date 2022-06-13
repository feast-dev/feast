from datetime import timedelta
from pyspark.sql import DataFrame

from feast import (
    FeatureView,
    Field,
)
from feast.stream_feature_view import stream_feature_view
from feast.types import Float32, Int32

from data_sources import *
from entities import *

customer_stats_view = FeatureView(
    name="customer_stats",
    description="customer features",
    entities=[customer],
    ttl=timedelta(seconds=8640000000),
    schema=[
        Field(name="current_balance", dtype=Float32),
        Field(name="avg_passenger_count", dtype=Float32),
        Field(name="lifetime_trip_count", dtype=Int32),
    ],
    online=True,
    source=customer_stats_batch_source,
    tags={"production": "True"},
    owner="test1@gmail.com",
)

@stream_feature_view(
    entities=[driver],
    ttl=timedelta(seconds=8640000000),
    mode="spark",
    schema=[
        Field(name="conv_percentage", dtype=Float32),
        Field(name="acc_percentage", dtype=Float32),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=driver_stats_stream_source,
    tags={},
)
def driver_hourly_stats_stream(df: DataFrame):
    from pyspark.sql.functions import col

    return (
        df.withColumn("conv_percentage", col("conv_rate") * 100.0)
        .withColumn("acc_percentage", col("acc_rate") * 100.0)
        .drop("conv_rate", "acc_rate")
    )
