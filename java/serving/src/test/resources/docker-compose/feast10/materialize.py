# This is an example feature definition file

import pandas as pd
import numpy as np

from google.protobuf.duration_pb2 import Duration

from datetime import datetime, timedelta
from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService, FeatureStore

print("Running materialize.py")

# Fill our temporary data source
start = datetime.now() - timedelta(days=10)

df = pd.DataFrame()
df["driver_id"] = np.arange(1000, 1010)
df["created"] = datetime.now()
df["conv_rate"] = np.arange(0, 1, 0.1)
df["acc_rate"] = np.arange(0.5, 1, 0.05)
df["avg_daily_trips"] = np.arange(0, 1000, 100)

# some of rows are beyond 7 days to test OUTSIDE_MAX_AGE status
df["event_timestamp"] = start + pd.Series(np.arange(0, 10)).map(lambda days: timedelta(days=days))

df.to_parquet("driver_stats.parquet")

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
    ttl=Duration(seconds=86400 * 7),
    features=[
        Feature(name="conv_rate", dtype=ValueType.DOUBLE),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)


# For Benchmarks
# Please read more in Feast RFC-031 (link https://docs.google.com/document/d/12UuvTQnTTCJhdRgy6h10zSbInNGSyEJkIxpOcgOen1I/edit)
# about this benchmark setup
def generate_data(num_rows: int, num_features: int, key_space: int, destination: str) -> pd.DataFrame:
    features = [f"feature_{i}" for i in range(num_features)]
    columns = ["entity", "event_timestamp"] + features
    df = pd.DataFrame(0, index=np.arange(num_rows), columns=columns)
    df["event_timestamp"] = datetime.utcnow()
    for column in ["entity"] + features:
        df[column] = np.random.randint(1, key_space, num_rows)

    df.to_parquet(destination)

generate_data(10**3, 250, 10**3, "benchmark_data.parquet")

generated_data_source = FileSource(
    path="benchmark_data.parquet",
    event_timestamp_column="event_timestamp",
)

entity = Entity(
    name="entity",
    value_type=ValueType.INT64,
)

benchmark_feature_views = [
    FeatureView(
        name=f"feature_view_{i}",
        entities=["entity"],
        ttl=Duration(seconds=86400),
        features=[
            Feature(name=f"feature_{10 * i + j}", dtype=ValueType.INT64)
            for j in range(10)
        ],
        online=True,
        batch_source=generated_data_source,
    )
    for i in range(25)
]

benchmark_feature_service = FeatureService(
    name=f"benchmark_feature_service",
    features=benchmark_feature_views,
)


fs = FeatureStore(".")
fs.apply([driver_hourly_stats_view, driver,
          entity, benchmark_feature_service, *benchmark_feature_views])

now = datetime.now()
fs.materialize(start, now)
print("Materialization finished")
