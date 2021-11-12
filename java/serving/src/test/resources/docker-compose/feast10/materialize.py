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

fs = FeatureStore(".")
fs.apply([driver_hourly_stats_view, driver])

now = datetime.now()
fs.materialize(start, now)
print("Materialization finished")
