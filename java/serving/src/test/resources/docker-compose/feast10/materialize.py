import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from feast import FeatureStore

from definitions import driver_hourly_stats_view, driver, entity,\
    benchmark_feature_service, benchmark_feature_views, transformed_conv_rate


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

# Store data in parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
df.to_parquet("driver_stats.parquet")


# For Benchmarks
# Please read more in Feast RFC-031
# (link https://docs.google.com/document/d/12UuvTQnTTCJhdRgy6h10zSbInNGSyEJkIxpOcgOen1I/edit)
# about this benchmark setup
def generate_data(num_rows: int, num_features: int, destination: str) -> pd.DataFrame:
    features = [f"feature_{i}" for i in range(num_features)]
    columns = ["entity", "event_timestamp"] + features
    df = pd.DataFrame(0, index=np.arange(num_rows), columns=columns)
    df["event_timestamp"] = datetime.utcnow()
    for column in features:
        df[column] = np.random.randint(1, num_rows, num_rows)

    df["entity"] = "key-" + \
                   pd.Series(np.arange(1, num_rows + 1)).astype(pd.StringDtype())

    df.to_parquet(destination)


generate_data(10**3, 250, "benchmark_data.parquet")


fs = FeatureStore(".")
fs.apply([driver_hourly_stats_view,
          transformed_conv_rate,
          driver,
          entity, benchmark_feature_service,
          *benchmark_feature_views])

now = datetime.now()
fs.materialize(start, now)
print("Materialization finished")
