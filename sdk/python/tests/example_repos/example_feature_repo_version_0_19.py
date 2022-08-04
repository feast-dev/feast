from datetime import timedelta

import pandas as pd

from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast.data_source import RequestDataSource
from feast.on_demand_feature_view import on_demand_feature_view

driver_hourly_stats = FileSource(
    path="%PARQUET_PATH%",  # placeholder to be replaced by the test
    event_timestamp_column="event_timestamp",  # Changed to `timestamp_field` in 0.20
    created_timestamp_column="created",
)

driver = Entity(
    name="driver_id",
    description="driver id",
    join_keys=["driver_id"],  # Changed to `join_keys` in 0.20
)


driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=timedelta(days=1),
    features=[  # Changed to `schema` in 0.20
        Feature(name="conv_rate", dtype=ValueType.FLOAT),  # Changed to `Field` in 0.20
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    source=driver_hourly_stats,  # Changed to `source` in 0.20
    tags={},
)


global_daily_stats = FileSource(
    path="%PARQUET_PATH_GLOBAL%",  # placeholder to be replaced by the test
    event_timestamp_column="event_timestamp",  # Changed to `timestamp_field` in 0.20
    created_timestamp_column="created",
)


global_stats_feature_view = FeatureView(
    name="global_daily_stats",
    entities=[],
    ttl=timedelta(days=1),
    features=[  # Changed to `schema` in 0.20
        Feature(name="num_rides", dtype=ValueType.INT32),  # Changed to `Field` in 0.20
        Feature(name="avg_ride_length", dtype=ValueType.FLOAT),
    ],
    online=True,
    source=global_daily_stats,  # Changed to `source` in 0.20
    tags={},
)


request_source = RequestDataSource(
    name="conv_rate_input",
    schema={"val_to_add": ValueType.INT64},
)


@on_demand_feature_view(
    inputs={
        "conv_rate_input": request_source,
        "driver_hourly_stats": driver_hourly_stats_view,
    },
    features=[
        Feature(name="conv_rate_plus_100", dtype=ValueType.DOUBLE),
        Feature(name="conv_rate_plus_val_to_add", dtype=ValueType.DOUBLE),
    ],
)
def conv_rate_plus_100(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_100"] = features_df["conv_rate"] + 100
    df["conv_rate_plus_val_to_add"] = (
        features_df["conv_rate"] + features_df["val_to_add"]
    )
    return df
