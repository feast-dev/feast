from datetime import timedelta
from typing import Dict

import pandas as pd

from feast import Feature, FeatureView, OnDemandFeatureView, ValueType
from feast.data_source import DataSource


def driver_feature_view(
    data_source: DataSource,
    name="test_correctness",
    infer_features: bool = False,
    value_type: ValueType = ValueType.FLOAT,
) -> FeatureView:
    return FeatureView(
        name=name,
        entities=["driver"],
        features=None if infer_features else [Feature("value", value_type)],
        ttl=timedelta(days=5),
        input=data_source,
    )


def conv_rate_plus_100(driver_hourly_stats: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_100"] = driver_hourly_stats["conv_rate"] + 100
    return df


def conv_rate_plus_100_feature_view(
    inputs: Dict[str, FeatureView]
) -> OnDemandFeatureView:
    return OnDemandFeatureView(
        name=conv_rate_plus_100.__name__,
        inputs=inputs,
        features=[Feature("conv_rate_plus_100", ValueType.FLOAT)],
        udf=conv_rate_plus_100,
    )


def create_driver_hourly_stats_feature_view(source, infer_features: bool = True):
    driver_stats_feature_view = FeatureView(
        name="driver_stats",
        entities=["driver"],
        features=None
        if infer_features
        else [
            Feature(name="conv_rate", dtype=ValueType.FLOAT),
            Feature(name="acc_rate", dtype=ValueType.FLOAT),
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
        ],
        batch_source=source,
        ttl=timedelta(hours=2),
    )
    return driver_stats_feature_view


def create_customer_daily_profile_feature_view(source, infer_features: bool = False):
    customer_profile_feature_view = FeatureView(
        name="customer_profile",
        entities=["customer_id"],
        features=None
        if infer_features
        else [
            Feature(name="current_balance", dtype=ValueType.FLOAT),
            Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
            Feature(name="lifetime_trip_count", dtype=ValueType.INT32),
        ],
        batch_source=source,
        ttl=timedelta(days=2),
    )
    return customer_profile_feature_view
