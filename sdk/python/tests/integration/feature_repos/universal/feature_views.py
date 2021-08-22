from datetime import timedelta

from feast import Feature, FeatureView, ValueType
from feast.data_source import DataSource


def driver_feature_view(
    data_source: DataSource, name="test_correctness"
) -> FeatureView:
    return FeatureView(
        name=name,
        entities=["driver"],
        features=[Feature("value", ValueType.FLOAT)],
        ttl=timedelta(days=5),
        input=data_source,
    )


def create_driver_hourly_stats_feature_view(source):
    driver_stats_feature_view = FeatureView(
        name="driver_stats",
        entities=["driver"],
        features=[
            Feature(name="conv_rate", dtype=ValueType.FLOAT),
            Feature(name="acc_rate", dtype=ValueType.FLOAT),
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
        ],
        batch_source=source,
        ttl=timedelta(hours=2),
    )
    return driver_stats_feature_view


def create_customer_daily_profile_feature_view(source):
    customer_profile_feature_view = FeatureView(
        name="customer_profile",
        entities=["customer_id"],
        features=[
            Feature(name="current_balance", dtype=ValueType.FLOAT),
            Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
            Feature(name="lifetime_trip_count", dtype=ValueType.INT32),
        ],
        batch_source=source,
        ttl=timedelta(days=2),
    )
    return customer_profile_feature_view
