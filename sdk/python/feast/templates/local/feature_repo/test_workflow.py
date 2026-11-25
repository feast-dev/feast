import json
import subprocess
from datetime import datetime

import pandas as pd

from feast import FeatureStore
from feast.data_source import PushMode


def run_demo():
    store = FeatureStore(repo_path=".")
    print("\n--- Run feast apply ---")
    subprocess.run(["feast", "apply"])

    print("\n--- Historical features for training ---")
    fetch_historical_features_entity_df(store, for_batch_scoring=False)

    print("\n--- Historical features for batch scoring ---")
    fetch_historical_features_entity_df(store, for_batch_scoring=True)

    print("\n--- Load features into online store ---")
    store.materialize_incremental(end_date=datetime.now())

    print("\n--- Online features ---")
    fetch_online_features(store)

    print("\n--- Online features retrieved (instead) through a feature service---")
    fetch_online_features(store, source="feature_service")

    print(
        "\n--- Online features retrieved (using feature service v3, which uses a feature view with a push source---"
    )
    fetch_online_features(store, source="push")

    print(
        "\n--- Online features computed at write time (materialized above, no transformation at read time) ---"
    )
    fetch_online_features(store, source="on_write")

    print("\n--- Simulate a stream event ingestion of the hourly stats df ---")
    event_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001],
            "event_timestamp": [
                datetime.now(),
            ],
            "created": [
                datetime.now(),
            ],
            "conv_rate": [1.0],
            "acc_rate": [1.0],
            "avg_daily_trips": [1000],
            "driver_metadata": [{"vehicle_type": "truck", "rating": "5.0"}],
            "driver_config": [
                json.dumps({"max_distance_km": 500, "preferred_zones": ["north"]})
            ],
            "driver_profile": [{"name": "driver_1001_updated", "age": "30"}],
        }
    )
    print(event_df)
    store.push("driver_stats_push_source", event_df, to=PushMode.ONLINE_AND_OFFLINE)

    print("\n--- Online features again with updated values from a stream push---")
    fetch_online_features(store, source="push")

    print(
        "\n--- Write raw stats to the transform-on-write view; the transformation runs during the write ---"
    )
    store.write_to_online_store(
        feature_view_name="transformed_conv_rate_on_write",
        df=event_df[
            ["driver_id", "event_timestamp", "conv_rate", "acc_rate", "avg_daily_trips"]
        ],
    )

    print("\n--- Online features computed at write time, after the write ---")
    fetch_online_features(store, source="on_write")

    print("\n--- Run feast teardown ---")
    subprocess.run(["feast", "teardown"])


def fetch_historical_features_entity_df(store: FeatureStore, for_batch_scoring: bool):
    # Note: see https://docs.feast.dev/getting-started/concepts/feature-retrieval for more details on how to retrieve
    # for all entities in the offline store instead
    entity_df = pd.DataFrame.from_dict(
        {
            # entity's join key -> entity values
            "driver_id": [1001, 1002, 1003],
            # "event_timestamp" (reserved key) -> timestamps
            "event_timestamp": [
                datetime(2021, 4, 12, 10, 59, 42),
                datetime(2021, 4, 12, 8, 12, 10),
                datetime(2021, 4, 12, 16, 40, 26),
            ],
            # (optional) label name -> label values. Feast does not process these
            "label_driver_reported_satisfaction": [1, 5, 3],
            # values we're using for an on-demand transformation
            "val_to_add": [1, 2, 3],
            "val_to_add_2": [10, 20, 30],
        }
    )
    # For batch scoring, we want the latest timestamps
    if for_batch_scoring:
        entity_df["event_timestamp"] = pd.to_datetime("now", utc=True)

    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
            # Pandas mode on demand transformation
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
            # Native Python mode on demand transformation
            "transformed_conv_rate_python:conv_rate_plus_val1_python",
            "transformed_conv_rate_python:conv_rate_plus_val2_python",
        ],
    ).to_df()
    print(training_df.head())


def fetch_online_features(store, source: str = ""):
    entity_rows = [
        # {join_key: entity_value}
        {
            "driver_id": 1001,
            "val_to_add": 1000,
            "val_to_add_2": 2000,
        },
        {
            "driver_id": 1002,
            "val_to_add": 1001,
            "val_to_add_2": 2002,
        },
    ]
    if source == "feature_service":
        features_to_fetch = store.get_feature_service("driver_activity_v1")
    elif source == "push":
        features_to_fetch = store.get_feature_service("driver_activity_v3")
    elif source == "on_write":
        features_to_fetch = [
            "transformed_conv_rate_on_write:conv_rate_x_acc_rate",
            "transformed_conv_rate_on_write:expected_daily_conversions",
        ]
    else:
        features_to_fetch = [
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:driver_metadata",
            "driver_hourly_stats:driver_config",
            "driver_hourly_stats:driver_profile",
            # Pandas mode on demand transformation
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
            # Native Python mode on demand transformation
            "transformed_conv_rate_python:conv_rate_plus_val1_python",
            "transformed_conv_rate_python:conv_rate_plus_val2_python",
        ]
    returned_features = store.get_online_features(
        features=features_to_fetch,
        entity_rows=entity_rows,
    ).to_dict()
    for key, value in sorted(returned_features.items()):
        print(key, " : ", value)


if __name__ == "__main__":
    run_demo()
