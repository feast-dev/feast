import os
from datetime import datetime

import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode


def run_demo():
    try:
        os.environ["LOCAL_K8S_TOKEN"] = ""

        store = FeatureStore(repo_path="/app/feature_repo")

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

        print("\n--- Simulate a stream event ingestion of the hourly stats df ---")
        event_df = pd.DataFrame.from_dict(
            {
                "driver_id": [1001],
                "event_timestamp": [datetime.now()],
                "created": [datetime.now()],
                "conv_rate": [1.0],
                "acc_rate": [1.0],
                "avg_daily_trips": [1000],
            }
        )
        print(event_df)
        store.push("driver_stats_push_source", event_df, to=PushMode.ONLINE_AND_OFFLINE)

        print("\n--- Online features again with updated values from a stream push---")
        fetch_online_features(store, source="push")

    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_historical_features_entity_df(store: FeatureStore, for_batch_scoring: bool):
    try:
        entity_df = pd.DataFrame.from_dict(
            {
                "driver_id": [1001, 1002, 1003],
                "event_timestamp": [
                    datetime(2021, 4, 12, 10, 59, 42),
                    datetime(2021, 4, 12, 8, 12, 10),
                    datetime(2021, 4, 12, 16, 40, 26),
                ],
                "label_driver_reported_satisfaction": [1, 5, 3],
                # values we're using for an on-demand transformation
                # "val_to_add": [1, 2, 3],
                # "val_to_add_2": [10, 20, 30],

            }

        )
        if for_batch_scoring:
            entity_df["event_timestamp"] = pd.to_datetime("now", utc=True)

        training_df = store.get_historical_features(
            entity_df=entity_df,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
               # "transformed_conv_rate:conv_rate_plus_val1",
               # "transformed_conv_rate:conv_rate_plus_val2",
            ],
        ).to_df()
        print(training_df.head())

    except Exception as e:
        print(f"An error occurred while fetching historical features: {e}")


def fetch_online_features(store, source: str = ""):
    try:
        entity_rows = [
            {
                "driver_id": 1001,
            },
            {
                "driver_id": 1002,
            },
        ]
        if source == "feature_service":
            features_to_fetch = store.get_feature_service("driver_activity_v1")
        elif source == "push":
            features_to_fetch = store.get_feature_service("driver_activity_v3")
        else:
            features_to_fetch = [
                "driver_hourly_stats:acc_rate",
            ]
        returned_features = store.get_online_features(
            features=features_to_fetch,
            entity_rows=entity_rows,
        ).to_dict()
        for key, value in sorted(returned_features.items()):
            print(key, " : ", value)

    except Exception as e:
        print(f"An error occurred while fetching online features: {e}")


if __name__ == "__main__":
    try:
        run_demo()
    except Exception as e:
        print(f"An error occurred in the main execution: {e}")
