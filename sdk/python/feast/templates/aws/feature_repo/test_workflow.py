import random
import subprocess
from datetime import datetime, timedelta, timezone

import pandas as pd

from feast import FeatureStore
from feast.data_source import PushMode


def run_demo():
    store = FeatureStore(repo_path=".")
    print("\n--- Run feast apply to setup feature store on AWS ---")
    subprocess.run(["feast", "apply"])

    print("\n--- Historical features for training ---")
    fetch_historical_features_entity_df(store, for_batch_scoring=False)

    print("\n--- Historical features for batch scoring ---")
    fetch_historical_features_entity_df(store, for_batch_scoring=True)

    print(
        "\n--- Historical features for training (all entities in a window using SQL entity dataframe) ---"
    )
    fetch_historical_features_entity_sql(store, for_batch_scoring=False)

    print(
        "\n--- Historical features for batch scoring (all entities in a window using SQL entity dataframe) ---"
    )
    fetch_historical_features_entity_sql(store, for_batch_scoring=True)

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
            "event_timestamp": [
                datetime.now(),
            ],
            "created": [
                datetime.now(),
            ],
            "conv_rate": [1.0],
            "acc_rate": [1.0 + random.random()],
            "avg_daily_trips": [int(1000 * random.random())],
        }
    )
    print(event_df)
    store.push("driver_stats_push_source", event_df, to=PushMode.ONLINE_AND_OFFLINE)

    print("\n--- Online features again with updated values from a stream push---")
    fetch_online_features(store, source="push")

    print("\n--- Run feast teardown ---")
    subprocess.run(["feast", "teardown"])


def fetch_historical_features_entity_sql(store: FeatureStore, for_batch_scoring):
    end_date = (
        datetime.now()
        .replace(microsecond=0, second=0, minute=0)
        .astimezone(tz=timezone.utc)
    )
    start_date = (end_date - timedelta(days=60)).astimezone(tz=timezone.utc)
    # For batch scoring, we want the latest timestamps
    if for_batch_scoring:
        print(
            "Generating a list of all unique entities in a time window for batch scoring"
        )
        # We use a group by since we want all distinct driver_ids.
        entity_sql = f"""
            SELECT
                driver_id,
                GETDATE() as event_timestamp
            FROM {store.get_data_source("feast_driver_hourly_stats").get_table_query_string()}
            WHERE event_timestamp BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            GROUP BY driver_id
        """
    else:
        print("Generating training data for all entities in a time window")
        # We don't need a group by if we want to generate training data
        entity_sql = f"""
            SELECT
                driver_id,
                event_timestamp
            FROM {store.get_data_source("feast_driver_hourly_stats").get_table_query_string()}
            WHERE event_timestamp BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
        """

    training_df = store.get_historical_features(
        entity_df=entity_sql,
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
    ).to_df()
    print(training_df.head())


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
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
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
    else:
        features_to_fetch = [
            "driver_hourly_stats:acc_rate",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
        ]
    returned_features = store.get_online_features(
        features=features_to_fetch,
        entity_rows=entity_rows,
    ).to_dict()
    for key, value in sorted(returned_features.items()):
        print(key, " : ", value)


if __name__ == "__main__":
    run_demo()
