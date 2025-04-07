import os

from feast import FeatureStore
from feast.data_source import PushMode
from datetime import datetime
import pandas as pd

# Initialize Feature Store
repo_path = os.getenv("FEAST_REPO_PATH", ".")
store = FeatureStore(repo_path=repo_path)

def fetch_historical_features_entity_df(store: FeatureStore, for_batch_scoring: bool):
    """Fetch historical features for training or batch scoring."""
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
                "val_to_add": [1, 2, 3],
                "val_to_add_2": [10, 20, 30],
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
                "transformed_conv_rate:conv_rate_plus_val1",
                "transformed_conv_rate:conv_rate_plus_val2",
            ],
        ).to_df()
        print(f"Successfully fetched {'batch scoring' if for_batch_scoring else 'training'} historical features:\n", training_df.head())

    except PermissionError:
        print("\n*** PERMISSION DENIED *** Cannot fetch historical features.")
    except Exception as e:
        print(f"Unexpected error while fetching historical features: {e}")

def fetch_online_features(store: FeatureStore, source: str = ""):
    """Fetch online features from the feature store."""
    try:
        entity_rows = [
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

        print(f"Successfully fetched online features {'via feature service' if source else 'directly'}:\n")
        for key, value in sorted(returned_features.items()):
            print(f"{key} : {value}")

    except PermissionError:
        print("\n*** PERMISSION DENIED *** Cannot fetch online features.")
    except Exception as e:
        print(f"Unexpected error while fetching online features: {e}")

def check_permissions():
    """Check user role, test various Feast operations."""
    feature_views = []

    # Step 1: List feature views
    print("\n--- List feature views ---")
    try:
        feature_views = store.list_feature_views()
        if not feature_views:
            print("No feature views found. You might not have access or they haven't been created.")
        else:
            print(f"Successfully listed {len(feature_views)} feature views:")
            for fv in feature_views:
                print(f"  - {fv.name}")
    except PermissionError:
        print("\n*** PERMISSION DENIED *** Cannot list feature views.")
    except Exception as e:
        print(f"Unexpected error listing feature views: {e}")

    # Step 2: Fetch Historical Features
    print("\n--- Fetching Historical Features for Training ---")
    fetch_historical_features_entity_df(store, for_batch_scoring=False)

    print("\n--- Fetching Historical Features for Batch Scoring ---")
    fetch_historical_features_entity_df(store, for_batch_scoring=True)

    # Step 3: Apply Feature Store
    print("\n--- Write to Feature Store ---")
    try:
        store.apply(feature_views)
        print("User has write access to the feature store.")
    except PermissionError:
        print("\n*** PERMISSION DENIED *** User lacks permission to modify the feature store.")
    except Exception as e:
        print(f"Unexpected error testing write access: {e}")

    # Step 4: Fetch Online Features
    print("\n--- Fetching Online Features ---")
    fetch_online_features(store)

    print("\n--- Fetching Online Features via Feature Service ---")
    fetch_online_features(store, source="feature_service")

    print("\n--- Fetching Online Features via Push Source ---")
    fetch_online_features(store, source="push")

    print("\n--- Performing Push Source ---")
    # Step 5: Simulate Event Push (Streaming Ingestion)
    try:
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
        store.push("driver_stats_push_source", event_df, to=PushMode.ONLINE_AND_OFFLINE)
        print("Successfully pushed a test event.")
    except PermissionError:
        print("\n*** PERMISSION DENIED *** Cannot push event (no write access).")
    except Exception as e:
        print(f"Unexpected error while pushing event: {e}")

if __name__ == "__main__":
    check_permissions()
