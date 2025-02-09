from datetime import timedelta
from feast import Entity, FeatureView, Field, FeatureStore, RepoConfig, FileSource
from feast.types import Int64, Float32, UnixTimestamp, Bool
from feast.value_type import ValueType
import pandas as pd
from .models import UserStats

def get_feature_store():
    """Initialize and return the feature store with user statistics."""
    # Create Entity
    user = Entity(
        name="user_id",
        value_type=ValueType.INT64,
        description="User identifier",
    )

    # Convert Django model data to DataFrame
    user_stats_df = pd.DataFrame.from_records(
        UserStats.objects.all().values()
    )

    import os
    # Save DataFrame to parquet file
    parquet_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "user_stats.parquet"))
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    
    if not user_stats_df.empty:
        user_stats_df.to_parquet(parquet_path)
    else:
        # Create empty DataFrame with correct schema
        user_stats_df = pd.DataFrame({
            "user_id": [],
            "event_timestamp": [],
            "total_orders": [],
            "average_order_value": [],
            "last_order_date": [],
            "is_active": [],
        })
        user_stats_df.to_parquet(parquet_path)

    # Create Feature View using FileSource
    user_stats_view = FeatureView(
        name="user_stats",
        entities=[user],
        ttl=timedelta(days=1),
        schema=[
            Field(name="total_orders", dtype=Int64),
            Field(name="average_order_value", dtype=Float32),
            Field(name="last_order_date", dtype=UnixTimestamp),
            Field(name="is_active", dtype=Bool),
        ],
        online=True,
        source=FileSource(
            path=parquet_path,
            timestamp_field="event_timestamp",
        ),
    )

    # Initialize Feature Store
    store = FeatureStore(
        config=RepoConfig(
            registry="data/registry.db",
            project="django_feast_demo",
            provider="local",
        )
    )

    # Apply feature view
    store.apply([user, user_stats_view])
    return store

def get_user_features(user_ids):
    """Get features for specific users from the feature store."""
    store = get_feature_store()
    return store.get_online_features(
        features=[
            "user_stats:total_orders",
            "user_stats:average_order_value",
            "user_stats:last_order_date",
            "user_stats:is_active",
        ],
        entity_rows=[{"user_id": user_id} for user_id in user_ids]
    ).to_dict()
