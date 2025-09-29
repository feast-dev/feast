#!/usr/bin/env python3

"""
Test workflow for Ray offline store and compute engine template.

This script demonstrates:
1. Ray offline store for efficient data I/O
2. Ray compute engine for distributed feature processing
3. Historical feature retrieval with point-in-time joins
4. Feature materialization to online store
5. Online feature serving

Run this after: feast apply
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Add the feature repo to the path
repo_path = Path(__file__).parent
sys.path.append(str(repo_path))

try:
    from feast import FeatureStore
except ImportError:
    print("Please install feast: pip install feast[ray]")
    sys.exit(1)


def run_demo():
    print("=" * 60)
    print("🚀 Ray Offline Store & Compute Engine Demo")
    print("=" * 60)

    # Initialize the feature store
    print("\n1. Initializing Feast with Ray configuration...")
    store = FeatureStore(repo_path=".")

    print(f"   ✓ Offline store: {store.config.offline_store.type}")
    if hasattr(store.config, "batch_engine") and store.config.batch_engine:
        print(f"   ✓ Compute engine: {store.config.batch_engine.type}")
    else:
        print("   ⚠ No compute engine configured")

    # Create entity DataFrame for historical features
    print("\n2. Creating entity DataFrame for historical feature retrieval...")
    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=2)

    entity_df = pd.DataFrame(
        {
            "driver_id": [1001, 1002, 1003],
            "customer_id": [2001, 2002, 2003],
            "event_timestamp": [
                pd.Timestamp(end_date - timedelta(hours=24), tz="UTC"),
                pd.Timestamp(end_date - timedelta(hours=12), tz="UTC"),
                pd.Timestamp(end_date - timedelta(hours=6), tz="UTC"),
            ],
        }
    )

    print(f"   ✓ Created entity DataFrame with {len(entity_df)} rows")
    print(f"   ✓ Time range: {start_date} to {end_date}")

    # Retrieve historical features using Ray compute engine
    print("\n3. Retrieving historical features with Ray compute engine...")
    print("   (This demonstrates distributed point-in-time joins)")

    try:
        # Get historical features - this uses Ray compute engine for distributed processing
        historical_features = store.get_historical_features(
            entity_df=entity_df,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
                "customer_daily_profile:current_balance",
                "customer_daily_profile:avg_passenger_count",
                "customer_daily_profile:lifetime_trip_count",
            ],
        )

        # Convert to DataFrame - Ray processes this efficiently
        historical_df = historical_features.to_df()
        print(f"   ✓ Retrieved {len(historical_df)} historical feature rows")
        print(f"   ✓ Features: {list(historical_df.columns)}")

        # Show sample of the data
        print("\n   Sample historical features:")
        print(historical_df.head(3).to_string(index=False))

    except Exception as e:
        print(f"   ⚠ Historical features retrieval failed: {e}")
        print("   This might be due to missing Ray dependencies or data")

    # Demonstrate on-demand feature transformations
    print("\n4. Testing on-demand feature transformations...")
    try:
        # Get features including on-demand transformations
        features_with_odfv = store.get_historical_features(
            entity_df=entity_df.head(1),
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
                "driver_activity_v2:conv_rate_plus_acc_rate",
                "driver_activity_v2:trips_per_day_normalized",
            ],
        )

        odfv_df = features_with_odfv.to_df()
        print(f"   ✓ Retrieved {len(odfv_df)} rows with on-demand transformations")

        # Show sample with transformations
        print("\n   Sample with on-demand features:")
        print(
            odfv_df[["driver_id", "conv_rate", "acc_rate", "conv_rate_plus_acc_rate"]]
            .head(3)
            .to_string(index=False)
        )

    except Exception as e:
        print(f"   ⚠ On-demand features failed: {e}")

    # Materialize features to online store
    print("\n5. Materializing features to online store...")
    try:
        materialize_end = end_date

        print(f"   Attempting materialization up to {materialize_end}")

        # Try materialization with Ray compute engine
        store.materialize_incremental(end_date=materialize_end)
        print("   ✓ Ray compute engine materialization successful!")

        # Test online feature retrieval
        print("\n6. Testing online feature serving...")
        online_features = store.get_online_features(
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "customer_daily_profile:current_balance",
            ],
            entity_rows=[
                {"driver_id": 1001, "customer_id": 2001},
                {"driver_id": 1002, "customer_id": 2002},
            ],
        )

        online_df = online_features.to_df()
        print(f"   ✓ Retrieved {len(online_df)} online feature rows")
        print("\n   Sample online features:")
        print(online_df.to_string(index=False))

    except Exception as e:
        print(f"   ⚠ Materialization/online serving failed: {e}")

    print("\n" + "=" * 60)
    print("🎉 Ray Demo Complete!")
    print("=" * 60)

    print(
        "\nIf you want to explore Feast with your existing ray cluster, you can configure ray_address to feature_store.yaml: \n"
    )
    print("""
    offline_store:
      ray_address: "127.0.0.1:10001"
    batch_engine:
      ray_address: "127.0.0.1:10001"
    """)


if __name__ == "__main__":
    run_demo()
