#!/usr/bin/env python
"""
Iceberg Local Example - Complete End-to-End Workflow

This script demonstrates:
1. Creating sample data and writing to Iceberg tables
2. Applying feature definitions to the feature store
3. Materializing features to the online store
4. Retrieving online features
5. Retrieving historical features (point-in-time correct)

Requirements:
    uv sync --extra iceberg
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    FloatType,
    LongType,
    NestedField,
    TimestampType,
)

# Add current directory to path for feature imports
sys.path.append(os.path.dirname(__file__))

from feast import FeatureStore


def create_sample_data() -> pd.DataFrame:
    """
    Create sample driver statistics data.

    Returns:
        DataFrame with driver statistics including timestamps
    """
    print("\n=== Creating Sample Data ===")

    # Create timestamps for the last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    # Generate hourly timestamps
    timestamps = pd.date_range(start_date, end_date, freq="1h")

    # Create sample data for 5 drivers
    driver_ids = [1001, 1002, 1003, 1004, 1005]

    data = []
    for driver_id in driver_ids:
        for timestamp in timestamps:
            data.append(
                {
                    "driver_id": driver_id,
                    "event_timestamp": timestamp,
                    "created": timestamp + timedelta(seconds=1),
                    "conv_rate": 0.5
                    + (driver_id % 100) / 1000.0
                    + (hash(str(timestamp)) % 50) / 1000.0,
                    "acc_rate": 0.8
                    + (driver_id % 100) / 1000.0
                    + (hash(str(timestamp)) % 30) / 1000.0,
                    "avg_daily_trips": 10
                    + (driver_id % 10)
                    + (hash(str(timestamp)) % 5),
                }
            )

    df = pd.DataFrame(data)
    print(f"Created {len(df)} rows of sample data for {len(driver_ids)} drivers")
    print(f"Date range: {df['event_timestamp'].min()} to {df['event_timestamp'].max()}")
    print(f"\nSample data:\n{df.head()}")

    return df


def setup_iceberg_table(df: pd.DataFrame):
    """
    Create Iceberg catalog and write sample data to a table.

    Args:
        df: DataFrame with driver statistics
    """
    print("\n=== Setting Up Iceberg Table ===")

    os.makedirs("data", exist_ok=True)
    os.makedirs("data/warehouse", exist_ok=True)

    # Create catalog
    catalog = load_catalog(
        "demo_catalog",
        **{
            "type": "sql",
            "uri": "sqlite:///data/iceberg_catalog.db",
            "warehouse": "data/warehouse",
        },
    )

    # Create namespace
    try:
        catalog.create_namespace("demo")
        print("Created namespace 'demo'")
    except Exception as e:
        print(f"Namespace 'demo' already exists: {e}")

    # Drop table if it exists (for clean runs)
    try:
        catalog.drop_table("demo.driver_stats")
        print("Dropped existing table 'demo.driver_stats'")
    except Exception:
        pass

    # Define Iceberg schema
    iceberg_schema = Schema(
        NestedField(1, "driver_id", LongType(), required=False),
        NestedField(2, "event_timestamp", TimestampType(), required=False),
        NestedField(3, "created", TimestampType(), required=False),
        NestedField(4, "conv_rate", FloatType(), required=False),
        NestedField(5, "acc_rate", FloatType(), required=False),
        NestedField(6, "avg_daily_trips", LongType(), required=False),
    )

    # Create table
    table = catalog.create_table("demo.driver_stats", schema=iceberg_schema)
    print("Created Iceberg table 'demo.driver_stats'")

    # Convert pandas to Arrow (with microsecond timestamps for Iceberg)
    arrow_schema = pa.schema(
        [
            pa.field("driver_id", pa.int64()),
            pa.field("event_timestamp", pa.timestamp("us")),
            pa.field("created", pa.timestamp("us")),
            pa.field("conv_rate", pa.float32()),
            pa.field("acc_rate", pa.float32()),
            pa.field("avg_daily_trips", pa.int64()),
        ]
    )

    arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)

    # Write data to Iceberg table
    table.append(arrow_table)
    print(f"Wrote {len(df)} rows to Iceberg table")

    # Verify data
    scan = table.scan()
    result = scan.to_arrow()
    print(f"Verified: Table contains {len(result)} rows")


def run_feast_workflow():
    """
    Run the complete Feast workflow:
    1. Apply features
    2. Materialize to online store
    3. Retrieve online features
    4. Retrieve historical features
    """
    print("\n=== Running Feast Workflow ===")

    # Initialize feature store
    fs = FeatureStore(repo_path=".")
    print("Initialized Feature Store")

    # Apply features from features.py
    print("\nApplying feature definitions...")
    from features import driver, driver_stats_fv, driver_activity_v1, driver_activity_v2

    fs.apply([driver, driver_stats_fv, driver_activity_v1, driver_activity_v2])
    print("Applied entities, feature views, and feature services")

    # Materialize features to online store
    print("\nMaterializing features to online store...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    fs.materialize(start_date, end_date)
    print(f"Materialized features from {start_date} to {end_date}")

    # Retrieve online features
    print("\n=== Retrieving Online Features ===")
    entity_rows = [
        {"driver_id": 1001},
        {"driver_id": 1002},
        {"driver_id": 1003},
    ]

    online_features = fs.get_online_features(
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
        entity_rows=entity_rows,
    )

    online_df = online_features.to_df()
    print(f"\nOnline Features (latest values):\n{online_df}")

    # Retrieve historical features
    print("\n=== Retrieving Historical Features (Point-in-Time Correct) ===")

    # Create entity dataframe for historical retrieval
    # This simulates requesting features at specific points in time
    entity_df = pd.DataFrame(
        {
            "driver_id": [1001, 1002, 1003, 1001, 1002],
            "event_timestamp": [
                end_date - timedelta(days=1),
                end_date - timedelta(days=2),
                end_date - timedelta(days=3),
                end_date - timedelta(hours=12),
                end_date - timedelta(hours=6),
            ],
        }
    )

    print(f"\nEntity DataFrame for historical retrieval:\n{entity_df}")

    training_df = fs.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
    ).to_df()

    print(f"\nHistorical Features (point-in-time correct):\n{training_df}")

    # Demonstrate feature service usage
    print("\n=== Using Feature Service ===")

    feature_service_features = fs.get_online_features(
        features=fs.get_feature_service("driver_activity_v1"),
        entity_rows=entity_rows,
    )

    feature_service_df = feature_service_features.to_df()
    print(f"\nFeature Service 'driver_activity_v1':\n{feature_service_df}")


def cleanup():
    """
    Optional cleanup function to remove generated files.
    Commented out by default to allow inspection of results.
    """
    print("\n=== Cleanup (skipped - uncomment to enable) ===")
    # import shutil
    # shutil.rmtree("data", ignore_errors=True)
    # print("Removed data directory")


def main():
    """Main execution function."""
    print("=" * 60)
    print("Iceberg Local Example - Feast Feature Store")
    print("=" * 60)

    try:
        # Step 1: Create sample data
        df = create_sample_data()

        # Step 2: Setup Iceberg table and write data
        setup_iceberg_table(df)

        # Step 3: Run Feast workflow
        run_feast_workflow()

        print("\n" + "=" * 60)
        print("Example completed successfully!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Explore the data/ directory to see Iceberg table files")
        print("2. Modify features.py to add new features")
        print("3. Run this script again to see changes")
        print("4. Check the Feast documentation for advanced features")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
