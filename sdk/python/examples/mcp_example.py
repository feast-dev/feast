"""Example of using the Feast MCP server."""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from feast import FeatureStore
from feast.data_source import PushSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.types import Float32, Int64

# Set up paths
repo_path = Path("./feature_repo")
repo_path.mkdir(exist_ok=True)

# Create feature_store.yaml
with open(repo_path / "feature_store.yaml", "w") as f:
    f.write("""
project: mcp_example
registry: data/registry.db
provider: local
online_store:
    type: sqlite
    path: data/online_store.db
offline_store:
    type: file
""")

# Create data directory
data_dir = repo_path / "data"
data_dir.mkdir(exist_ok=True)

# Create example data
entity_df = pd.DataFrame(
    {
        "driver_id": [1001, 1002, 1003, 1004, 1005],
        "event_timestamp": [
            datetime.now() - timedelta(minutes=11),
            datetime.now() - timedelta(minutes=10),
            datetime.now() - timedelta(minutes=9),
            datetime.now() - timedelta(minutes=8),
            datetime.now() - timedelta(minutes=7),
        ],
        "conv_rate": [0.5, 0.3, 0.4, 0.6, 0.5],
        "avg_daily_trips": [15, 10, 20, 30, 25],
        "acc_rate": [0.9, 0.8, 0.7, 0.9, 0.8],
    }
)

# Save example data
entity_df.to_parquet(data_dir / "driver_stats.parquet")

# Create feature definitions
with open(repo_path / "example.py", "w") as f:
    f.write("""
from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, FeatureService, Field, PushSource
from feast.types import Float32, Int64

# Define an entity for the driver
driver = Entity(
    name="driver",
    description="Driver entity",
    join_keys=["driver_id"],
)

# Define a push source for driver statistics
driver_stats_push_source = PushSource(
    name="driver_stats_push_source",
    batch_source=Path("data/driver_stats.parquet"),
)

# Define a feature view for driver statistics
driver_stats = FeatureView(
    name="driver_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="acc_rate", dtype=Float32),
    ],
    online=True,
    source=driver_stats_push_source,
    tags={"team": "driver_performance"},
)

# Define a feature service
driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[driver_stats[["conv_rate", "avg_daily_trips"]]],
)

driver_activity_v2 = FeatureService(
    name="driver_activity_v2",
    features=[driver_stats[["conv_rate", "avg_daily_trips", "acc_rate"]]],
)
""")

def main():
    # Initialize the feature store
    store = FeatureStore(repo_path=str(repo_path))
    
    # Apply the feature definitions
    store.apply([])
    
    # Materialize the features
    store.materialize(
        end_date=datetime.now(),
        start_date=datetime.now() - timedelta(days=1),
    )
    
    # Start the MCP server
    print("Starting MCP server on http://localhost:8080/mcp")
    print("Press Ctrl+C to stop the server")
    
    try:
        store.serve_mcp(
            host="localhost",
            port=8080,
        )
    except KeyboardInterrupt:
        print("Stopping MCP server")

if __name__ == "__main__":
    main()
