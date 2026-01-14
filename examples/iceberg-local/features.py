"""
Feature definitions for Iceberg local example.

This module demonstrates how to define features using Iceberg as both
offline and online storage.
"""

from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
    IcebergSource,
)
from feast.types import Float32, Int64

# Define the driver entity
# Entities are the primary keys used to fetch features
driver = Entity(
    name="driver",
    join_keys=["driver_id"],
    description="Driver entity for the ride-sharing platform",
)

# Define the Iceberg data source for driver statistics
# This points to an Iceberg table in the local warehouse
driver_stats_source = IcebergSource(
    name="driver_hourly_stats_source",
    table_identifier="demo.driver_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="Hourly driver statistics from Iceberg table",
)

# Define the feature view
# This maps the Iceberg source to features that can be served online
driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(
            name="conv_rate",
            dtype=Float32,
            description="Driver conversion rate (percentage of successful rides)",
        ),
        Field(
            name="acc_rate",
            dtype=Float32,
            description="Driver acceptance rate (percentage of accepted requests)",
        ),
        Field(
            name="avg_daily_trips",
            dtype=Int64,
            description="Average number of trips per day",
        ),
    ],
    online=True,
    source=driver_stats_source,
    tags={"team": "driver_performance", "pii": "false"},
)

# Define a feature service
# This groups features for a specific model or use case
driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[
        driver_stats_fv[["conv_rate", "acc_rate"]],  # Sub-select specific features
    ],
    description="Driver activity features for real-time model v1",
)

driver_activity_v2 = FeatureService(
    name="driver_activity_v2",
    features=[driver_stats_fv],  # Include all features
    description="Driver activity features for real-time model v2",
)
