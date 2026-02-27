"""
Integration tests for rate limiting across all online stores.
"""

import os
import time
from datetime import datetime, timedelta

import pandas as pd
import pytest

from feast import FeatureView, Field
from feast.types import Float32, Int64
from tests.integration.feature_repos.universal.entities import driver


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_rate_limiting_enforced_with_feature_view_tags(
    environment, universal_data_sources
):
    """Test that rate limiting is enforced via feature view tags."""
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return

    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources

    # Create feature view with rate limit via tags
    driver_stats_fv = FeatureView(
        name="driver_rate_limit_test",
        entities=[driver()],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
        ],
        source=data_sources.driver,
        ttl=timedelta(days=1),
        tags={"write_rate_limit": "20"},  # 20 writes/sec via tag
    )

    fs.apply([driver(), driver_stats_fv])

    # Create test dataframe with 60 records
    df = pd.DataFrame(
        {
            "driver_id": list(range(1001, 1061)),
            "conv_rate": [0.5 + i * 0.01 for i in range(60)],
            "avg_daily_trips": [100 + i for i in range(60)],
            "event_timestamp": [datetime.now()] * 60,
        }
    )

    # Measure write time
    start_time = time.time()
    fs.write_to_online_store("driver_rate_limit_test", df)
    elapsed_time = time.time() - start_time

    # Verify rate limiting: 60 records at 20/sec = ~3 seconds
    assert 2.5 <= elapsed_time <= 4.5, (
        f"Rate limit not enforced. Expected 2.5-4.5s for 60 records at 20/sec, "
        f"got {elapsed_time:.2f}s"
    )

    # Verify data integrity
    online_features = fs.get_online_features(
        features=["driver_rate_limit_test:avg_daily_trips"],
        entity_rows=[{"driver_id": 1001}, {"driver_id": 1030}],
    ).to_df()

    assert len(online_features) == 2
    assert online_features["avg_daily_trips"].iloc[0] == 100
    assert online_features["avg_daily_trips"].iloc[1] == 129


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_no_rate_limit_when_tag_not_specified(environment, universal_data_sources):
    """Test that writes are fast when no rate limit tag is specified."""
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return

    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources

    # Feature view WITHOUT rate limit tag
    driver_stats_fv = FeatureView(
        name="driver_no_limit_test",
        entities=[driver()],
        schema=[Field(name="trips", dtype=Int64)],
        source=data_sources.driver,
        ttl=timedelta(days=1),
    )

    fs.apply([driver(), driver_stats_fv])

    # Create 100 records
    df = pd.DataFrame(
        {
            "driver_id": list(range(2001, 2101)),
            "trips": list(range(100)),
            "event_timestamp": [datetime.now()] * 100,
        }
    )

    # Should complete quickly without rate limiting
    start_time = time.time()
    fs.write_to_online_store("driver_no_limit_test", df)
    elapsed_time = time.time() - start_time

    # Should be fast without rate limiting (< 2 seconds)
    assert elapsed_time < 2.0, (
        f"Writes too slow without rate limit: {elapsed_time:.2f}s"
    )


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_multiple_feature_views_independent_rate_limits(
    environment, universal_data_sources
):
    """Test that different feature views have independent rate limiters."""
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return

    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources

    # Feature view 1: 20 writes/sec
    fv1 = FeatureView(
        name="driver_fv1_20ps",
        entities=[driver()],
        schema=[Field(name="value1", dtype=Int64)],
        source=data_sources.driver,
        ttl=timedelta(days=1),
        tags={"write_rate_limit": "20"},
    )

    # Feature view 2: 40 writes/sec (faster)
    fv2 = FeatureView(
        name="driver_fv2_40ps",
        entities=[driver()],
        schema=[Field(name="value2", dtype=Int64)],
        source=data_sources.driver,
        ttl=timedelta(days=1),
        tags={"write_rate_limit": "40"},
    )

    fs.apply([driver(), fv1, fv2])

    # Create data for both FVs (40 records each)
    df1 = pd.DataFrame(
        {
            "driver_id": list(range(4001, 4041)),
            "value1": list(range(40)),
            "event_timestamp": [datetime.now()] * 40,
        }
    )

    df2 = pd.DataFrame(
        {
            "driver_id": list(range(5001, 5041)),
            "value2": list(range(40)),
            "event_timestamp": [datetime.now()] * 40,
        }
    )

    # Write to first FV
    start1 = time.time()
    fs.write_to_online_store("driver_fv1_20ps", df1)
    elapsed1 = time.time() - start1

    # Write to second FV
    start2 = time.time()
    fs.write_to_online_store("driver_fv2_40ps", df2)
    elapsed2 = time.time() - start2

    # FV1 should take ~2 seconds (slower rate)
    assert 1.5 <= elapsed1 <= 3.0, f"FV1 timing unexpected: {elapsed1:.2f}s"

    # FV2 should take ~1 second (faster rate)
    assert 0.8 <= elapsed2 <= 1.5, f"FV2 timing unexpected: {elapsed2:.2f}s"

    # FV2 should be faster than FV1
    assert elapsed2 < elapsed1


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_zero_rate_limit_disables_limiting(environment, universal_data_sources):
    """Test that setting write_rate_limit=0 disables rate limiting."""
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return

    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources

    # Feature view with explicit rate_limit=0
    driver_fv = FeatureView(
        name="driver_zero_limit",
        entities=[driver()],
        schema=[Field(name="value", dtype=Int64)],
        source=data_sources.driver,
        ttl=timedelta(days=1),
        tags={"write_rate_limit": "0"},
    )

    fs.apply([driver(), driver_fv])

    # Write 100 records
    df = pd.DataFrame(
        {
            "driver_id": list(range(6001, 6101)),
            "value": list(range(100)),
            "event_timestamp": [datetime.now()] * 100,
        }
    )

    start_time = time.time()
    fs.write_to_online_store("driver_zero_limit", df)
    elapsed_time = time.time() - start_time

    # Should be fast with rate_limit=0
    assert elapsed_time < 2.0


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["cassandra"])
def test_cassandra_config_level_rate_limiting(environment, universal_data_sources):
    """Test that Cassandra respects write_rate_limit from config."""
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return

    fs = environment.feature_store

    # Verify we're testing with Cassandra
    if not hasattr(fs.config.online_store, "write_rate_limit"):
        return

    # Set rate limit at config level
    original_rate_limit = fs.config.online_store.write_rate_limit
    fs.config.online_store.write_rate_limit = 15

    entities, datasets, data_sources = universal_data_sources

    # Feature view WITHOUT tag
    driver_fv = FeatureView(
        name="cassandra_config_rate_test",
        entities=[driver()],
        schema=[
            Field(name="trips", dtype=Int64),
            Field(name="rating", dtype=Float32),
        ],
        source=data_sources.driver,
        ttl=timedelta(days=1),
    )

    fs.apply([driver(), driver_fv])

    # Create 45 records
    df = pd.DataFrame(
        {
            "driver_id": list(range(7001, 7046)),
            "trips": list(range(45)),
            "rating": [4.5] * 45,
            "event_timestamp": [datetime.now()] * 45,
        }
    )

    # Measure write time
    start_time = time.time()
    fs.write_to_online_store("cassandra_config_rate_test", df)
    elapsed_time = time.time() - start_time

    # Restore original
    fs.config.online_store.write_rate_limit = original_rate_limit

    # Verify rate limiting: 45 records at 15/sec = ~3 seconds
    assert 2.5 <= elapsed_time <= 4.5


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["cassandra"])
def test_cassandra_tag_overrides_config(environment, universal_data_sources):
    """Test that feature view tag overrides Cassandra config."""
    if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
        return

    fs = environment.feature_store

    if not hasattr(fs.config.online_store, "write_rate_limit"):
        return

    # Set low config-level rate limit
    original_rate_limit = fs.config.online_store.write_rate_limit
    fs.config.online_store.write_rate_limit = 10

    entities, datasets, data_sources = universal_data_sources

    # Feature view with tag override
    driver_fv = FeatureView(
        name="cassandra_tag_override_test",
        entities=[driver()],
        schema=[Field(name="value", dtype=Int64)],
        source=data_sources.driver,
        ttl=timedelta(days=1),
        tags={"write_rate_limit": "50"},
    )

    fs.apply([driver(), driver_fv])

    df = pd.DataFrame(
        {
            "driver_id": list(range(8001, 8101)),
            "value": list(range(100)),
            "event_timestamp": [datetime.now()] * 100,
        }
    )

    start_time = time.time()
    fs.write_to_online_store("cassandra_tag_override_test", df)
    elapsed_time = time.time() - start_time

    # Restore original
    fs.config.online_store.write_rate_limit = original_rate_limit

    # At 50/sec (tag), should take ~2 seconds, NOT 10 seconds
    assert elapsed_time < 3.5
