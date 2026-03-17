"""
Unit tests for MongoDB offline store (Ibis-based implementation).

Docker-dependent tests are marked with ``@_requires_docker`` and are skipped when
Docker is unavailable.
"""

from datetime import datetime, timedelta
from typing import Generator
from unittest.mock import MagicMock

import pandas as pd
import pytest
import pytz

pytest.importorskip("pymongo")

from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb import (
    MongoDBOfflineStoreIbis,
    MongoDBOfflineStoreIbisConfig,
)
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_source import (
    MongoDBSource,
)
from feast.repo_config import RepoConfig
from feast.types import Float64, Int64
from feast.value_type import ValueType

# Check if Docker is available
docker_available = False
try:
    import docker

    try:
        client = docker.from_env()
        client.ping()
        docker_available = True
    except Exception:
        pass
except ImportError:
    pass

_requires_docker = pytest.mark.skipif(
    not docker_available,
    reason="Docker is not available or not running.",
)


@pytest.fixture(scope="module")
def mongodb_container() -> Generator[MongoDbContainer, None, None]:
    """Start a MongoDB container for testing."""
    container = MongoDbContainer(
        "mongo:latest",
        username="test",
        password="test",  # pragma: allowlist secret
    ).with_exposed_ports(27017)
    container.start()
    yield container
    container.stop()


@pytest.fixture
def mongodb_connection_string(mongodb_container: MongoDbContainer) -> str:
    """Get MongoDB connection string from the container."""
    exposed_port = mongodb_container.get_exposed_port(27017)
    return f"mongodb://test:test@localhost:{exposed_port}"  # pragma: allowlist secret


@pytest.fixture
def repo_config(mongodb_connection_string: str) -> RepoConfig:
    """Create a RepoConfig with MongoDB offline store."""
    return RepoConfig(
        project="test_project",
        registry="memory://",
        provider="local",
        offline_store=MongoDBOfflineStoreIbisConfig(
            connection_string=mongodb_connection_string,
            database="feast_test",
        ),
        online_store={"type": "sqlite"},
        entity_key_serialization_version=3,
    )


@pytest.fixture
def sample_data(mongodb_connection_string: str) -> datetime:
    """Insert sample driver stats data into MongoDB.

    Returns the 'now' timestamp used as the latest event_timestamp.

    Note: The collection name 'driver_stats' is defined in the MongoDBSource
    (see driver_source fixture), not in the RepoConfig. RepoConfig provides
    connection_string and database; the source defines the collection.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["driver_stats"]
    collection.drop()

    now = datetime.now(tz=pytz.UTC)
    docs = [
        {
            "driver_id": 1,
            "conv_rate": 0.5,
            "acc_rate": 0.9,
            "event_timestamp": now - timedelta(hours=2),
        },
        {
            "driver_id": 1,
            "conv_rate": 0.6,
            "acc_rate": 0.85,
            "event_timestamp": now - timedelta(hours=1),
        },
        {"driver_id": 1, "conv_rate": 0.7, "acc_rate": 0.8, "event_timestamp": now},
        {
            "driver_id": 2,
            "conv_rate": 0.3,
            "acc_rate": 0.95,
            "event_timestamp": now - timedelta(hours=2),
        },
        # Driver 2 has no "now" timestamp - only data from 2 hours ago
        # This tests that pull_latest correctly handles entities with different latest timestamps
    ]
    collection.insert_many(docs)
    client.close()
    return now


@pytest.fixture
def driver_source() -> MongoDBSource:
    """Create a MongoDBSource for driver stats."""
    return MongoDBSource(
        name="driver_stats",
        database="feast_test",
        collection="driver_stats",
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def driver_fv(driver_source: MongoDBSource) -> FeatureView:
    """Create a FeatureView for driver stats.

    The ttl (time-to-live) parameter defines how far back in time Feast will look
    for feature values during point-in-time joins. If a feature's event_timestamp
    is older than (entity_timestamp - ttl), that feature value is considered stale
    and will be returned as NULL.

    This is different from MongoDB TTL indexes which automatically delete documents
    after a period of time. Feast TTL is a query-time filter, not a storage policy.
    """
    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    return FeatureView(
        name="driver_stats",
        entities=[driver_entity],
        schema=[
            # Include entity column in schema so entity_columns is populated
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
            Field(name="acc_rate", dtype=Float64),
        ],
        source=driver_source,
        ttl=timedelta(days=1),
    )


@_requires_docker
def test_pull_latest_from_table_or_query(
    repo_config: RepoConfig, sample_data: datetime, driver_source: MongoDBSource
) -> None:
    """Test pulling latest features per entity from MongoDB.

    This test verifies that pull_latest returns only the most recent feature
    values for each entity (driver_id), even when entities have different
    latest timestamps. Driver 1 has data at now, but driver 2's latest data
    is from 2 hours ago.
    """
    now = sample_data
    job = MongoDBOfflineStoreIbis.pull_latest_from_table_or_query(
        config=repo_config,
        data_source=driver_source,
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate", "acc_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        start_date=now - timedelta(days=1),
        end_date=now + timedelta(hours=1),
    )

    df = job.to_df()

    # Validate DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {"driver_id", "conv_rate", "acc_rate", "event_timestamp"}
    assert len(df) == 2  # Two unique drivers

    # Extract rows for each driver
    driver1_rows = df[df["driver_id"] == 1]
    driver2_rows = df[df["driver_id"] == 2]

    # Each driver should have exactly one row (the latest)
    assert len(driver1_rows) == 1
    assert len(driver2_rows) == 1

    driver1 = driver1_rows.iloc[0]
    driver2 = driver2_rows.iloc[0]

    # Validate types
    assert isinstance(driver1["conv_rate"], float)
    assert isinstance(driver1["acc_rate"], float)

    # Driver 1's latest values (from "now")
    assert driver1["conv_rate"] == pytest.approx(0.7)
    assert driver1["acc_rate"] == pytest.approx(0.8)

    # Driver 2's latest values (from 2 hours ago - driver 2 has no "now" data)
    # This demonstrates that pull_latest correctly handles entities with
    # different "latest" timestamps
    assert driver2["conv_rate"] == pytest.approx(0.3)
    assert driver2["acc_rate"] == pytest.approx(0.95)


@_requires_docker
def test_get_historical_features_pit_join(
    repo_config: RepoConfig, sample_data: datetime, driver_fv: FeatureView
) -> None:
    """Test point-in-time join retrieves correct feature values.

    Point-in-time (PIT) join ensures that for each entity row, we get the
    feature values that were valid AT THAT POINT IN TIME - not future data
    that would cause data leakage in ML training.
    """
    now = sample_data

    # Entity dataframe: request features at specific timestamps
    # Each row says "give me driver X's features as they were at time T"
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                now
                - timedelta(
                    hours=1, minutes=30
                ),  # Should get conv_rate=0.5 (before 0.6 was written)
                now
                - timedelta(
                    minutes=30
                ),  # Should get conv_rate=0.6 (before 0.7 was written)
                now
                - timedelta(hours=1),  # Should get conv_rate=0.3 (only data available)
            ],
        }
    )

    job = MongoDBOfflineStoreIbis.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df()
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 3

    # Sort by driver_id and event_timestamp for predictable assertions
    result_df = result_df.sort_values(["driver_id", "event_timestamp"]).reset_index(
        drop=True
    )

    # Driver 1, first request (1.5 hours ago) → should get value from 2 hours ago
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.5)

    # Driver 1, second request (30 min ago) → should get value from 1 hour ago
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.6)

    # Driver 2, request (1 hour ago) → should get value from 2 hours ago
    assert result_df.loc[2, "conv_rate"] == pytest.approx(0.3)


@_requires_docker
def test_pull_all_from_table_or_query(
    repo_config: RepoConfig, sample_data: datetime, driver_source: MongoDBSource
) -> None:
    """Test pulling all features within a time range (no deduplication)."""
    now = sample_data
    job = MongoDBOfflineStoreIbis.pull_all_from_table_or_query(
        config=repo_config,
        data_source=driver_source,
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate", "acc_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        start_date=now - timedelta(hours=1, minutes=30),
        end_date=now + timedelta(hours=1),
    )

    df = job.to_df()
    assert isinstance(df, pd.DataFrame)
    # Should get 2 rows: driver 1 (1hr ago, now)
    # Excludes: driver 1 row from 2 hours ago (before start_date)
    #           driver 2 row from 2 hours ago (before start_date)
    assert len(df) == 2


@_requires_docker
def test_ttl_excludes_stale_features(
    repo_config: RepoConfig,
    mongodb_connection_string: str,
    driver_source: MongoDBSource,
) -> None:
    """Test that TTL causes stale feature values to be returned as NULL.

    Feast TTL (time-to-live) is a query-time filter: if a feature's event_timestamp
    is older than (entity_timestamp - ttl), that feature is considered stale.
    This is different from MongoDB TTL indexes which delete documents.
    """
    # Insert data with a very old timestamp
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["driver_stats_ttl_test"]
    collection.drop()

    now = datetime.now(tz=pytz.UTC)
    docs = [
        # Fresh data (within TTL)
        {"driver_id": 1, "conv_rate": 0.9, "event_timestamp": now - timedelta(hours=1)},
        # Stale data (outside 1-day TTL when queried from "now")
        {"driver_id": 2, "conv_rate": 0.5, "event_timestamp": now - timedelta(days=2)},
    ]
    collection.insert_many(docs)
    client.close()

    # Create source and feature view with 1-day TTL
    ttl_source = MongoDBSource(
        name="driver_stats_ttl_test",
        database="feast_test",
        collection="driver_stats_ttl_test",
        timestamp_field="event_timestamp",
    )
    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    ttl_fv = FeatureView(
        name="driver_stats_ttl_test",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
        ],
        source=ttl_source,
        ttl=timedelta(days=1),  # Features older than 1 day are stale
    )

    # Request features "as of now" for both drivers
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "event_timestamp": [now, now],
        }
    )

    job = MongoDBOfflineStoreIbis.get_historical_features(
        config=repo_config,
        feature_views=[ttl_fv],
        feature_refs=["driver_stats_ttl_test:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)

    # Driver 1: fresh data within TTL → should have value
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.9)

    # Driver 2: stale data outside TTL → should be NULL
    assert pd.isna(result_df.loc[1, "conv_rate"])
