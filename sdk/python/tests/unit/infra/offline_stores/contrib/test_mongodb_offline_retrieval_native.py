"""
Unit tests for MongoDB Native offline store implementation.

This tests the single-collection schema where all feature views share one
collection (``feature_history``), discriminated by ``feature_view`` field.

Schema:
    {
        "entity_id": bytes,  # serialized entity key
        "feature_view": str,
        "features": { "feat1": val, ... },
        "event_timestamp": datetime,
        "created_at": datetime
    }

Docker-dependent tests are marked with ``@_requires_docker`` and are skipped
when Docker is unavailable.
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
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_native import (
    MongoDBOfflineStoreNative,
    MongoDBOfflineStoreNativeConfig,
    MongoDBSourceNative,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Float64, Int64, String
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

ENTITY_KEY_VERSION = 3


def _make_entity_id(join_keys: dict) -> bytes:
    """Create serialized entity key from join key dict."""
    entity_key = EntityKeyProto()
    for key in sorted(join_keys.keys()):
        entity_key.join_keys.append(key)
        val = ValueProto()
        value = join_keys[key]
        if isinstance(value, int):
            val.int64_val = value
        elif isinstance(value, str):
            val.string_val = value
        else:
            val.string_val = str(value)
        entity_key.entity_values.append(val)
    return serialize_entity_key(entity_key, ENTITY_KEY_VERSION)


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
    """Create a RepoConfig with MongoDB Native offline store."""
    return RepoConfig(
        project="test_project",
        registry="memory://",
        provider="local",
        offline_store=MongoDBOfflineStoreNativeConfig(
            connection_string=mongodb_connection_string,
            database="feast_test",
            collection="feature_history",
        ),
        online_store={"type": "sqlite"},
        entity_key_serialization_version=ENTITY_KEY_VERSION,
    )


@pytest.fixture
def sample_data(mongodb_connection_string: str) -> datetime:
    """Insert sample data using the single-collection schema.

    Creates documents for 'driver_stats' feature view with entity_id,
    feature_view discriminator, and nested features subdocument.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]
    collection.drop()

    now = datetime.now(tz=pytz.UTC)

    # Create documents using the native schema
    docs = [
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats",
            "features": {"conv_rate": 0.5, "acc_rate": 0.9},
            "event_timestamp": now - timedelta(hours=2),
            "created_at": now - timedelta(hours=2),
        },
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats",
            "features": {"conv_rate": 0.6, "acc_rate": 0.85},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats",
            "features": {"conv_rate": 0.7, "acc_rate": 0.8},
            "event_timestamp": now,
            "created_at": now,
        },
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "driver_stats",
            "features": {"conv_rate": 0.3, "acc_rate": 0.95},
            "event_timestamp": now - timedelta(hours=2),
            "created_at": now - timedelta(hours=2),
        },
    ]
    collection.insert_many(docs)
    client.close()
    return now


@pytest.fixture
def driver_source() -> MongoDBSourceNative:
    """Create a MongoDBSourceNative for driver stats."""
    return MongoDBSourceNative(
        name="driver_stats",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_at",
    )


@pytest.fixture
def driver_fv(driver_source: MongoDBSourceNative) -> FeatureView:
    """Create a FeatureView for driver stats."""
    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    return FeatureView(
        name="driver_stats",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
            Field(name="acc_rate", dtype=Float64),
        ],
        source=driver_source,
        ttl=timedelta(days=1),
    )


@_requires_docker
def test_pull_latest_from_table_or_query(
    repo_config: RepoConfig, sample_data: datetime, driver_source: MongoDBSourceNative
) -> None:
    """Test pulling latest features per entity from the single collection."""
    now = sample_data
    job = MongoDBOfflineStoreNative.pull_latest_from_table_or_query(
        config=repo_config,
        data_source=driver_source,
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate", "acc_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column="created_at",
        start_date=now - timedelta(days=1),
        end_date=now + timedelta(hours=1),
    )

    df = job.to_df()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # Two unique entity_ids

    # Sort by entity_id for predictable assertions
    # Note: entity_id is bytes, so we check features directly
    conv_rates = sorted(df["conv_rate"].tolist())
    assert conv_rates[0] == pytest.approx(0.3)  # Driver 2's only value
    assert conv_rates[1] == pytest.approx(0.7)  # Driver 1's latest value


@_requires_docker
def test_get_historical_features_pit_join(
    repo_config: RepoConfig, sample_data: datetime, driver_fv: FeatureView
) -> None:
    """Test point-in-time join retrieves correct feature values."""
    now = sample_data

    # Entity dataframe with driver_id column (must match join keys)
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                now - timedelta(hours=1, minutes=30),  # Should get conv_rate=0.5
                now - timedelta(minutes=30),  # Should get conv_rate=0.6
                now - timedelta(hours=1),  # Should get conv_rate=0.3
            ],
        }
    )

    job = MongoDBOfflineStoreNative.get_historical_features(
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
    repo_config: RepoConfig, sample_data: datetime, driver_source: MongoDBSourceNative
) -> None:
    """Test pulling all features within a time range (no deduplication)."""
    now = sample_data
    job = MongoDBOfflineStoreNative.pull_all_from_table_or_query(
        config=repo_config,
        data_source=driver_source,
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate", "acc_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column="created_at",
        start_date=now - timedelta(hours=1, minutes=30),
        end_date=now + timedelta(hours=1),
    )

    df = job.to_df()
    assert isinstance(df, pd.DataFrame)
    # Should get 2 rows: driver 1 (1hr ago, now)
    # Excludes: driver 1 from 2 hours ago, driver 2 from 2 hours ago
    assert len(df) == 2


@_requires_docker
def test_ttl_excludes_stale_features(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Test that TTL causes stale feature values to be returned as NULL."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)

    # Insert docs with different ages
    ttl_docs = [
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats_ttl",
            "features": {"conv_rate": 0.9},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "driver_stats_ttl",
            "features": {"conv_rate": 0.5},
            "event_timestamp": now - timedelta(days=2),  # Stale
            "created_at": now - timedelta(days=2),
        },
    ]
    collection.insert_many(ttl_docs)
    client.close()

    ttl_source = MongoDBSourceNative(
        name="driver_stats_ttl",
        timestamp_field="event_timestamp",
    )
    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    ttl_fv = FeatureView(
        name="driver_stats_ttl",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
        ],
        source=ttl_source,
        ttl=timedelta(days=1),
    )

    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "event_timestamp": [now, now],
        }
    )

    job = MongoDBOfflineStoreNative.get_historical_features(
        config=repo_config,
        feature_views=[ttl_fv],
        feature_refs=["driver_stats_ttl:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)

    # Driver 1: fresh → has value
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.9)

    # Driver 2: stale → NULL
    assert pd.isna(result_df.loc[1, "conv_rate"])


@_requires_docker
def test_multiple_feature_views(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Test joining features from multiple feature views in the same collection."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)

    # Insert documents for two different feature views
    multi_docs = [
        # driver_stats_multi
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats_multi",
            "features": {"rating": 4.8},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "driver_stats_multi",
            "features": {"rating": 4.5},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        # vehicle_stats_multi
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "vehicle_stats_multi",
            "features": {"vehicle_age": 2, "mileage": 50000},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "vehicle_stats_multi",
            "features": {"vehicle_age": 5, "mileage": 120000},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
    ]
    collection.insert_many(multi_docs)
    client.close()

    # Create sources and feature views
    driver_source = MongoDBSourceNative(name="driver_stats_multi")
    vehicle_source = MongoDBSourceNative(name="vehicle_stats_multi")

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )

    driver_fv = FeatureView(
        name="driver_stats_multi",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="rating", dtype=Float64),
        ],
        source=driver_source,
        ttl=timedelta(days=1),
    )

    vehicle_fv = FeatureView(
        name="vehicle_stats_multi",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="vehicle_age", dtype=Int64),
            Field(name="mileage", dtype=Int64),
        ],
        source=vehicle_source,
        ttl=timedelta(days=1),
    )

    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "event_timestamp": [now, now],
        }
    )

    job = MongoDBOfflineStoreNative.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv, vehicle_fv],
        feature_refs=[
            "driver_stats_multi:rating",
            "vehicle_stats_multi:vehicle_age",
            "vehicle_stats_multi:mileage",
        ],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)

    assert len(result_df) == 2
    assert set(result_df.columns) >= {"driver_id", "rating", "vehicle_age", "mileage"}

    # Driver 1
    assert result_df.loc[0, "rating"] == pytest.approx(4.8)
    assert result_df.loc[0, "vehicle_age"] == 2
    assert result_df.loc[0, "mileage"] == 50000

    # Driver 2
    assert result_df.loc[1, "rating"] == pytest.approx(4.5)
    assert result_df.loc[1, "vehicle_age"] == 5
    assert result_df.loc[1, "mileage"] == 120000


@_requires_docker
def test_compound_join_keys(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Test with compound/composite join keys (multiple entity columns)."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)

    # Insert documents with compound keys (user_id + device_id)
    compound_docs = [
        {
            "entity_id": _make_entity_id({"user_id": 1, "device_id": "mobile"}),
            "feature_view": "user_device_features",
            "features": {"app_opens": 50},
            "event_timestamp": now - timedelta(hours=2),
            "created_at": now - timedelta(hours=2),
        },
        {
            "entity_id": _make_entity_id({"user_id": 1, "device_id": "mobile"}),
            "feature_view": "user_device_features",
            "features": {"app_opens": 55},  # Latest for this entity
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"user_id": 1, "device_id": "desktop"}),
            "feature_view": "user_device_features",
            "features": {"app_opens": 10},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"user_id": 2, "device_id": "tablet"}),
            "feature_view": "user_device_features",
            "features": {"app_opens": 25},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
    ]
    collection.insert_many(compound_docs)
    client.close()

    source = MongoDBSourceNative(name="user_device_features")

    user_entity = Entity(
        name="user_id", join_keys=["user_id"], value_type=ValueType.INT64
    )
    device_entity = Entity(
        name="device_id", join_keys=["device_id"], value_type=ValueType.STRING
    )

    fv = FeatureView(
        name="user_device_features",
        entities=[user_entity, device_entity],
        schema=[
            Field(name="user_id", dtype=Int64),
            Field(name="device_id", dtype=String),
            Field(name="app_opens", dtype=Int64),
        ],
        source=source,
        ttl=timedelta(days=1),
    )

    # Test pull_latest: should get one row per unique (user_id, device_id)
    job = MongoDBOfflineStoreNative.pull_latest_from_table_or_query(
        config=repo_config,
        data_source=source,
        join_key_columns=["user_id", "device_id"],
        feature_name_columns=["app_opens"],
        timestamp_field="event_timestamp",
        created_timestamp_column="created_at",
        start_date=now - timedelta(days=1),
        end_date=now + timedelta(hours=1),
    )

    df = job.to_df()
    assert len(df) == 3  # 3 unique (user_id, device_id) combinations

    # Verify we got the latest value (55) for user 1, mobile
    app_opens_values = sorted(df["app_opens"].tolist())
    assert 55 in app_opens_values  # Latest for user 1, mobile
    assert 10 in app_opens_values  # user 1, desktop
    assert 25 in app_opens_values  # user 2, tablet

    # Test get_historical_features with compound keys
    entity_df = pd.DataFrame(
        {
            "user_id": [1, 1, 2],
            "device_id": ["mobile", "desktop", "tablet"],
            "event_timestamp": [now, now, now],
        }
    )

    job = MongoDBOfflineStoreNative.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["user_device_features:app_opens"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df()
    assert len(result_df) == 3

    # Sort for predictable assertions
    result_df = result_df.sort_values(["user_id", "device_id"]).reset_index(drop=True)

    # user 1, desktop
    assert result_df.loc[0, "app_opens"] == 10
    # user 1, mobile (latest value)
    assert result_df.loc[1, "app_opens"] == 55
    # user 2, tablet
    assert result_df.loc[2, "app_opens"] == 25
