"""
Unit tests for MongoDB offline store (mongodb.py).

Tests the single-collection schema with grouped aggregation.  All feature
views share one collection (``feature_history``) discriminated by the
``feature_view`` field.

Schema:
    {
        "entity_id":       bytes,  # serialized entity key
        "feature_view":    str,    # discriminator
        "features":        {"feat1": val, ...},
        "event_timestamp": datetime,
        "created_at":      datetime
    }

Docker-dependent tests are marked with ``@_requires_docker`` and are skipped
when Docker is unavailable.
"""

import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Generator, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow
import pytest
import pytz

pytest.importorskip("pymongo")

from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from feast import Entity, FeatureView, Field
from feast.errors import SavedDatasetLocationAlreadyExists
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb import (
    MongoDBOfflineStore,
    MongoDBOfflineStoreConfig,
    MongoDBSource,
)
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Float64, Int32, Int64, String
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# Docker availability check
# ---------------------------------------------------------------------------

docker_available = False
try:
    import docker

    try:
        _docker_client = docker.from_env()
        _docker_client.ping()
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entity_id(
    join_keys: dict,
    value_types: Optional[Dict[str, ValueType]] = None,
) -> bytes:
    """Serialize an entity key dict to bytes (matches offline_write_batch serialization)."""
    entity_key = EntityKeyProto()
    for key in sorted(join_keys.keys()):
        entity_key.join_keys.append(key)
        val = ValueProto()
        value = join_keys[key]
        declared_type = value_types.get(key) if value_types else None
        if declared_type == ValueType.INT32:
            val.int32_val = int(value)
        elif declared_type == ValueType.INT64:
            val.int64_val = int(value)
        elif declared_type == ValueType.STRING:
            val.string_val = str(value)
        else:
            if isinstance(value, bool):
                val.bool_val = value
            elif isinstance(value, int):
                val.int64_val = value
            elif isinstance(value, str):
                val.string_val = value
            else:
                val.string_val = str(value)
        entity_key.entity_values.append(val)
    return serialize_entity_key(entity_key, ENTITY_KEY_VERSION)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mongodb_container() -> Generator[MongoDbContainer, None, None]:
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
    exposed_port = mongodb_container.get_exposed_port(27017)
    return f"mongodb://test:test@localhost:{exposed_port}"  # pragma: allowlist secret


@pytest.fixture
def repo_config(mongodb_connection_string: str) -> RepoConfig:
    return RepoConfig(
        project="test_project",
        registry="memory://",
        provider="local",
        offline_store=MongoDBOfflineStoreConfig(
            connection_string=mongodb_connection_string,
            database="feast_test",
            collection="feature_history",
        ),
        online_store={"type": "sqlite"},
        entity_key_serialization_version=ENTITY_KEY_VERSION,
    )


@pytest.fixture
def sample_data(mongodb_connection_string: str) -> datetime:
    """Insert sample data using the feature_history schema.

    4 documents: driver 1 at 3 timestamps, driver 2 at 1 timestamp.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]
    collection.drop()

    now = datetime.now(tz=pytz.UTC)

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
def driver_source() -> MongoDBSource:
    return MongoDBSource(
        name="driver_stats",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_at",
    )


@pytest.fixture
def driver_fv(driver_source: MongoDBSource) -> FeatureView:
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


# ---------------------------------------------------------------------------
# Tests: pull_latest_from_table_or_query
# ---------------------------------------------------------------------------


@_requires_docker
def test_pull_latest_from_table_or_query(
    repo_config: RepoConfig, sample_data: datetime, driver_source: MongoDBSource
) -> None:
    """pull_latest returns the most recent row per entity within the time window."""
    now = sample_data
    job = MongoDBOfflineStore.pull_latest_from_table_or_query(
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
    assert len(df) == 2  # Two unique entities

    # Join key columns must be present in the output.
    assert "driver_id" in df.columns, (
        f"Expected 'driver_id' in output columns, got {list(df.columns)}"
    )

    conv_rates = sorted(df["conv_rate"].tolist())
    assert conv_rates[0] == pytest.approx(0.3)  # Driver 2's only value
    assert conv_rates[1] == pytest.approx(0.7)  # Driver 1's latest


# ---------------------------------------------------------------------------
# Tests: get_historical_features (training path — repeated entity IDs)
# ---------------------------------------------------------------------------


@_requires_docker
def test_get_historical_features_training_path(
    repo_config: RepoConfig, sample_data: datetime, driver_fv: FeatureView
) -> None:
    """Training path: repeated entity IDs at different timestamps → merge_asof PIT join."""
    now = sample_data

    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                now - timedelta(hours=1, minutes=30),  # → conv_rate=0.5 (doc 2h ago)
                now - timedelta(minutes=30),  # → conv_rate=0.6 (doc 1h ago)
                now - timedelta(hours=1),  # → conv_rate=0.3 (only doc)
            ],
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
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

    result_df = result_df.sort_values(["driver_id", "event_timestamp"]).reset_index(
        drop=True
    )

    # Driver 1, 1.5 hours ago → feature row from 2 hours ago
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.5)
    # Driver 1, 30 min ago → feature row from 1 hour ago
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.6)
    # Driver 2, 1 hour ago → only feature row (2 hours ago)
    assert result_df.loc[2, "conv_rate"] == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# Tests: get_historical_features (scoring path — unique entity IDs)
# ---------------------------------------------------------------------------


@_requires_docker
def test_get_historical_features_scoring_path(
    repo_config: RepoConfig, sample_data: datetime, driver_fv: FeatureView
) -> None:
    """Scoring path: unique entity IDs → server-side $group dedup, vectorized join."""
    now = sample_data

    # Unique entity IDs → scoring path uses $group
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "event_timestamp": [now, now],
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)
    assert len(result_df) == 2

    # Driver 1: latest value at or before now
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.7)
    assert result_df.loc[0, "acc_rate"] == pytest.approx(0.8)

    # Driver 2: only value (2 hours ago)
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.3)
    assert result_df.loc[1, "acc_rate"] == pytest.approx(0.95)


# ---------------------------------------------------------------------------
# Tests: pull_all_from_table_or_query
# ---------------------------------------------------------------------------


@_requires_docker
def test_pull_all_from_table_or_query(
    repo_config: RepoConfig, sample_data: datetime, driver_source: MongoDBSource
) -> None:
    """pull_all returns all rows in the time window without deduplication."""
    now = sample_data
    job = MongoDBOfflineStore.pull_all_from_table_or_query(
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
    # 2 rows in window: driver 1 at 1h ago and driver 1 at now.
    # Excludes: driver 1 at 2h ago (before start_date) and driver 2 at 2h ago.
    assert len(df) == 2

    # Join key columns must be present in the output.
    assert "driver_id" in df.columns, (
        f"Expected 'driver_id' in output columns, got {list(df.columns)}"
    )


# ---------------------------------------------------------------------------
# Tests: TTL
# ---------------------------------------------------------------------------


@_requires_docker
def test_ttl_excludes_stale_features(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """TTL causes stale feature values to be returned as NULL."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)

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

    ttl_source = MongoDBSource(
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

    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    job = MongoDBOfflineStore.get_historical_features(
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

    # Driver 2: stale (2 days ago, TTL=1 day) → NULL
    assert pd.isna(result_df.loc[1, "conv_rate"])


# ---------------------------------------------------------------------------
# Tests: K-collapse (multiple FVs, same join key signature)
# ---------------------------------------------------------------------------


@_requires_docker
def test_k_collapse_multiple_feature_views(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """K=2 FVs sharing the same join key are resolved in a single aggregation group."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)

    docs = [
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats_k",
            "features": {"rating": 4.8},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "driver_stats_k",
            "features": {"rating": 4.5},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "vehicle_stats_k",
            "features": {"vehicle_age": 2, "mileage": 50000},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "vehicle_stats_k",
            "features": {"vehicle_age": 5, "mileage": 120000},
            "event_timestamp": now - timedelta(hours=1),
            "created_at": now - timedelta(hours=1),
        },
    ]
    collection.insert_many(docs)
    client.close()

    driver_source_k = MongoDBSource(name="driver_stats_k")
    vehicle_source_k = MongoDBSource(name="vehicle_stats_k")

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )

    driver_fv_k = FeatureView(
        name="driver_stats_k",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="rating", dtype=Float64),
        ],
        source=driver_source_k,
        ttl=timedelta(days=1),
    )

    vehicle_fv_k = FeatureView(
        name="vehicle_stats_k",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="vehicle_age", dtype=Int64),
            Field(name="mileage", dtype=Int64),
        ],
        source=vehicle_source_k,
        ttl=timedelta(days=1),
    )

    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv_k, vehicle_fv_k],
        feature_refs=[
            "driver_stats_k:rating",
            "vehicle_stats_k:vehicle_age",
            "vehicle_stats_k:mileage",
        ],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)

    assert len(result_df) == 2
    assert set(result_df.columns) >= {"driver_id", "rating", "vehicle_age", "mileage"}

    assert result_df.loc[0, "rating"] == pytest.approx(4.8)
    assert result_df.loc[0, "vehicle_age"] == 2
    assert result_df.loc[0, "mileage"] == 50000

    assert result_df.loc[1, "rating"] == pytest.approx(4.5)
    assert result_df.loc[1, "vehicle_age"] == 5
    assert result_df.loc[1, "mileage"] == 120000


# ---------------------------------------------------------------------------
# Tests: mixed join key cardinality (scoring_path per-FV)
# ---------------------------------------------------------------------------


@_requires_docker
def test_mixed_join_key_cardinality(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """FVs with different join key sets must not lose data via $group.

    FV_A uses (user_id), FV_B uses (user_id, device_id).
    entity_df has two rows: (user=1, device=X, t=09:00) and (user=1, device=Y, t=10:00).
    Unique on (user_id, device_id), but user_id=1 appears twice.

    For FV_A, both rows map to the same entity_id.  The old code would use
    the scoring path ($group) globally, discarding older docs for user_id=1.
    The fix detects that FV_A's entity_id is non-unique and falls back to
    merge_asof for FV_A while FV_B can still use the scoring path.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)
    t_0800 = now - timedelta(hours=2)  # 08:00
    t_0900 = now - timedelta(hours=1)  # 09:00
    t_0930 = now - timedelta(minutes=30)  # 09:30

    user_entity = Entity(
        name="user_id", join_keys=["user_id"], value_type=ValueType.INT64
    )
    device_entity = Entity(
        name="device_id", join_keys=["device_id"], value_type=ValueType.STRING
    )

    # FV_A: keyed on user_id only
    source_a = MongoDBSource(name="user_prefs")
    fv_a = FeatureView(
        name="user_prefs",
        entities=[user_entity],
        schema=[
            Field(name="user_id", dtype=Int64),
            Field(name="theme", dtype=String),
        ],
        source=source_a,
        ttl=timedelta(days=1),
    )

    # FV_B: keyed on (user_id, device_id)
    source_b = MongoDBSource(name="device_stats")
    fv_b = FeatureView(
        name="device_stats",
        entities=[user_entity, device_entity],
        schema=[
            Field(name="user_id", dtype=Int64),
            Field(name="device_id", dtype=String),
            Field(name="battery", dtype=Float64),
        ],
        source=source_b,
        ttl=timedelta(days=1),
    )

    # Insert docs for FV_A (user_id only key)
    # Two docs for user 1: one at 08:00 ("dark") and one at 09:30 ("light")
    docs = [
        {
            "entity_id": _make_entity_id({"user_id": 1}),
            "feature_view": "user_prefs",
            "features": {"theme": "dark"},
            "event_timestamp": t_0800,
            "created_at": t_0800,
        },
        {
            "entity_id": _make_entity_id({"user_id": 1}),
            "feature_view": "user_prefs",
            "features": {"theme": "light"},
            "event_timestamp": t_0930,
            "created_at": t_0930,
        },
        # FV_B: keyed on (user_id, device_id)
        {
            "entity_id": _make_entity_id({"device_id": "X", "user_id": 1}),
            "feature_view": "device_stats",
            "features": {"battery": 0.8},
            "event_timestamp": t_0800,
            "created_at": t_0800,
        },
        {
            "entity_id": _make_entity_id({"device_id": "Y", "user_id": 1}),
            "feature_view": "device_stats",
            "features": {"battery": 0.6},
            "event_timestamp": t_0800,
            "created_at": t_0800,
        },
    ]
    collection.insert_many(docs)
    client.close()

    # entity_df: two rows, unique on (user_id, device_id) but NOT on user_id
    entity_df = pd.DataFrame(
        {
            "user_id": [1, 1],
            "device_id": ["X", "Y"],
            "event_timestamp": [t_0900, now],  # 09:00 and 10:00
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[fv_a, fv_b],
        feature_refs=["user_prefs:theme", "device_stats:battery"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
        strict_pit=True,
    )

    result_df = job.to_df().sort_values("device_id").reset_index(drop=True)

    assert len(result_df) == 2

    # Row 0: device=X, request at 09:00
    # FV_A (user_id=1): 09:30 doc is AFTER 09:00 (strict_pit), 08:00 doc is valid → "dark"
    # FV_B (user=1, device=X): 08:00 doc is before 09:00 → battery=0.8
    assert result_df.loc[0, "theme"] == "dark", (
        f"Expected 'dark' for row at 09:00 but got {result_df.loc[0, 'theme']!r}. "
        "The scoring path $group may have discarded the 08:00 doc."
    )
    assert result_df.loc[0, "battery"] == pytest.approx(0.8)

    # Row 1: device=Y, request at 10:00
    # FV_A (user_id=1): 09:30 doc is before 10:00 → "light"
    # FV_B (user=1, device=Y): 08:00 doc is before 10:00 → battery=0.6
    assert result_df.loc[1, "theme"] == "light"
    assert result_df.loc[1, "battery"] == pytest.approx(0.6)


# ---------------------------------------------------------------------------
# Tests: heterogeneous timestamps prevent scoring path
# ---------------------------------------------------------------------------


@_requires_docker
def test_heterogeneous_timestamps_fall_back_to_training_path(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Unique entity IDs but different request timestamps must use training path.

    Entity A requests at 09:00, Entity B at 10:00.
    Doc for A: 08:00 (valid) and 09:30 (future for A's request).
    Doc for B: 09:00 (valid).

    If the scoring path were used, $group with $lte max_ts=10:00 would pick
    the 09:30 doc for A.  future_mask would null it, but the valid 08:00 doc
    was already discarded.  The training path (merge_asof) correctly returns
    the 08:00 doc for A.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)
    t_0800 = now - timedelta(hours=2)
    t_0900 = now - timedelta(hours=1)
    t_0930 = now - timedelta(minutes=30)

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    source = MongoDBSource(name="driver_stats_het")
    fv = FeatureView(
        name="driver_stats_het",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
        ],
        source=source,
        ttl=timedelta(days=1),
    )

    docs = [
        # Driver 1: valid doc at 08:00
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats_het",
            "features": {"conv_rate": 0.5},
            "event_timestamp": t_0800,
            "created_at": t_0800,
        },
        # Driver 1: doc at 09:30 (future relative to driver 1's request at 09:00)
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats_het",
            "features": {"conv_rate": 0.9},
            "event_timestamp": t_0930,
            "created_at": t_0930,
        },
        # Driver 2: valid doc at 09:00
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "driver_stats_het",
            "features": {"conv_rate": 0.7},
            "event_timestamp": t_0900,
            "created_at": t_0900,
        },
    ]
    collection.insert_many(docs)
    client.close()

    # Different request timestamps: driver 1 at 09:00, driver 2 at 10:00
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "event_timestamp": [t_0900, now],
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["driver_stats_het:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
        strict_pit=True,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)
    assert len(result_df) == 2

    # Driver 1: request at 09:00, 09:30 doc is future → must return 08:00 doc (0.5)
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.5), (
        f"Expected 0.5 for driver 1 (08:00 doc) but got "
        f"{result_df.loc[0, 'conv_rate']!r}. The scoring path may have "
        f"discarded the valid older doc via $group."
    )

    # Driver 2: request at 10:00, 09:00 doc is valid → 0.7
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# Tests: overlapping feature names
# ---------------------------------------------------------------------------


@_requires_docker
def test_overlapping_feature_names_full_feature_names(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """full_feature_names=True prevents collision when multiple FVs share a feature name."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)

    docs = []
    for fv_name, score_val in [
        ("fv_overlap_a", 1.0),
        ("fv_overlap_b", 2.0),
        ("fv_overlap_c", 3.0),
    ]:
        for driver_id in [1, 2]:
            docs.append(
                {
                    "entity_id": _make_entity_id({"driver_id": driver_id}),
                    "feature_view": fv_name,
                    "features": {"score": score_val + driver_id * 0.1},
                    "event_timestamp": now - timedelta(hours=1),
                    "created_at": now - timedelta(hours=1),
                }
            )
    collection.insert_many(docs)
    client.close()

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    fvs = []
    feature_refs = []
    for fv_name in ["fv_overlap_a", "fv_overlap_b", "fv_overlap_c"]:
        source = MongoDBSource(name=fv_name)
        fv = FeatureView(
            name=fv_name,
            entities=[driver_entity],
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="score", dtype=Float64),
            ],
            source=source,
            ttl=timedelta(days=1),
        )
        fvs.append(fv)
        feature_refs.append(f"{fv_name}:score")

    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=fvs,
        feature_refs=feature_refs,
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=True,
    )
    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)

    assert len(result_df) == 2
    assert "fv_overlap_a__score" in result_df.columns
    assert "fv_overlap_b__score" in result_df.columns
    assert "fv_overlap_c__score" in result_df.columns

    assert result_df.loc[0, "fv_overlap_a__score"] == pytest.approx(1.1)
    assert result_df.loc[0, "fv_overlap_b__score"] == pytest.approx(2.1)
    assert result_df.loc[0, "fv_overlap_c__score"] == pytest.approx(3.1)
    assert result_df.loc[1, "fv_overlap_a__score"] == pytest.approx(1.2)
    assert result_df.loc[1, "fv_overlap_b__score"] == pytest.approx(2.2)
    assert result_df.loc[1, "fv_overlap_c__score"] == pytest.approx(3.2)


# ---------------------------------------------------------------------------
# Tests: compound join keys
# ---------------------------------------------------------------------------


@_requires_docker
def test_compound_join_keys(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Compound join keys (user_id + device_id) are serialized and matched correctly."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]

    now = datetime.now(tz=pytz.UTC)

    docs = [
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
            "features": {"app_opens": 55},  # Latest
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
    collection.insert_many(docs)
    client.close()

    source = MongoDBSource(name="user_device_features")
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

    # pull_latest: one row per unique (user_id, device_id)
    job = MongoDBOfflineStore.pull_latest_from_table_or_query(
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
    assert len(df) == 3

    app_opens_values = sorted(df["app_opens"].tolist())
    assert 10 in app_opens_values
    assert 25 in app_opens_values
    assert 55 in app_opens_values

    # get_historical_features with compound keys
    entity_df = pd.DataFrame(
        {
            "user_id": [1, 1, 2],
            "device_id": ["mobile", "desktop", "tablet"],
            "event_timestamp": [now, now, now],
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
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

    result_df = result_df.sort_values(["user_id", "device_id"]).reset_index(drop=True)

    assert result_df.loc[0, "app_opens"] == 10  # user 1, desktop
    assert result_df.loc[1, "app_opens"] == 55  # user 1, mobile (latest)
    assert result_df.loc[2, "app_opens"] == 25  # user 2, tablet


# ---------------------------------------------------------------------------
# Tests: extra columns in entity_df
# ---------------------------------------------------------------------------


@_requires_docker
def test_entity_df_with_extra_columns(
    repo_config: RepoConfig, sample_data: datetime, driver_fv: FeatureView
) -> None:
    """Label columns in entity_df must not corrupt entity key serialization."""
    now = sample_data

    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                now - timedelta(hours=1, minutes=30),
                now - timedelta(minutes=30),
                now - timedelta(hours=1),
            ],
            "trip_success": [1, 0, 1],  # label — must not enter entity key
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
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
    assert "trip_success" in result_df.columns
    assert sorted(result_df["trip_success"].tolist()) == [0, 1, 1]

    assert result_df["conv_rate"].notna().all(), (
        "conv_rate is null — label column was likely included in entity key serialization."
    )

    result_df = result_df.sort_values(["driver_id", "event_timestamp"]).reset_index(
        drop=True
    )
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.5)
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.6)
    assert result_df.loc[2, "conv_rate"] == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# Tests: INT32 entity key
# ---------------------------------------------------------------------------


@_requires_docker
def test_int32_entity_key(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """INT32 entity keys use int32_val in the proto, not int64_val.

    offline_write_batch must serialize INT32 entities with int32_val so that
    the bytes match what get_historical_features produces.
    """
    driver_entity = Entity(
        name="order_id", join_keys=["order_id"], value_type=ValueType.INT32
    )
    source = MongoDBSource(name="order_features")
    fv = FeatureView(
        name="order_features",
        entities=[driver_entity],
        schema=[
            Field(name="order_id", dtype=Int32),
            Field(name="amount", dtype=Float64),
        ],
        source=source,
        ttl=timedelta(days=1),
    )

    client: MongoClient = MongoClient(mongodb_connection_string)
    client["feast_test"]["feature_history"].drop()
    client.close()

    now = datetime.now(tz=pytz.UTC)

    # Write via offline_write_batch (uses mongodb.py's serializer)
    df = pd.DataFrame(
        {
            "order_id": [1, 2],
            "amount": [100.0, 200.0],
            "event_timestamp": [now - timedelta(hours=1), now - timedelta(hours=1)],
            "created_at": [now, now],
        }
    )
    table = pyarrow.Table.from_pandas(df)

    MongoDBOfflineStore.offline_write_batch(
        config=repo_config,
        feature_view=fv,
        table=table,
        progress=None,
    )

    entity_df = pd.DataFrame(
        {
            "order_id": [1, 2],
            "event_timestamp": [now, now],
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["order_features:amount"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("order_id").reset_index(drop=True)
    assert len(result_df) == 2
    assert result_df["amount"].notna().all(), (
        "amount is null — INT32 entity key serialization mismatch between "
        "offline_write_batch and get_historical_features."
    )
    assert result_df.loc[0, "amount"] == pytest.approx(100.0)
    assert result_df.loc[1, "amount"] == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# Tests: persist
# ---------------------------------------------------------------------------


@_requires_docker
def test_persist_writes_parquet(
    repo_config: RepoConfig,
    sample_data: datetime,
    driver_fv: FeatureView,
) -> None:
    """persist() writes the joined result to a parquet file."""
    now = sample_data

    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "event_timestamp": [now, now - timedelta(hours=2)],
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        path = f.name

    try:
        os.unlink(path)  # persist creates the file; it must not already exist
        job.persist(SavedDatasetFileStorage(path=path))

        assert os.path.exists(path)
        result_df = pd.read_parquet(path)
        assert len(result_df) == 2
        assert {"driver_id", "conv_rate", "acc_rate", "event_timestamp"}.issubset(
            result_df.columns
        )
    finally:
        if os.path.exists(path):
            os.unlink(path)


@_requires_docker
def test_persist_raises_if_file_exists(
    repo_config: RepoConfig,
    sample_data: datetime,
    driver_fv: FeatureView,
) -> None:
    """persist() raises SavedDatasetLocationAlreadyExists when the file already exists."""
    now = sample_data

    entity_df = pd.DataFrame({"driver_id": [1], "event_timestamp": [now]})

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        path = f.name

    try:
        with pytest.raises(SavedDatasetLocationAlreadyExists):
            job.persist(SavedDatasetFileStorage(path=path), allow_overwrite=False)
    finally:
        if os.path.exists(path):
            os.unlink(path)


@_requires_docker
def test_persist_allow_overwrite(
    repo_config: RepoConfig,
    sample_data: datetime,
    driver_fv: FeatureView,
) -> None:
    """persist(allow_overwrite=True) replaces any existing file."""
    now = sample_data

    entity_df = pd.DataFrame({"driver_id": [1], "event_timestamp": [now]})

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        path = f.name
        f.write(b"stale content")

    try:
        job.persist(SavedDatasetFileStorage(path=path), allow_overwrite=True)

        result_df = pd.read_parquet(path)
        assert len(result_df) == 1
        assert "driver_id" in result_df.columns
    finally:
        if os.path.exists(path):
            os.unlink(path)


# ---------------------------------------------------------------------------
# Tests: offline_write_batch round-trip
# ---------------------------------------------------------------------------


@_requires_docker
def test_offline_write_batch_round_trip(
    repo_config: RepoConfig,
    mongodb_connection_string: str,
    driver_fv: FeatureView,
) -> None:
    """offline_write_batch writes documents that get_historical_features reads back."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    client["feast_test"]["feature_history"].drop()
    client.close()

    now = datetime.now(tz=pytz.UTC)

    df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "conv_rate": [0.8, 0.6],
            "acc_rate": [0.9, 0.7],
            "event_timestamp": [now - timedelta(hours=1), now - timedelta(hours=1)],
            "created_at": [now, now],
        }
    )
    table = pyarrow.Table.from_pandas(df)

    MongoDBOfflineStore.offline_write_batch(
        config=repo_config,
        feature_view=driver_fv,
        table=table,
        progress=None,
    )

    # Verify document structure
    client = MongoClient(mongodb_connection_string)
    docs = list(
        client["feast_test"]["feature_history"].find(
            {"feature_view": "driver_stats"}, {"_id": 0}
        )
    )
    client.close()

    assert len(docs) == 2
    doc = docs[0]
    assert isinstance(doc["entity_id"], bytes), "entity_id must be serialized bytes"
    assert doc["feature_view"] == "driver_stats"
    assert set(doc["features"].keys()) == {"conv_rate", "acc_rate"}
    assert "event_timestamp" in doc
    assert "created_at" in doc

    # Round-trip: read back via get_historical_features
    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)
    assert len(result_df) == 2
    assert result_df["conv_rate"].notna().all(), (
        "conv_rate is null — offline_write_batch and get_historical_features "
        "produced different entity_id bytes."
    )
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.8)
    assert result_df.loc[0, "acc_rate"] == pytest.approx(0.9)
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.6)
    assert result_df.loc[1, "acc_rate"] == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# Tests: max_ts overshoot (strict_pit semantics)
# ---------------------------------------------------------------------------


@_requires_docker
def test_scoring_path_nulls_future_doc(
    repo_config: RepoConfig, mongodb_connection_string: str, driver_fv: FeatureView
) -> None:
    """strict_pit=True: a doc whose timestamp is after the entity request time → NULL.

    Scenario:
        Entity A requests features at 09:00.
        The only available document is at 10:30 (after the request time).
        With strict_pit=True the scoring path must return NULL for entity A.

    This exercises the ``future_mask`` in the scoring path.  The aggregation
    uses ``max_ts`` as the server-side upper bound, so the 10:30 doc IS
    fetched from MongoDB.  The Python-side ``future_mask`` is what nulls it.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]
    collection.drop()

    base = datetime.now(tz=pytz.UTC).replace(microsecond=0)
    request_a = base.replace(hour=9, minute=0, second=0)
    request_b = base.replace(hour=11, minute=0, second=0)
    doc_ts_a = base.replace(hour=10, minute=30, second=0)  # after A's request time
    doc_ts_b = base.replace(hour=10, minute=0, second=0)  # before B's request time

    # Replace with fixed dates to avoid day-boundary issues
    from datetime import timezone

    now = datetime.now(tz=timezone.utc)
    request_a = now - timedelta(hours=3)  # 3 hours ago
    request_b = now  # now
    doc_ts_a = now - timedelta(
        hours=1
    )  # 1 hour ago — AFTER request_a, BEFORE request_b
    doc_ts_b = now - timedelta(hours=2)  # 2 hours ago — before both requests

    docs = [
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats_pit",
            "features": {"conv_rate": 0.9},
            "event_timestamp": doc_ts_a,
            "created_at": doc_ts_a,
        },
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "driver_stats_pit",
            "features": {"conv_rate": 0.5},
            "event_timestamp": doc_ts_b,
            "created_at": doc_ts_b,
        },
    ]
    collection.insert_many(docs)
    client.close()

    pit_source = MongoDBSource(
        name="driver_stats_pit", timestamp_field="event_timestamp"
    )
    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    pit_fv = FeatureView(
        name="driver_stats_pit",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
        ],
        source=pit_source,
        ttl=timedelta(days=7),
    )

    # Unique entity IDs → scoring path
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "event_timestamp": [request_a, request_b],
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[pit_fv],
        feature_refs=["driver_stats_pit:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
        strict_pit=True,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)
    assert len(result_df) == 2

    # Driver 1: only doc is at doc_ts_a which is AFTER request_a → NULL
    assert pd.isna(result_df.loc[0, "conv_rate"]), (
        f"Expected NULL for driver 1 (doc at {doc_ts_a} is after request at "
        f"{request_a}), got {result_df.loc[0, 'conv_rate']!r}"
    )

    # Driver 2: doc is at doc_ts_b which is BEFORE request_b → value returned
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.5)


@_requires_docker
def test_scoring_path_nulls_future_doc_chunk_size_1(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Same as test_scoring_path_nulls_future_doc but with CHUNK_SIZE=1.

    With CHUNK_SIZE=1 each entity is processed in its own chunk.  Per-chunk
    ``max_ts`` equals that entity's own request timestamp, so the 10:30 doc
    for entity A is never even fetched from MongoDB (server-side filter).
    Either way, the result for entity A must be NULL and entity B must have
    a value.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]
    collection.drop()

    from datetime import timezone

    now = datetime.now(tz=timezone.utc)
    request_a = now - timedelta(hours=3)
    request_b = now
    doc_ts_a = now - timedelta(hours=1)  # after request_a
    doc_ts_b = now - timedelta(hours=2)  # before both

    docs = [
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats_pit2",
            "features": {"conv_rate": 0.9},
            "event_timestamp": doc_ts_a,
            "created_at": doc_ts_a,
        },
        {
            "entity_id": _make_entity_id({"driver_id": 2}),
            "feature_view": "driver_stats_pit2",
            "features": {"conv_rate": 0.5},
            "event_timestamp": doc_ts_b,
            "created_at": doc_ts_b,
        },
    ]
    collection.insert_many(docs)
    client.close()

    pit_source = MongoDBSource(
        name="driver_stats_pit2", timestamp_field="event_timestamp"
    )
    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    pit_fv = FeatureView(
        name="driver_stats_pit2",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
        ],
        source=pit_source,
        ttl=timedelta(days=7),
    )

    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 2],
            "event_timestamp": [request_a, request_b],
        }
    )

    mongodb_module = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb"

    # Force CHUNK_SIZE=1 so each entity is in its own chunk
    with patch(f"{mongodb_module}._CHUNK_SIZE", 1):
        job = MongoDBOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[pit_fv],
            feature_refs=["driver_stats_pit2:conv_rate"],
            entity_df=entity_df,
            registry=MagicMock(),
            project=repo_config.project,
            full_feature_names=False,
            strict_pit=True,
        )
        result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)

    assert len(result_df) == 2

    # Driver 1: doc at doc_ts_a (after request_a) → NULL regardless of chunk boundary
    assert pd.isna(result_df.loc[0, "conv_rate"]), (
        f"Expected NULL for driver 1 with CHUNK_SIZE=1, got "
        f"{result_df.loc[0, 'conv_rate']!r}"
    )

    # Driver 2: doc at doc_ts_b (before request_b) → value returned
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.5)


@_requires_docker
def test_strict_pit_false_returns_future_doc(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """strict_pit=False: future doc IS returned (real-time inference mode).

    Same scenario as test_scoring_path_nulls_future_doc:
        Entity A requests at 3 hours ago; only doc is at 1 hour ago (future).

    With strict_pit=False the future_mask is suppressed and the most recent
    document is returned regardless of whether it post-dates the request time.
    This is correct for real-time scoring where you always want the latest
    observation.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    collection = db["feature_history"]
    collection.drop()

    from datetime import timezone

    now = datetime.now(tz=timezone.utc)
    request_a = now - timedelta(hours=3)
    doc_ts_a = now - timedelta(hours=1)  # AFTER request_a

    docs = [
        {
            "entity_id": _make_entity_id({"driver_id": 1}),
            "feature_view": "driver_stats_pit3",
            "features": {"conv_rate": 0.9},
            "event_timestamp": doc_ts_a,
            "created_at": doc_ts_a,
        },
    ]
    collection.insert_many(docs)
    client.close()

    pit_source = MongoDBSource(
        name="driver_stats_pit3", timestamp_field="event_timestamp"
    )
    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    pit_fv = FeatureView(
        name="driver_stats_pit3",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
        ],
        source=pit_source,
        ttl=timedelta(days=7),
    )

    entity_df = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": [request_a],
        }
    )

    job = MongoDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[pit_fv],
        feature_refs=["driver_stats_pit3:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
        strict_pit=False,  # Accept future doc
    )

    result_df = job.to_df().reset_index(drop=True)
    assert len(result_df) == 1

    # With strict_pit=False the doc at doc_ts_a (after request_a) IS returned
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.9), (
        f"Expected conv_rate=0.9 with strict_pit=False but got "
        f"{result_df.loc[0, 'conv_rate']!r}. The future_mask may not be "
        f"correctly suppressed."
    )
