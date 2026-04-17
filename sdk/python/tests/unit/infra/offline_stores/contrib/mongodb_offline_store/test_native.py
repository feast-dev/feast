"""
Unit tests for the Atlas-first MongoDB native offline store.

Uses the single-collection schema (all FVs share ``feature_history``,
discriminated by ``feature_view`` field) and performs point-in-time joins
entirely via ``$documents`` + ``$lookup`` aggregation pipelines.

Requires MongoDB 5.1+ (Atlas M10+, or self-managed).  The testcontainer
uses ``mongo:latest`` which satisfies this requirement.

Docker-dependent tests are marked with ``@_requires_docker`` and are skipped
when Docker is unavailable.
"""

from datetime import datetime, timedelta
from typing import Dict, Optional
from unittest.mock import MagicMock

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
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_native import (
    MongoDBOfflineStoreNative,
    MongoDBOfflineStoreNativeConfig,
    MongoDBSourceNative,
    SavedDatasetMongoDBStorageNative,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Float64, Int32, Int64, String
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# Docker guard
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
    """Serialize an entity key from a plain dict, mirroring _serialize_entity_key_from_row.

    Args:
        join_keys: Mapping of join key name → value.
        value_types: Optional declared ValueType per key.  When provided the
            correct proto field is used (e.g. INT32 → int32_val).  Omit to
            get int64_val/string_val inference.
    """
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
        elif declared_type == ValueType.BYTES:
            val.bytes_val = bytes(value)
        elif declared_type == ValueType.UNIX_TIMESTAMP:
            val.unix_timestamp_val = int(value)
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
def mongodb_container():
    container = MongoDbContainer(
        "mongo:latest",
        username="test",
        password="test",  # pragma: allowlist secret
    ).with_exposed_ports(27017)
    container.start()
    yield container
    container.stop()


@pytest.fixture
def mongodb_connection_string(mongodb_container) -> str:
    port = mongodb_container.get_exposed_port(27017)
    return f"mongodb://test:test@localhost:{port}"  # pragma: allowlist secret


@pytest.fixture
def repo_config(mongodb_connection_string: str) -> RepoConfig:
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
    """Insert driver_stats documents using the single-collection schema."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    db = client["feast_test"]
    coll = db["feature_history"]
    coll.drop()

    now = datetime.now(tz=pytz.UTC)
    coll.insert_many(
        [
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
    )
    client.close()
    return now


@pytest.fixture
def driver_source() -> MongoDBSourceNative:
    return MongoDBSourceNative(
        name="driver_stats",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_at",
    )


@pytest.fixture
def driver_fv(driver_source: MongoDBSourceNative) -> FeatureView:
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
# Tests — pull_latest_from_table_or_query
# ---------------------------------------------------------------------------


@_requires_docker
def test_pull_latest_from_table_or_query(
    repo_config: RepoConfig, sample_data: datetime, driver_source: MongoDBSourceNative
) -> None:
    """pull_latest returns one row per entity, taking the most recent document."""
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
    assert len(df) == 2  # two unique entity_ids

    conv_rates = sorted(df["conv_rate"].tolist())
    assert conv_rates[0] == pytest.approx(0.3)  # driver 2's only value
    assert conv_rates[1] == pytest.approx(0.7)  # driver 1's latest


# ---------------------------------------------------------------------------
# Tests — pull_all_from_table_or_query
# ---------------------------------------------------------------------------


@_requires_docker
def test_pull_all_from_table_or_query(
    repo_config: RepoConfig, sample_data: datetime, driver_source: MongoDBSourceNative
) -> None:
    """pull_all returns every document in the time range without deduplication."""
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
    # 2 rows: driver 1 from 1 hr ago and driver 1 from now
    # Excludes: driver 1 from 2 hrs ago, driver 2 from 2 hrs ago
    assert len(df) == 2


# ---------------------------------------------------------------------------
# Tests — get_historical_features (PIT join)
# ---------------------------------------------------------------------------


@_requires_docker
def test_get_historical_features_pit_join(
    repo_config: RepoConfig, sample_data: datetime, driver_fv: FeatureView
) -> None:
    """PIT join returns the most recent feature value at or before each entity row's timestamp."""
    now = sample_data

    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                now - timedelta(hours=1, minutes=30),  # → conv_rate=0.5
                now - timedelta(minutes=30),  # → conv_rate=0.6
                now - timedelta(hours=1),  # → conv_rate=0.3
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

    result_df = result_df.sort_values(["driver_id", "event_timestamp"]).reset_index(
        drop=True
    )

    # driver 1, 1.5 hrs ago → feature row from 2 hrs ago
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.5)
    # driver 1, 30 min ago → feature row from 1 hr ago
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.6)
    # driver 2, 1 hr ago → feature row from 2 hrs ago
    assert result_df.loc[2, "conv_rate"] == pytest.approx(0.3)


@_requires_docker
def test_ttl_excludes_stale_features(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Documents older than TTL must come back as NULL, not the stale value."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    now = datetime.now(tz=pytz.UTC)
    coll = client["feast_test"]["feature_history"]
    coll.insert_many(
        [
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
                "event_timestamp": now - timedelta(days=2),  # stale
                "created_at": now - timedelta(days=2),
            },
        ]
    )
    client.close()

    source = MongoDBSourceNative(name="driver_stats_ttl")
    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    fv = FeatureView(
        name="driver_stats_ttl",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
        ],
        source=source,
        ttl=timedelta(days=1),
    )

    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    job = MongoDBOfflineStoreNative.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["driver_stats_ttl:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)

    # driver 1: fresh → value present
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.9)
    # driver 2: stale → NULL
    assert pd.isna(result_df.loc[1, "conv_rate"])


@_requires_docker
def test_multiple_feature_views(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Features from two FVs with the same join key are joined correctly."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    now = datetime.now(tz=pytz.UTC)
    coll = client["feast_test"]["feature_history"]
    coll.insert_many(
        [
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
    )
    client.close()

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
        source=MongoDBSourceNative(name="driver_stats_multi"),
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
        source=MongoDBSourceNative(name="vehicle_stats_multi"),
        ttl=timedelta(days=1),
    )

    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

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
    assert {"driver_id", "rating", "vehicle_age", "mileage"}.issubset(result_df.columns)

    assert result_df.loc[0, "rating"] == pytest.approx(4.8)
    assert result_df.loc[0, "vehicle_age"] == 2
    assert result_df.loc[0, "mileage"] == 50000
    assert result_df.loc[1, "rating"] == pytest.approx(4.5)
    assert result_df.loc[1, "vehicle_age"] == 5
    assert result_df.loc[1, "mileage"] == 120000


@_requires_docker
def test_heterogeneous_join_keys(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """FVs with different join keys must each match only their own documents.

    This is the core correctness invariant of the per-FV entity key design.
    driver_stats documents are keyed by serialize({driver_id: X}).
    customer_stats documents are keyed by serialize({customer_id: Y}).
    The entity_df has both columns.

    A naive implementation would serialize {customer_id: Y, driver_id: X}
    (union of all entity columns) and query that single key against both FVs —
    producing bytes that match neither stored collection.  The correct
    implementation serializes each FV's key independently and issues a separate
    $lookup per FV.
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    now = datetime.now(tz=pytz.UTC)
    coll = client["feast_test"]["feature_history"]
    coll.insert_many(
        [
            # driver_stats: keyed by driver_id only
            {
                "entity_id": _make_entity_id({"driver_id": 10}),
                "feature_view": "driver_stats_het",
                "features": {"conv_rate": 0.72},
                "event_timestamp": now - timedelta(hours=1),
                "created_at": now - timedelta(hours=1),
            },
            {
                "entity_id": _make_entity_id({"driver_id": 20}),
                "feature_view": "driver_stats_het",
                "features": {"conv_rate": 0.55},
                "event_timestamp": now - timedelta(hours=1),
                "created_at": now - timedelta(hours=1),
            },
            # customer_stats: keyed by customer_id only
            {
                "entity_id": _make_entity_id({"customer_id": 100}),
                "feature_view": "customer_stats_het",
                "features": {"lifetime_spend": 1500.0},
                "event_timestamp": now - timedelta(hours=1),
                "created_at": now - timedelta(hours=1),
            },
            {
                "entity_id": _make_entity_id({"customer_id": 200}),
                "feature_view": "customer_stats_het",
                "features": {"lifetime_spend": 800.0},
                "event_timestamp": now - timedelta(hours=1),
                "created_at": now - timedelta(hours=1),
            },
        ]
    )
    client.close()

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    customer_entity = Entity(
        name="customer_id", join_keys=["customer_id"], value_type=ValueType.INT64
    )

    driver_fv = FeatureView(
        name="driver_stats_het",
        entities=[driver_entity],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float64),
        ],
        source=MongoDBSourceNative(name="driver_stats_het"),
        ttl=timedelta(days=1),
    )
    customer_fv = FeatureView(
        name="customer_stats_het",
        entities=[customer_entity],
        schema=[
            Field(name="customer_id", dtype=Int64),
            Field(name="lifetime_spend", dtype=Float64),
        ],
        source=MongoDBSourceNative(name="customer_stats_het"),
        ttl=timedelta(days=1),
    )

    # entity_df carries both join keys — exactly the scenario the union-key
    # bug would break.
    entity_df = pd.DataFrame(
        {
            "driver_id": [10, 20],
            "customer_id": [100, 200],
            "event_timestamp": [now, now],
        }
    )

    job = MongoDBOfflineStoreNative.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv, customer_fv],
        feature_refs=[
            "driver_stats_het:conv_rate",
            "customer_stats_het:lifetime_spend",
        ],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    result_df = job.to_df().sort_values("driver_id").reset_index(drop=True)
    assert len(result_df) == 2

    # If the union-key bug were present, both columns would be NULL.
    assert result_df["conv_rate"].notna().all(), (
        "conv_rate is null — union join key serialization would include "
        "customer_id in the driver_stats entity key, producing bytes that "
        "never match stored documents."
    )
    assert result_df["lifetime_spend"].notna().all(), (
        "lifetime_spend is null — union join key serialization would include "
        "driver_id in the customer_stats entity key."
    )

    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.72)
    assert result_df.loc[0, "lifetime_spend"] == pytest.approx(1500.0)
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.55)
    assert result_df.loc[1, "lifetime_spend"] == pytest.approx(800.0)


@_requires_docker
def test_multiple_feature_views_overlapping_feature_names(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Three FVs all defining a feature named 'score' must not collide under full_feature_names=True."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    now = datetime.now(tz=pytz.UTC)
    coll = client["feast_test"]["feature_history"]

    docs = []
    for fv_name, base in [("fv_ol_a", 1.0), ("fv_ol_b", 2.0), ("fv_ol_c", 3.0)]:
        for driver_id in [1, 2]:
            docs.append(
                {
                    "entity_id": _make_entity_id({"driver_id": driver_id}),
                    "feature_view": fv_name,
                    "features": {"score": base + driver_id * 0.1},
                    "event_timestamp": now - timedelta(hours=1),
                    "created_at": now - timedelta(hours=1),
                }
            )
    coll.insert_many(docs)
    client.close()

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    fvs = []
    feature_refs = []
    for fv_name in ["fv_ol_a", "fv_ol_b", "fv_ol_c"]:
        fv = FeatureView(
            name=fv_name,
            entities=[driver_entity],
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="score", dtype=Float64),
            ],
            source=MongoDBSourceNative(name=fv_name),
            ttl=timedelta(days=1),
        )
        fvs.append(fv)
        feature_refs.append(f"{fv_name}:score")

    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    job = MongoDBOfflineStoreNative.get_historical_features(
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
    assert "fv_ol_a__score" in result_df.columns
    assert "fv_ol_b__score" in result_df.columns
    assert "fv_ol_c__score" in result_df.columns

    assert result_df.loc[0, "fv_ol_a__score"] == pytest.approx(1.1)
    assert result_df.loc[0, "fv_ol_b__score"] == pytest.approx(2.1)
    assert result_df.loc[0, "fv_ol_c__score"] == pytest.approx(3.1)
    assert result_df.loc[1, "fv_ol_a__score"] == pytest.approx(1.2)
    assert result_df.loc[1, "fv_ol_b__score"] == pytest.approx(2.2)
    assert result_df.loc[1, "fv_ol_c__score"] == pytest.approx(3.2)


@_requires_docker
def test_compound_join_keys(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """Compound (multi-column) join keys are serialized and matched correctly."""
    client: MongoClient = MongoClient(mongodb_connection_string)
    now = datetime.now(tz=pytz.UTC)
    coll = client["feast_test"]["feature_history"]
    coll.insert_many(
        [
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
                "features": {"app_opens": 55},
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
    )
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

    # pull_latest: one row per unique (user_id, device_id)
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
    assert len(df) == 3
    assert set(df["app_opens"].tolist()) == {55, 10, 25}

    # get_historical_features with compound keys
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
    result_df = job.to_df().sort_values(["user_id", "device_id"]).reset_index(drop=True)
    assert len(result_df) == 3
    assert result_df.loc[0, "app_opens"] == 10  # user 1, desktop
    assert result_df.loc[1, "app_opens"] == 55  # user 1, mobile (latest)
    assert result_df.loc[2, "app_opens"] == 25  # user 2, tablet


@_requires_docker
def test_entity_df_with_extra_columns(
    repo_config: RepoConfig, sample_data: datetime, driver_fv: FeatureView
) -> None:
    """Extra (label) columns in entity_df must not corrupt entity key serialization.

    The native store derives join keys from fv.entity_columns, not from all
    non-timestamp columns in the entity_df.  A label column such as
    ``trip_success`` must pass through unchanged and must not enter the key.
    """
    now = sample_data

    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                now - timedelta(hours=1, minutes=30),
                now - timedelta(minutes=30),
                now - timedelta(hours=1),
            ],
            "trip_success": [1, 0, 1],  # label — must not affect entity key
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
    assert len(result_df) == 3
    assert "trip_success" in result_df.columns
    assert sorted(result_df["trip_success"].tolist()) == [0, 1, 1]
    assert result_df["conv_rate"].notna().all(), (
        "conv_rate is null — label column was likely included in the entity key."
    )

    result_df = result_df.sort_values(["driver_id", "event_timestamp"]).reset_index(
        drop=True
    )
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.5)
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.6)
    assert result_df.loc[2, "conv_rate"] == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# Tests — INT32 entity key round-trip
# ---------------------------------------------------------------------------


@_requires_docker
def test_int32_entity_key(
    repo_config: RepoConfig, mongodb_connection_string: str
) -> None:
    """INT32 entity keys round-trip through the correct proto field (int32_val).

    Without the declared-type fix, Python ``int`` always maps to int64_val
    (8 bytes), silently mismatching documents stored with int32_val (4 bytes).
    """
    client: MongoClient = MongoClient(mongodb_connection_string)
    now = datetime.now(tz=pytz.UTC)
    int32_types = {"order_id": ValueType.INT32}
    client["feast_test"]["feature_history"].insert_many(
        [
            {
                "entity_id": _make_entity_id({"order_id": 1}, int32_types),
                "feature_view": "order_features",
                "features": {"amount": 100.0},
                "event_timestamp": now - timedelta(hours=1),
                "created_at": now - timedelta(hours=1),
            },
            {
                "entity_id": _make_entity_id({"order_id": 2}, int32_types),
                "feature_view": "order_features",
                "features": {"amount": 200.0},
                "event_timestamp": now - timedelta(hours=1),
                "created_at": now - timedelta(hours=1),
            },
        ]
    )
    client.close()

    order_entity = Entity(
        name="order_id", join_keys=["order_id"], value_type=ValueType.INT32
    )
    fv = FeatureView(
        name="order_features",
        entities=[order_entity],
        schema=[
            Field(name="order_id", dtype=Int32),
            Field(name="amount", dtype=Float64),
        ],
        source=MongoDBSourceNative(name="order_features"),
        ttl=timedelta(days=1),
    )

    entity_df = pd.DataFrame({"order_id": [1, 2], "event_timestamp": [now, now]})

    job = MongoDBOfflineStoreNative.get_historical_features(
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
        "amount is null — INT32 entity key was serialized as INT64, producing "
        "bytes that do not match the stored INT32 documents."
    )
    assert result_df.loc[0, "amount"] == pytest.approx(100.0)
    assert result_df.loc[1, "amount"] == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# Tests — offline_write_batch round-trip
# ---------------------------------------------------------------------------


@_requires_docker
def test_offline_write_batch_round_trip(
    repo_config: RepoConfig,
    mongodb_connection_string: str,
    driver_fv: FeatureView,
) -> None:
    """offline_write_batch writes documents that get_historical_features can read back."""
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

    MongoDBOfflineStoreNative.offline_write_batch(
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

    # Verify round-trip
    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})
    job = MongoDBOfflineStoreNative.get_historical_features(
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
        "are producing different entity_id bytes."
    )
    assert result_df.loc[0, "conv_rate"] == pytest.approx(0.8)
    assert result_df.loc[0, "acc_rate"] == pytest.approx(0.9)
    assert result_df.loc[1, "conv_rate"] == pytest.approx(0.6)
    assert result_df.loc[1, "acc_rate"] == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# Tests — persist()
# ---------------------------------------------------------------------------


@_requires_docker
def test_persist_writes_flat_result(
    repo_config: RepoConfig,
    sample_data: datetime,
    driver_fv: FeatureView,
    mongodb_connection_string: str,
) -> None:
    """persist() writes the flat joined DataFrame to the named destination collection."""
    now = sample_data
    dest = "saved_ds_native_basic"

    client: MongoClient = MongoClient(mongodb_connection_string)
    client["feast_test"][dest].drop()
    client.close()

    entity_df = pd.DataFrame(
        {"driver_id": [1, 2], "event_timestamp": [now, now - timedelta(hours=2)]}
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
    job.persist(
        SavedDatasetMongoDBStorageNative(database="feast_test", collection=dest)
    )

    client = MongoClient(mongodb_connection_string)
    try:
        docs = list(client["feast_test"][dest].find({}, {"_id": 0}))
    finally:
        client.close()

    assert len(docs) == 2
    assert {"driver_id", "conv_rate", "acc_rate", "event_timestamp"}.issubset(
        docs[0].keys()
    )


@_requires_docker
def test_persist_raises_if_collection_exists(
    repo_config: RepoConfig,
    sample_data: datetime,
    driver_fv: FeatureView,
    mongodb_connection_string: str,
) -> None:
    """persist() raises SavedDatasetLocationAlreadyExists when destination is non-empty."""
    now = sample_data
    dest = "saved_ds_native_no_overwrite"

    client: MongoClient = MongoClient(mongodb_connection_string)
    client["feast_test"][dest].drop()
    client["feast_test"][dest].insert_one({"placeholder": True})
    client.close()

    entity_df = pd.DataFrame({"driver_id": [1], "event_timestamp": [now]})

    job = MongoDBOfflineStoreNative.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )

    with pytest.raises(SavedDatasetLocationAlreadyExists):
        job.persist(
            SavedDatasetMongoDBStorageNative(database="feast_test", collection=dest),
            allow_overwrite=False,
        )


@_requires_docker
def test_persist_allow_overwrite(
    repo_config: RepoConfig,
    sample_data: datetime,
    driver_fv: FeatureView,
    mongodb_connection_string: str,
) -> None:
    """persist(allow_overwrite=True) replaces any existing collection contents."""
    now = sample_data
    dest = "saved_ds_native_overwrite"

    client: MongoClient = MongoClient(mongodb_connection_string)
    client["feast_test"][dest].drop()
    client["feast_test"][dest].insert_many([{"stale": True}, {"stale": True}])
    client.close()

    entity_df = pd.DataFrame({"driver_id": [1], "event_timestamp": [now]})

    job = MongoDBOfflineStoreNative.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project=repo_config.project,
        full_feature_names=False,
    )
    job.persist(
        SavedDatasetMongoDBStorageNative(database="feast_test", collection=dest),
        allow_overwrite=True,
    )

    client = MongoClient(mongodb_connection_string)
    try:
        docs = list(client["feast_test"][dest].find({}, {"_id": 0}))
    finally:
        client.close()

    assert len(docs) == 1
    assert "stale" not in docs[0]
    assert "driver_id" in docs[0]
