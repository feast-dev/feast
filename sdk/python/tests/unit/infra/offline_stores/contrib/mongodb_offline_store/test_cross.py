"""Cross-implementation equivalence suite for MongoDB offline stores.

Each test seeds the same logical data in **both** storage schemas, runs all
three implementations, and asserts that the feature values returned by each
are identical for the same entity/timestamp inputs.

Storage schemas
---------------
single-collection  (serves ``one`` and ``native``):
    feature_history  { entity_id: bytes, feature_view: str, features: {...},
                       event_timestamp: datetime, created_at: datetime }

per-FV-collection  (serves ``many``):
    <fv_name>        { <entity_col>: val, ..., <feat_col>: val, ...,
                       event_timestamp: datetime }

One call to ``_seed()`` writes both schemas so all three implementations read
from their natural format without any manual duplication in test bodies.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock

import pandas as pd
import pytest
import pytz

pytest.importorskip("pymongo")

from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from feast import Entity, FeatureView, Field
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_many import (
    MongoDBOfflineStoreMany,
    MongoDBOfflineStoreManyConfig,
    MongoDBSourceMany,
)
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_native import (
    MongoDBOfflineStoreNative,
    MongoDBOfflineStoreNativeConfig,
    MongoDBSourceNative,
)
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_one import (
    MongoDBOfflineStoreOne,
    MongoDBOfflineStoreOneConfig,
    MongoDBSourceOne,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Float64, Int64, String
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
# Separate DB so this suite never contaminates per-impl test suites
DB = "feast_cross"


# ---------------------------------------------------------------------------
# Entity key helper
# ---------------------------------------------------------------------------


def _make_entity_id(
    join_keys: dict,
    value_types: Optional[Dict[str, ValueType]] = None,
) -> bytes:
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
            if isinstance(value, int):
                val.int64_val = value
            elif isinstance(value, str):
                val.string_val = value
            else:
                val.string_val = str(value)
        entity_key.entity_values.append(val)
    return serialize_entity_key(entity_key, ENTITY_KEY_VERSION)


# ---------------------------------------------------------------------------
# Store implementation descriptors
# ---------------------------------------------------------------------------


@dataclass
class StoreImpl:
    """Bundles all implementation-specific factories into one object."""

    id: str
    store_class: Any
    make_offline_config: Callable[[str], Any]  # (conn_str) → offline store config
    make_source: Callable[[str], Any]  # (fv_name) → data source


ALL_IMPLS: List[StoreImpl] = [
    StoreImpl(
        id="one",
        store_class=MongoDBOfflineStoreOne,
        make_offline_config=lambda conn: MongoDBOfflineStoreOneConfig(
            connection_string=conn,
            database=DB,
            collection="feature_history",
        ),
        make_source=lambda fv_name: MongoDBSourceOne(
            name=fv_name,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_at",
        ),
    ),
    StoreImpl(
        id="native",
        store_class=MongoDBOfflineStoreNative,
        make_offline_config=lambda conn: MongoDBOfflineStoreNativeConfig(
            connection_string=conn,
            database=DB,
            collection="feature_history",
        ),
        make_source=lambda fv_name: MongoDBSourceNative(
            name=fv_name,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_at",
        ),
    ),
    StoreImpl(
        id="many",
        store_class=MongoDBOfflineStoreMany,
        make_offline_config=lambda conn: MongoDBOfflineStoreManyConfig(
            connection_string=conn,
            database=DB,
        ),
        make_source=lambda fv_name: MongoDBSourceMany(
            name=fv_name,
            database=DB,
            collection=fv_name,
            timestamp_field="event_timestamp",
        ),
    ),
]


# ---------------------------------------------------------------------------
# Seed helper — writes both schemas from the same logical rows
# ---------------------------------------------------------------------------


def _seed(client: MongoClient, rows: List[Dict]) -> None:
    """Insert rows into both storage schemas.

    Each element of *rows* is a dict with keys:
        fv_name     – feature view name (also the collection name for 'many')
        entity_dict – {join_key: value, ...}
        feature_dict – {feature_name: value, ...}
        event_ts    – datetime

    After this call:
    - 'one' and 'native' can read from ``DB.feature_history``
    - 'many' can read from ``DB.<fv_name>``
    """
    db = client[DB]

    # ── Single-collection schema (one + native) ────────────────────────────
    db["feature_history"].insert_many(
        [
            {
                "entity_id": _make_entity_id(r["entity_dict"]),
                "feature_view": r["fv_name"],
                "features": r["feature_dict"],
                "event_timestamp": r["event_ts"],
                "created_at": r["event_ts"],
            }
            for r in rows
        ]
    )

    # ── Per-FV-collection schema (many) ────────────────────────────────────
    by_fv: Dict[str, List[Dict]] = defaultdict(list)
    for r in rows:
        by_fv[r["fv_name"]].append(r)

    for fv_name, fv_rows in by_fv.items():
        db[fv_name].insert_many(
            [
                {
                    **r["entity_dict"],
                    **r["feature_dict"],
                    "event_timestamp": r["event_ts"],
                }
                for r in fv_rows
            ]
        )


# ---------------------------------------------------------------------------
# Run helper
# ---------------------------------------------------------------------------


def _run(
    impl: StoreImpl,
    conn_str: str,
    fvs: List[FeatureView],
    feature_refs: List[str],
    entity_df: pd.DataFrame,
    full_feature_names: bool = False,
) -> pd.DataFrame:
    """Run get_historical_features for one implementation and return the result."""
    config = RepoConfig(
        project="cross_test",
        registry="memory://",
        provider="local",
        offline_store=impl.make_offline_config(conn_str),
        online_store={"type": "sqlite"},
        entity_key_serialization_version=ENTITY_KEY_VERSION,
    )
    job = impl.store_class.get_historical_features(
        config=config,
        feature_views=fvs,
        feature_refs=feature_refs,
        entity_df=entity_df,
        registry=MagicMock(),
        project="cross_test",
        full_feature_names=full_feature_names,
    )
    return job.to_df()


# ---------------------------------------------------------------------------
# Assertion helper
# ---------------------------------------------------------------------------


def _assert_equivalence(
    dfs: List[pd.DataFrame],
    ids: List[str],
    sort_cols: List[str],
    feature_cols: List[str],
) -> None:
    """Assert all DataFrames return identical feature values after sorting.

    Lenient about column order, extra columns (e.g. entity key columns that
    differ between schemas), and numeric dtype coercion.  NaN == NaN for
    the purposes of this comparison.
    """
    normed = [df.sort_values(sort_cols).reset_index(drop=True) for df in dfs]
    ref, ref_id = normed[0], ids[0]

    for other, other_id in zip(normed[1:], ids[1:]):
        for col in feature_cols:
            pd.testing.assert_series_equal(
                ref[col].reset_index(drop=True),
                other[col].reset_index(drop=True),
                check_names=False,
                check_dtype=False,
                rtol=1e-5,
                obj=f"column '{col}' ({ref_id} vs {other_id})",
            )


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
def conn_str(mongodb_container) -> str:
    port = mongodb_container.get_exposed_port(27017)
    return f"mongodb://test:test@localhost:{port}"  # pragma: allowlist secret


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@_requires_docker
def test_pit_join_equivalence(conn_str: str) -> None:
    """PIT join returns the same feature values across all three implementations.

    Uses a time-varying dataset (three rows for driver 1 at different timestamps)
    and three entity rows that each point to a different historical slice.
    """
    now = datetime.now(tz=pytz.UTC)
    fv = "driver_pit_x"

    client = MongoClient(conn_str)
    _seed(
        client,
        [
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 1},
                "feature_dict": {"conv_rate": 0.5, "acc_rate": 0.9},
                "event_ts": now - timedelta(hours=2),
            },
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 1},
                "feature_dict": {"conv_rate": 0.6, "acc_rate": 0.85},
                "event_ts": now - timedelta(hours=1),
            },
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 1},
                "feature_dict": {"conv_rate": 0.7, "acc_rate": 0.8},
                "event_ts": now,
            },
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 2},
                "feature_dict": {"conv_rate": 0.3, "acc_rate": 0.95},
                "event_ts": now - timedelta(hours=2),
            },
        ],
    )
    client.close()

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                now - timedelta(hours=1, minutes=30),  # → 0.5
                now - timedelta(minutes=30),  # → 0.6
                now - timedelta(hours=1),  # → 0.3
            ],
        }
    )

    dfs = []
    for impl in ALL_IMPLS:
        source = impl.make_source(fv)
        feature_view = FeatureView(
            name=fv,
            entities=[driver_entity],
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float64),
                Field(name="acc_rate", dtype=Float64),
            ],
            source=source,
            ttl=timedelta(days=1),
        )
        dfs.append(
            _run(
                impl,
                conn_str,
                [feature_view],
                [f"{fv}:conv_rate", f"{fv}:acc_rate"],
                entity_df,
            )
        )

    _assert_equivalence(
        dfs,
        [impl.id for impl in ALL_IMPLS],
        sort_cols=["driver_id", "event_timestamp"],
        feature_cols=["conv_rate", "acc_rate"],
    )


@_requires_docker
def test_ttl_equivalence(conn_str: str) -> None:
    """All three implementations produce NULL for stale features and a value for fresh ones.

    Tests that TTL filtering is consistent: driver 1 (within 1-day TTL) has a
    non-NULL conv_rate; driver 2 (2 days old, outside TTL) has a NULL conv_rate.
    """
    now = datetime.now(tz=pytz.UTC)
    fv = "driver_ttl_x"

    client = MongoClient(conn_str)
    _seed(
        client,
        [
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 1},
                "feature_dict": {"conv_rate": 0.9},
                "event_ts": now - timedelta(hours=1),
            },
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 2},
                "feature_dict": {"conv_rate": 0.5},
                "event_ts": now - timedelta(days=2),  # stale
            },
        ],
    )
    client.close()

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    dfs = []
    for impl in ALL_IMPLS:
        source = impl.make_source(fv)
        feature_view = FeatureView(
            name=fv,
            entities=[driver_entity],
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float64),
            ],
            source=source,
            ttl=timedelta(days=1),
        )
        dfs.append(_run(impl, conn_str, [feature_view], [f"{fv}:conv_rate"], entity_df))

    # Feature values must be identical across implementations
    _assert_equivalence(
        dfs,
        [impl.id for impl in ALL_IMPLS],
        sort_cols=["driver_id"],
        feature_cols=["conv_rate"],
    )

    # Verify the NULL pattern itself is consistent
    for df, impl in zip(dfs, ALL_IMPLS):
        normed = df.sort_values("driver_id").reset_index(drop=True)
        assert not pd.isna(normed.loc[0, "conv_rate"]), (
            f"{impl.id}: driver 1 should be non-NULL (within TTL)"
        )
        assert pd.isna(normed.loc[1, "conv_rate"]), (
            f"{impl.id}: driver 2 should be NULL (outside TTL)"
        )


@_requires_docker
def test_multi_fv_equivalence(conn_str: str) -> None:
    """Features from two FVs joined on the same entity key are identical across all impls."""
    now = datetime.now(tz=pytz.UTC)
    fv_driver = "driver_mfv_x"
    fv_vehicle = "vehicle_mfv_x"

    client = MongoClient(conn_str)
    _seed(
        client,
        [
            {
                "fv_name": fv_driver,
                "entity_dict": {"driver_id": 1},
                "feature_dict": {"rating": 4.8},
                "event_ts": now - timedelta(hours=1),
            },
            {
                "fv_name": fv_driver,
                "entity_dict": {"driver_id": 2},
                "feature_dict": {"rating": 4.5},
                "event_ts": now - timedelta(hours=1),
            },
            {
                "fv_name": fv_vehicle,
                "entity_dict": {"driver_id": 1},
                "feature_dict": {"vehicle_age": 2, "mileage": 50000},
                "event_ts": now - timedelta(hours=1),
            },
            {
                "fv_name": fv_vehicle,
                "entity_dict": {"driver_id": 2},
                "feature_dict": {"vehicle_age": 5, "mileage": 120000},
                "event_ts": now - timedelta(hours=1),
            },
        ],
    )
    client.close()

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    dfs = []
    for impl in ALL_IMPLS:
        fv1 = FeatureView(
            name=fv_driver,
            entities=[driver_entity],
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="rating", dtype=Float64),
            ],
            source=impl.make_source(fv_driver),
            ttl=timedelta(days=1),
        )
        fv2 = FeatureView(
            name=fv_vehicle,
            entities=[driver_entity],
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="vehicle_age", dtype=Int64),
                Field(name="mileage", dtype=Int64),
            ],
            source=impl.make_source(fv_vehicle),
            ttl=timedelta(days=1),
        )
        dfs.append(
            _run(
                impl,
                conn_str,
                [fv1, fv2],
                [
                    f"{fv_driver}:rating",
                    f"{fv_vehicle}:vehicle_age",
                    f"{fv_vehicle}:mileage",
                ],
                entity_df,
            )
        )

    _assert_equivalence(
        dfs,
        [impl.id for impl in ALL_IMPLS],
        sort_cols=["driver_id"],
        feature_cols=["rating", "vehicle_age", "mileage"],
    )


@_requires_docker
def test_overlapping_feature_names_equivalence(conn_str: str) -> None:
    """Three FVs sharing a feature named 'score' are handled identically with full_feature_names=True.

    This also adds overlapping-feature-name coverage to the 'many' implementation,
    which lacked this test in its individual suite.
    """
    now = datetime.now(tz=pytz.UTC)
    fv_names = ["fv_ol_xa", "fv_ol_xb", "fv_ol_xc"]
    bases = {"fv_ol_xa": 1.0, "fv_ol_xb": 2.0, "fv_ol_xc": 3.0}

    client = MongoClient(conn_str)
    rows = []
    for fv_name in fv_names:
        for driver_id in [1, 2]:
            rows.append(
                {
                    "fv_name": fv_name,
                    "entity_dict": {"driver_id": driver_id},
                    "feature_dict": {"score": bases[fv_name] + driver_id * 0.1},
                    "event_ts": now - timedelta(hours=1),
                }
            )
    _seed(client, rows)
    client.close()

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    entity_df = pd.DataFrame({"driver_id": [1, 2], "event_timestamp": [now, now]})

    dfs = []
    for impl in ALL_IMPLS:
        fvs = [
            FeatureView(
                name=fv_name,
                entities=[driver_entity],
                schema=[
                    Field(name="driver_id", dtype=Int64),
                    Field(name="score", dtype=Float64),
                ],
                source=impl.make_source(fv_name),
                ttl=timedelta(days=1),
            )
            for fv_name in fv_names
        ]
        dfs.append(
            _run(
                impl,
                conn_str,
                fvs,
                [f"{n}:score" for n in fv_names],
                entity_df,
                full_feature_names=True,
            )
        )

    prefixed_cols = [f"{n}__score" for n in fv_names]
    _assert_equivalence(
        dfs,
        [impl.id for impl in ALL_IMPLS],
        sort_cols=["driver_id"],
        feature_cols=prefixed_cols,
    )


@_requires_docker
def test_compound_keys_equivalence(conn_str: str) -> None:
    """Compound join keys (user_id, device_id) produce identical results across all impls."""
    now = datetime.now(tz=pytz.UTC)
    fv = "user_device_x"

    client = MongoClient(conn_str)
    _seed(
        client,
        [
            {
                "fv_name": fv,
                "entity_dict": {"user_id": 1, "device_id": "mobile"},
                "feature_dict": {"app_opens": 55},
                "event_ts": now - timedelta(hours=1),
            },
            {
                "fv_name": fv,
                "entity_dict": {"user_id": 1, "device_id": "desktop"},
                "feature_dict": {"app_opens": 10},
                "event_ts": now - timedelta(hours=1),
            },
            {
                "fv_name": fv,
                "entity_dict": {"user_id": 2, "device_id": "tablet"},
                "feature_dict": {"app_opens": 25},
                "event_ts": now - timedelta(hours=1),
            },
        ],
    )
    client.close()

    user_entity = Entity(
        name="user_id", join_keys=["user_id"], value_type=ValueType.INT64
    )
    device_entity = Entity(
        name="device_id", join_keys=["device_id"], value_type=ValueType.STRING
    )
    entity_df = pd.DataFrame(
        {
            "user_id": [1, 1, 2],
            "device_id": ["mobile", "desktop", "tablet"],
            "event_timestamp": [now, now, now],
        }
    )

    dfs = []
    for impl in ALL_IMPLS:
        feature_view = FeatureView(
            name=fv,
            entities=[user_entity, device_entity],
            schema=[
                Field(name="user_id", dtype=Int64),
                Field(name="device_id", dtype=String),
                Field(name="app_opens", dtype=Int64),
            ],
            source=impl.make_source(fv),
            ttl=timedelta(days=1),
        )
        dfs.append(_run(impl, conn_str, [feature_view], [f"{fv}:app_opens"], entity_df))

    _assert_equivalence(
        dfs,
        [impl.id for impl in ALL_IMPLS],
        sort_cols=["user_id", "device_id"],
        feature_cols=["app_opens"],
    )


@_requires_docker
def test_extra_columns_equivalence(conn_str: str) -> None:
    """Extra label columns in entity_df pass through unchanged and do not affect features.

    This is the cross-implementation regression test for the union-key bug: if any
    implementation folds non-join-key columns into the entity key, features come
    back NULL.  Here we verify that all three return the same non-NULL feature
    values and preserve the label column.
    """
    now = datetime.now(tz=pytz.UTC)
    fv = "driver_extra_x"

    client = MongoClient(conn_str)
    _seed(
        client,
        [
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 1},
                "feature_dict": {"conv_rate": 0.5},
                "event_ts": now - timedelta(hours=2),
            },
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 1},
                "feature_dict": {"conv_rate": 0.6},
                "event_ts": now - timedelta(hours=1),
            },
            {
                "fv_name": fv,
                "entity_dict": {"driver_id": 2},
                "feature_dict": {"conv_rate": 0.3},
                "event_ts": now - timedelta(hours=2),
            },
        ],
    )
    client.close()

    driver_entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    entity_df = pd.DataFrame(
        {
            "driver_id": [1, 1, 2],
            "event_timestamp": [
                now - timedelta(hours=1, minutes=30),
                now - timedelta(minutes=30),
                now - timedelta(hours=1),
            ],
            "trip_success": [1, 0, 1],  # label column — must not enter entity key
        }
    )

    dfs = []
    for impl in ALL_IMPLS:
        source = impl.make_source(fv)
        feature_view = FeatureView(
            name=fv,
            entities=[driver_entity],
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float64),
            ],
            source=source,
            ttl=timedelta(days=1),
        )
        dfs.append(_run(impl, conn_str, [feature_view], [f"{fv}:conv_rate"], entity_df))

    # Feature values must be identical
    _assert_equivalence(
        dfs,
        [impl.id for impl in ALL_IMPLS],
        sort_cols=["driver_id", "event_timestamp"],
        feature_cols=["conv_rate"],
    )

    # Features must be non-NULL (union-key bug would produce all-NULL here)
    for df, impl in zip(dfs, ALL_IMPLS):
        assert df["conv_rate"].notna().all(), (
            f"{impl.id}: conv_rate is null — label column may have been "
            "folded into the entity key serialization."
        )

    # Label column must be present and unchanged in all three
    for df, impl in zip(dfs, ALL_IMPLS):
        assert "trip_success" in df.columns, f"{impl.id}: trip_success column missing"
        assert sorted(df["trip_success"].tolist()) == [0, 1, 1], (
            f"{impl.id}: trip_success values changed"
        )
