"""Integration tests for ScyllaDBOnlineStore, including vector search.

These tests require a running ScyllaDB cluster. Set the following environment
variables before running:

    SCYLLA_HOSTS="host1,host2"   contact points (required to run tests)
    SCYLLA_KEYSPACE="feast_test" keyspace (default: feast_test)
    SCYLLA_USERNAME="scylla"     username (optional)
    SCYLLA_PASSWORD="..."        password (optional)
    SCYLLA_LOCAL_DC="..."        DC name, e.g. AWS_US_EAST_1 (required for vector tests)

Run:
    SCYLLA_HOSTS=... pytest sdk/python/tests/integration/online_store/test_scylladb_online_store.py -v
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Dict, List

import pytest

from feast import Entity, FeatureView, RepoConfig
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.scylladb_online_store.scylladb import (
    ScyllaDBOnlineStore,
    ScyllaDBOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList, Value as ValueProto
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# Skip entire module when SCYLLA_HOSTS is not set
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.skipif(
    not os.environ.get("SCYLLA_HOSTS"),
    reason="Set SCYLLA_HOSTS to run ScyllaDB integration tests",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _make_config(vector: bool = False) -> RepoConfig:
    hosts = os.environ.get("SCYLLA_HOSTS", "").split(",")
    store_cfg = ScyllaDBOnlineStoreConfig(
        hosts=hosts,
        keyspace=os.environ.get("SCYLLA_KEYSPACE", "feast_test"),
        username=os.environ.get("SCYLLA_USERNAME"),
        password=os.environ.get("SCYLLA_PASSWORD"),
        local_dc=os.environ.get("SCYLLA_LOCAL_DC"),
    )
    cfg = RepoConfig(
        project="integ_test",
        provider="local",
        online_store=store_cfg,
        registry="memory://",
        entity_key_serialization_version=3,
    )
    return cfg


def _make_entity_key(val: str) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["item_id"],
        entity_values=[ValueProto(string_val=val)],
    )


def _make_feature_view(name: str, with_vector: bool = False) -> FeatureView:
    source = FileSource(path="dummy.parquet", timestamp_field="event_timestamp")
    schema: List[Field] = [Field(name="score", dtype=Array(Float32))]
    if with_vector:
        schema.append(
            Field(
                name="embedding",
                dtype=Array(Float32),
                tags={
                    "vector_index": "true",
                    "dimensions": "4",
                    "similarity_function": "COSINE",
                },
            )
        )
    return FeatureView(
        name=name,
        entities=[Entity(name="item_id", join_keys=["item_id"], value_type=ValueType.STRING)],
        schema=schema,
        online=True,
        source=source,
    )


# ---------------------------------------------------------------------------
# Tests — regular online store
# ---------------------------------------------------------------------------


def test_write_and_read():
    store = ScyllaDBOnlineStore()
    cfg = _make_config()
    fv = _make_feature_view("test_write_read")

    store.update(cfg, [], [fv], [], [], partial=False)
    try:
        ek = _make_entity_key("item_1")
        store.online_write_batch(
            cfg,
            fv,
            [(ek, {"score": ValueProto(float_list_val=FloatList(val=[0.9]))}, _utc_now(), None)],
            None,
        )
        results = store.online_read(cfg, fv, [ek])
        assert len(results) == 1
        ts, feats = results[0]
        assert feats is not None
        assert "score" in feats
    finally:
        store.teardown(cfg, [fv], [])


def test_missing_key_returns_none():
    store = ScyllaDBOnlineStore()
    cfg = _make_config()
    fv = _make_feature_view("test_missing_key")

    store.update(cfg, [], [fv], [], [], partial=False)
    try:
        existing = _make_entity_key("present")
        missing = _make_entity_key("does_not_exist")
        store.online_write_batch(
            cfg,
            fv,
            [(existing, {"score": ValueProto(float_list_val=FloatList(val=[0.5]))}, _utc_now(), None)],
            None,
        )
        results = store.online_read(cfg, fv, [existing, missing])
        assert len(results) == 2
        assert results[0][1] is not None
        assert results[1][1] is None
    finally:
        store.teardown(cfg, [fv], [])


# ---------------------------------------------------------------------------
# Tests — vector search
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.environ.get("SCYLLA_LOCAL_DC"),
    reason="Set SCYLLA_LOCAL_DC to run vector search tests",
)
def test_vector_search():
    import time

    store = ScyllaDBOnlineStore()
    cfg = _make_config(vector=True)
    fv = _make_feature_view("test_vector_search", with_vector=True)

    store.update(cfg, [], [fv], [], [], partial=False)
    try:
        rows = [
            ("vec_a", [1.0, 0.0, 0.0, 0.0]),
            ("vec_b", [0.0, 1.0, 0.0, 0.0]),
            ("vec_c", [1.0, 0.1, 0.0, 0.0]),  # close to vec_a
        ]
        batch = []
        for item_id, vec in rows:
            ek = _make_entity_key(item_id)
            batch.append((
                ek,
                {
                    "score": ValueProto(float_list_val=FloatList(val=[0.5])),
                    "embedding": ValueProto(float_list_val=FloatList(val=vec)),
                },
                _utc_now(),
                None,
            ))
        store.online_write_batch(cfg, fv, batch, None)

        time.sleep(2)  # allow vector index to propagate

        results = store.retrieve_online_documents_v2(
            cfg, fv,
            requested_features=["score", "embedding"],
            embedding=[1.0, 0.0, 0.0, 0.0],
            top_k=2,
        )
        assert len(results) == 2
        for ts, ek_proto, feats in results:
            assert feats is not None
    finally:
        store.teardown(cfg, [fv], [])
