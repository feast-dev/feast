"""Integration tests for ScyllaDBOnlineStore, including vector search.

All tests (regular read/write and vector search) run against a local
ScyllaDB + Vector Store stack started via ``ScyllaDBOnlineStoreCreator``.
No external cloud cluster is required.

To run against a ScyllaDB Cloud cluster instead, set:

    SCYLLA_HOSTS="host1,host2"   contact points
    SCYLLA_KEYSPACE="feast_test" keyspace (default: feast_test)
    SCYLLA_USERNAME="scylla"     username (optional)
    SCYLLA_PASSWORD="..."        password (optional)
    SCYLLA_LOCAL_DC="..."        DC name, e.g. AWS_US_EAST_1 (required)
"""

import os
from typing import List

import pytest

from feast import Entity, FeatureView, RepoConfig
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.scylladb_online_store.scylladb import (
    ScyllaDBOnlineStore,
    ScyllaDBOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Array, Float32
from feast.utils import _utc_now
from feast.value_type import ValueType
from tests.universal.feature_repos.universal.online_store.scylladb import (
    ScyllaDBOnlineStoreCreator,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def docker_config() -> RepoConfig:
    """Start a local ScyllaDB container and return a RepoConfig for it."""
    creator = ScyllaDBOnlineStoreCreator(project_name="integ_test")
    store_dict = creator.create_online_store()
    cfg = RepoConfig(
        project="integ_test",
        provider="local",
        online_store=ScyllaDBOnlineStoreConfig(**store_dict),
        registry="memory://",
        entity_key_serialization_version=3,
    )
    yield cfg
    creator.teardown()


@pytest.fixture(scope="module")
def cloud_config():
    """RepoConfig pointing at a ScyllaDB Cloud cluster (skipped if env vars absent)."""
    if not os.environ.get("SCYLLA_HOSTS") or not os.environ.get("SCYLLA_LOCAL_DC"):
        pytest.skip("Set SCYLLA_HOSTS and SCYLLA_LOCAL_DC to run vector search tests")
    hosts = os.environ["SCYLLA_HOSTS"].split(",")
    cfg = RepoConfig(
        project="integ_test",
        provider="local",
        online_store=ScyllaDBOnlineStoreConfig(
            hosts=hosts,
            keyspace=os.environ.get("SCYLLA_KEYSPACE", "feast_test"),
            username=os.environ.get("SCYLLA_USERNAME"),
            password=os.environ.get("SCYLLA_PASSWORD"),
            local_dc=os.environ["SCYLLA_LOCAL_DC"],
        ),
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
        entities=[
            Entity(name="item_id", join_keys=["item_id"], value_type=ValueType.STRING)
        ],
        schema=schema,
        online=True,
        source=source,
    )


# ---------------------------------------------------------------------------
# Tests — regular online store (uses Docker via docker_config fixture)
# ---------------------------------------------------------------------------


def test_write_and_read(docker_config):
    store = ScyllaDBOnlineStore()
    cfg = docker_config
    fv = _make_feature_view("test_write_read")

    store.update(cfg, [], [fv], [], [], partial=False)
    try:
        ek = _make_entity_key("item_1")
        store.online_write_batch(
            cfg,
            fv,
            [
                (
                    ek,
                    {"score": ValueProto(float_list_val=FloatList(val=[0.9]))},
                    _utc_now(),
                    None,
                )
            ],
            None,
        )
        results = store.online_read(cfg, fv, [ek])
        assert len(results) == 1
        ts, feats = results[0]
        assert feats is not None
        assert "score" in feats
    finally:
        store.teardown(cfg, [fv], [])


def test_missing_key_returns_none(docker_config):
    store = ScyllaDBOnlineStore()
    cfg = docker_config
    fv = _make_feature_view("test_missing_key")

    store.update(cfg, [], [fv], [], [], partial=False)
    try:
        existing = _make_entity_key("present")
        missing = _make_entity_key("does_not_exist")
        store.online_write_batch(
            cfg,
            fv,
            [
                (
                    existing,
                    {"score": ValueProto(float_list_val=FloatList(val=[0.5]))},
                    _utc_now(),
                    None,
                )
            ],
            None,
        )
        results = store.online_read(cfg, fv, [existing, missing])
        assert len(results) == 2
        assert results[0][1] is not None
        assert results[1][1] is None
    finally:
        store.teardown(cfg, [fv], [])


# ---------------------------------------------------------------------------
# Tests — vector search (local Docker stack via docker_config)
# ---------------------------------------------------------------------------


def test_vector_search(docker_config):
    import time

    store = ScyllaDBOnlineStore()
    cfg = docker_config
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
            batch.append(
                (
                    ek,
                    {
                        "score": ValueProto(float_list_val=FloatList(val=[0.5])),
                        "embedding": ValueProto(float_list_val=FloatList(val=vec)),
                    },
                    _utc_now(),
                    None,
                )
            )
        store.online_write_batch(cfg, fv, batch, None)

        # Wait for the vector index to finish building (up to 60 s).
        deadline = time.time() + 60
        results = None
        while time.time() < deadline:
            try:
                results = store.retrieve_online_documents_v2(
                    cfg,
                    fv,
                    requested_features=["score", "embedding"],
                    embedding=[1.0, 0.0, 0.0, 0.0],
                    top_k=2,
                )
                break  # index is ready
            except Exception as exc:
                if "still being constructed" in str(exc) or "missing index" in str(exc):
                    time.sleep(2)
                else:
                    raise
        else:
            raise TimeoutError("Vector index was not ready within 60 s")

        assert results is not None
        assert len(results) == 2

        # Extract entity IDs from the returned entity key protos.
        returned_ids = []
        for ts, ek_proto, feats in results:
            assert ts is not None
            assert feats is not None
            assert "score" in feats
            assert "embedding" in feats
            assert ek_proto is not None
            returned_ids.append(ek_proto.entity_values[0].string_val)

        # Query is [1,0,0,0]; vec_a=[1,0,0,0] (exact match) and
        # vec_c=[1,0.1,0,0] are the two nearest neighbours by cosine similarity.
        # vec_b=[0,1,0,0] is orthogonal and must NOT appear in top-2.
        assert set(returned_ids) == {"vec_a", "vec_c"}, (
            f"Expected top-2 neighbours to be vec_a and vec_c, got {returned_ids}"
        )
        # Exact match must be ranked first.
        assert returned_ids[0] == "vec_a", (
            f"Expected vec_a (exact match) to be ranked first, got {returned_ids[0]}"
        )
    finally:
        store.teardown(cfg, [fv], [])
