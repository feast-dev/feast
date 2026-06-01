"""Pure unit tests for MongoDB online store helpers."""

# ruff: noqa: E402

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pymongo")

from feast import Entity, FeatureView, Field, FileSource, RepoConfig  # noqa: E402
from feast.infra.online_stores.mongodb_online_store.mongodb import (  # noqa: E402
    MongoDBOnlineStore,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Array, Float32, Int64, String


def _make_fv(*field_names: str) -> FeatureView:
    """Build a minimal FeatureView with Int64 features for use in unit tests."""
    return FeatureView(
        name="test_fv",
        entities=[],
        schema=[Field(name=n, dtype=Int64) for n in field_names],
        source=FileSource(path="fake.parquet", timestamp_field="event_timestamp"),
        ttl=timedelta(days=1),
    )


def test_convert_raw_docs_missing_entity():
    """Entity key absent from docs → result tuple is (None, None) for that position."""
    fv = _make_fv("score")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"present", b"missing"]
    docs = {
        b"present": {
            "features": {"test_fv": {"score": 42}},
            "event_timestamps": {"test_fv": ts},
        }
    }

    results = MongoDBOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert len(results) == 2
    ts_out, feats_out = results[0]
    assert ts_out == ts
    assert feats_out["score"].int64_val == 42
    assert results[1] == (None, None)


def test_convert_raw_docs_partial_doc():
    """Entity exists but one feature key is absent → empty ValueProto for that feature."""
    fv = _make_fv("present_feat", "missing_feat")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"entity1"]
    docs = {
        b"entity1": {
            # missing_feat intentionally omitted (e.g. schema migration scenario)
            "features": {"test_fv": {"present_feat": 99}},
            "event_timestamps": {"test_fv": ts},
        }
    }

    results = MongoDBOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert len(results) == 1
    ts_out, feats_out = results[0]
    assert ts_out == ts
    assert feats_out["present_feat"].int64_val == 99
    assert feats_out["missing_feat"] == ValueProto()  # null / not-set


def test_convert_raw_docs_entity_exists_but_fv_not_written():
    """Entity doc exists (written by another FV) but this FV was never written → (None, None).

    MongoDB stores all feature views for the same entity in one document.
    If FV "driver_stats" was written, an entity doc exists for driver_1.
    A subsequent read for FV "pricing" (never written) must return (None, None),
    not a truthy dict of empty ValueProtos.
    """
    pricing_fv = _make_fv("price")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"driver_1"]
    # doc was created by driver_stats, pricing key is absent entirely
    docs = {
        b"driver_1": {
            "features": {"driver_stats": {"acc_rate": 0.9}},
            "event_timestamps": {"driver_stats": ts},
        }
    }

    results = MongoDBOnlineStore._convert_raw_docs_to_proto(ids, docs, pricing_fv)

    assert len(results) == 1
    assert results[0] == (None, None)


def test_convert_raw_docs_ordering():
    """Result order matches the ids list regardless of dict insertion order in docs."""
    fv = _make_fv("score")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Request entity keys in z → a → m order
    ids = [b"entity_z", b"entity_a", b"entity_m"]

    # docs is in a different order (simulating arbitrary MongoDB cursor return order)
    docs = {
        b"entity_a": {
            "features": {"test_fv": {"score": 2}},
            "event_timestamps": {"test_fv": ts},
        },
        b"entity_m": {
            "features": {"test_fv": {"score": 3}},
            "event_timestamps": {"test_fv": ts},
        },
        b"entity_z": {
            "features": {"test_fv": {"score": 1}},
            "event_timestamps": {"test_fv": ts},
        },
    }

    results = MongoDBOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert len(results) == 3
    # Results must follow the ids order: z=1, a=2, m=3
    assert results[0][1]["score"].int64_val == 1  # entity_z
    assert results[1][1]["score"].int64_val == 2  # entity_a
    assert results[2][1]["score"].int64_val == 3  # entity_m


# ---------------------------------------------------------------------------
# Vector search validation — pure Python, no Docker required
# ---------------------------------------------------------------------------


def _make_vector_fv(
    *, vector_index: bool = True, vector_length: int = 3
) -> FeatureView:
    """Build a FeatureView with an embedding field for vector search tests."""
    embedding_kwargs: dict = {"name": "embedding", "dtype": Array(Float32)}
    if vector_index:
        embedding_kwargs.update(
            vector_index=True,
            vector_length=vector_length,
            vector_search_metric="cosine",
        )
    return FeatureView(
        name="item_embeddings",
        entities=[Entity(name="item_id", join_keys=["item_id"])],
        schema=[
            Field(**embedding_kwargs),
            Field(name="title", dtype=String),
            Field(name="item_id", dtype=Int64),
        ],
        source=FileSource(path="fake.parquet", timestamp_field="event_timestamp"),
        ttl=timedelta(hours=24),
    )


def _make_repo_config(*, vector_enabled: bool = True) -> RepoConfig:
    """Build a RepoConfig with MongoDB online store for vector search tests."""
    return RepoConfig(
        project="test_vs",
        provider="local",
        online_store={
            "type": "mongodb",
            "connection_string": "mongodb://localhost:27017",
            "vector_enabled": vector_enabled,
        },
        registry="memory://",
        entity_key_serialization_version=3,
    )


def test_retrieve_raises_when_vector_not_enabled():
    """retrieve_online_documents_v2 raises ValueError when vector_enabled=False."""
    store = MongoDBOnlineStore()
    config = _make_repo_config(vector_enabled=False)
    fv = _make_vector_fv()

    with pytest.raises(ValueError, match="Vector search is not enabled"):
        store.retrieve_online_documents_v2(
            config=config,
            table=fv,
            requested_features=["embedding"],
            embedding=[1.0, 0.0, 0.0],
            top_k=3,
        )


def test_retrieve_raises_when_embedding_is_none():
    """retrieve_online_documents_v2 raises ValueError when embedding is None."""
    store = MongoDBOnlineStore()
    config = _make_repo_config(vector_enabled=True)
    fv = _make_vector_fv()

    with pytest.raises(ValueError, match="embedding vector must be provided"):
        store.retrieve_online_documents_v2(
            config=config,
            table=fv,
            requested_features=["embedding"],
            embedding=None,
            top_k=3,
        )


def test_retrieve_raises_when_no_vector_fields():
    """retrieve_online_documents_v2 raises ValueError when no fields have vector_index=True."""
    store = MongoDBOnlineStore()
    config = _make_repo_config(vector_enabled=True)
    fv = _make_vector_fv(vector_index=False)

    with pytest.raises(ValueError, match="has no fields with vector_index=True"):
        store.retrieve_online_documents_v2(
            config=config,
            table=fv,
            requested_features=["embedding"],
            embedding=[1.0, 0.0, 0.0],
            top_k=3,
        )


def test_ensure_vector_indexes_raises_on_missing_vector_length():
    """_ensure_vector_indexes raises ValueError when vector_length is not set."""
    from unittest.mock import MagicMock

    store = MongoDBOnlineStore()
    config = _make_repo_config(vector_enabled=True)
    online_config = config.online_store

    # Create a feature view where vector_index=True but vector_length=0/None
    fv = _make_vector_fv(vector_index=True, vector_length=0)

    # Mock collection so we don't need a real MongoDB connection
    mock_collection = MagicMock()
    mock_collection.database.list_collection_names.return_value = ["test_vs_latest"]
    mock_collection.name = "test_vs_latest"
    mock_collection.list_search_indexes.return_value = []

    with pytest.raises(
        ValueError, match="vector_index=True but vector_length is not set"
    ):
        store._ensure_vector_indexes(mock_collection, [fv], online_config)
