"""Integration tests for MongoDB Atlas Vector Search in MongoDBOnlineStore.

These tests require Docker and the ``mongodb/mongodb-atlas-local:8.0.4`` image.
They exercise:
  - Vector index creation during ``update()``
  - Write + retrieve round-trip with known embeddings
  - ``top_k`` limiting
  - Index cleanup on teardown
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pytest

from feast import Entity, FeatureView, RepoConfig
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.mongodb_online_store.mongodb import (
    MongoDBOnlineStore,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType
from tests.universal.feature_repos.universal.online_store.mongodb import (
    MongoDBAtlasOnlineStoreCreator,
)

VECTOR_DIM = 3
NUM_ROWS = 5


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def atlas_creator():
    """Start an Atlas-local container for the whole module."""
    creator = MongoDBAtlasOnlineStoreCreator(project_name="test_vs")
    online_store_dict = creator.create_online_store()
    yield online_store_dict, creator
    creator.teardown()


@pytest.fixture(scope="module")
def repo_config(atlas_creator) -> RepoConfig:
    online_store_dict, _ = atlas_creator
    return RepoConfig(
        project="test_vs",
        provider="local",
        online_store=online_store_dict,
        registry="memory://",
        entity_key_serialization_version=3,
    )


@pytest.fixture(scope="module")
def item_entity() -> Entity:
    return Entity(name="item_id", join_keys=["item_id"], value_type=ValueType.INT64)


@pytest.fixture(scope="module")
def feature_view() -> FeatureView:
    data_source = FileSource(
        path="dummy_path.parquet", timestamp_field="event_timestamp"
    )
    return FeatureView(
        name="item_embeddings",
        entities=[Entity(name="item_id", join_keys=["item_id"])],
        schema=[
            Field(
                name="embedding",
                dtype=Array(Float32),
                vector_index=True,
                vector_length=VECTOR_DIM,
                vector_search_metric="cosine",
            ),
            Field(name="title", dtype=String),
            Field(name="item_id", dtype=Int64),
        ],
        source=data_source,
        ttl=timedelta(hours=24),
        online=True,
    )


@pytest.fixture(scope="module")
def write_data() -> List[Dict[str, Any]]:
    """Deterministic embeddings so we know the expected ordering."""
    np.random.seed(42)
    rows = []
    for i in range(1, NUM_ROWS + 1):
        rows.append(
            {
                "item_id": i,
                "embedding": list(np.random.rand(VECTOR_DIM).astype(float)),
                "title": f"Document {i}",
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestMongoDBVectorSearch:
    """Tests for MongoDB Atlas Vector Search integration."""

    def _make_store(self) -> MongoDBOnlineStore:
        return MongoDBOnlineStore()

    def test_vector_index_created_on_update(
        self, repo_config, feature_view, item_entity
    ):
        """Verify that update() creates an Atlas vector search index."""
        store = self._make_store()
        store.update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[feature_view],
            entities_to_delete=[],
            entities_to_keep=[item_entity],
            partial=False,
        )
        clxn = store._get_collection(repo_config)
        indexes = list(clxn.list_search_indexes())
        vs_index_names = [idx["name"] for idx in indexes]
        expected = "item_embeddings__embedding__vs_index"
        assert expected in vs_index_names, (
            f"Expected index '{expected}' not found. Got: {vs_index_names}"
        )

    def test_write_and_retrieve_round_trip(
        self, repo_config, feature_view, item_entity, write_data
    ):
        """Write embeddings then retrieve via vector search."""

        store = self._make_store()
        store.update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[feature_view],
            entities_to_delete=[],
            entities_to_keep=[item_entity],
            partial=False,
        )

        # Write data using online_write_batch
        batch = []
        now = datetime.utcnow()
        for row in write_data:
            entity_key = EntityKeyProto(
                join_keys=["item_id"],
                entity_values=[ValueProto(int64_val=row["item_id"])],
            )
            features: Dict[str, ValueProto] = {
                "title": ValueProto(string_val=row["title"]),
            }
            emb_proto = ValueProto()
            emb_proto.float_list_val.val.extend(row["embedding"])
            features["embedding"] = emb_proto
            batch.append((entity_key, features, now, now))

        store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=batch,
            progress=None,
        )

        # Query using the first document's embedding
        query_embedding = write_data[0]["embedding"]
        results = store.retrieve_online_documents_v2(
            config=repo_config,
            table=feature_view,
            requested_features=["embedding", "title"],
            embedding=query_embedding,
            top_k=3,
        )
        assert len(results) == 3
        # Each result is (event_ts, entity_key_proto, feature_dict)
        for ts, ek, fdict in results:
            assert fdict is not None
            assert "distance" in fdict
            assert "title" in fdict
            assert fdict["distance"].float_val >= 0

        # The closest match should be the document itself (highest score)
        _, _, best = results[0]
        assert best["title"].string_val == "Document 1"

    def test_top_k_limiting(self, repo_config, feature_view, item_entity, write_data):
        """Verify that top_k correctly limits the number of results."""
        store = self._make_store()
        query_embedding = write_data[0]["embedding"]

        results_2 = store.retrieve_online_documents_v2(
            config=repo_config,
            table=feature_view,
            requested_features=["embedding", "title"],
            embedding=query_embedding,
            top_k=2,
        )
        assert len(results_2) == 2

        results_all = store.retrieve_online_documents_v2(
            config=repo_config,
            table=feature_view,
            requested_features=["embedding", "title"],
            embedding=query_embedding,
            top_k=100,
        )
        assert len(results_all) == NUM_ROWS

    def test_index_cleanup_on_teardown(self, repo_config, feature_view, item_entity):
        """Verify teardown drops the collection (and thus all indexes)."""
        store = self._make_store()
        store.teardown(
            config=repo_config,
            tables=[feature_view],
            entities=[item_entity],
        )
        # After teardown the collection should be dropped
        client = store._get_client(repo_config)
        online_config = repo_config.online_store
        db = client[online_config.database_name]
        clxn_name = f"{repo_config.project}_{online_config.collection_suffix}"
        assert clxn_name not in db.list_collection_names()
