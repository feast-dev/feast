"""Unit tests for the Pinecone online store."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.pinecone_online_store.pinecone import (
    PineconeOnlineStore,
    PineconeOnlineStoreConfig,
    _extract_vector,
    _metadata_to_proto_value,
    _proto_value_to_metadata,
    _table_id,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_feature_view(name="driver_stats", version_number=None, version_tag=None):
    entity = Entity(
        name="driver_id",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )
    fv = FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="trips_today", dtype=Float32),
            Field(name="driver_name", dtype=String),
        ],
    )
    if version_number is not None:
        fv.current_version_number = version_number
    if version_tag is not None:
        fv.projection.version_tag = version_tag
    return fv


def _make_vector_feature_view():
    entity = Entity(
        name="item_id",
        join_keys=["item_id"],
        value_type=ValueType.INT64,
    )
    return FeatureView(
        name="embedded_docs",
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[
            Field(
                name="vector",
                dtype=Array(Float32),
                vector_index=True,
                vector_search_metric="COSINE",
            ),
            Field(name="item_id", dtype=Int64),
            Field(name="sentence_chunks", dtype=String),
        ],
    )


def _make_config(project="test_project", versioning=False):
    config = MagicMock()
    config.project = project
    config.entity_key_serialization_version = 3
    config.registry.enable_online_feature_view_versioning = versioning
    config.online_store = PineconeOnlineStoreConfig(
        type="pinecone",
        api_key="test-api-key",  # pragma: allowlist secret
        index_name="test-index",
        embedding_dim=3,
        metric="cosine",
        vector_enabled=True,
    )
    return config


def _make_entity_key(driver_id: int = 1) -> EntityKeyProto:
    key = EntityKeyProto()
    key.join_keys.append("driver_id")
    val = ValueProto(int64_val=driver_id)
    key.entity_values.append(val)
    return key


# ---------------------------------------------------------------------------
# Config Tests
# ---------------------------------------------------------------------------


class TestPineconeOnlineStoreConfig:
    def test_defaults(self):
        config = PineconeOnlineStoreConfig()
        assert config.type == "pinecone"
        assert config.index_name == "feast-online"
        assert config.embedding_dim == 128
        assert config.metric == "cosine"
        assert config.cloud == "aws"
        assert config.region == "us-east-1"
        assert config.vector_enabled is True

    def test_custom_values(self):
        config = PineconeOnlineStoreConfig(
            api_key="my-key",  # pragma: allowlist secret
            index_name="my-index",
            embedding_dim=384,
            metric="dotproduct",
            cloud="gcp",
            region="us-central1",
            namespace="custom-ns",
        )
        assert config.api_key == "my-key"  # pragma: allowlist secret
        assert config.index_name == "my-index"
        assert config.embedding_dim == 384
        assert config.metric == "dotproduct"
        assert config.cloud == "gcp"
        assert config.region == "us-central1"
        assert config.namespace == "custom-ns"


# ---------------------------------------------------------------------------
# Table ID Tests
# ---------------------------------------------------------------------------


class TestTableId:
    def test_no_versioning(self):
        fv = _make_feature_view()
        assert _table_id("test_project", fv) == "test_project_driver_stats"

    def test_versioning_enabled_with_version(self):
        fv = _make_feature_view(version_number=2)
        assert (
            _table_id("test_project", fv, enable_versioning=True)
            == "test_project_driver_stats_v2"
        )

    def test_versioning_disabled_ignores_version(self):
        fv = _make_feature_view(version_number=5)
        assert _table_id("test_project", fv) == "test_project_driver_stats"


# ---------------------------------------------------------------------------
# Proto Conversion Tests
# ---------------------------------------------------------------------------


class TestProtoConversions:
    def test_extract_vector_float_list(self):
        val = ValueProto()
        val.float_list_val.val.extend([1.0, 2.0, 3.0])
        result = _extract_vector(val)
        assert result == [1.0, 2.0, 3.0]

    def test_extract_vector_double_list(self):
        val = ValueProto()
        val.double_list_val.val.extend([1.0, 2.0])
        result = _extract_vector(val)
        assert result == [1.0, 2.0]

    def test_extract_vector_none_for_non_list(self):
        val = ValueProto(string_val="hello")
        assert _extract_vector(val) is None

    def test_proto_value_to_metadata_string(self):
        val = ValueProto(string_val="hello")
        assert _proto_value_to_metadata(val) == "hello"

    def test_proto_value_to_metadata_int(self):
        val = ValueProto(int64_val=42)
        assert _proto_value_to_metadata(val) == 42

    def test_proto_value_to_metadata_float(self):
        val = ValueProto(float_val=3.14)
        assert abs(_proto_value_to_metadata(val) - 3.14) < 1e-6

    def test_proto_value_to_metadata_bool(self):
        val = ValueProto(bool_val=True)
        assert _proto_value_to_metadata(val) is True

    def test_metadata_to_proto_value_string(self):
        result = _metadata_to_proto_value("hello", None)
        assert result.string_val == "hello"

    def test_metadata_to_proto_value_int(self):
        result = _metadata_to_proto_value(42, None)
        assert result.int64_val == 42

    def test_metadata_to_proto_value_float(self):
        result = _metadata_to_proto_value(3.14, None)
        assert abs(result.double_val - 3.14) < 1e-6

    def test_metadata_to_proto_value_bool(self):
        result = _metadata_to_proto_value(True, None)
        assert result.bool_val is True

    def test_metadata_to_proto_value_list(self):
        result = _metadata_to_proto_value([1.0, 2.0, 3.0], None)
        assert list(result.float_list_val.val) == [1.0, 2.0, 3.0]


# ---------------------------------------------------------------------------
# Online Store Tests (mocked Pinecone client)
# ---------------------------------------------------------------------------


class TestPineconeOnlineStoreWriteBatch:
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_online_write_batch(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index

        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_feature_view()
        entity_key = _make_entity_key(1)

        data = [
            (
                entity_key,
                {
                    "trips_today": ValueProto(float_val=10.0),
                    "driver_name": ValueProto(string_val="Alice"),
                },
                datetime(2024, 1, 1, 12, 0, 0),
                None,
            )
        ]

        store.online_write_batch(config, fv, data, progress=None)

        mock_index.upsert.assert_called_once()
        call_kwargs = mock_index.upsert.call_args
        vectors = call_kwargs.kwargs.get("vectors") or call_kwargs[1].get("vectors")
        assert len(vectors) == 1
        vec = vectors[0]
        assert "id" in vec
        assert "values" in vec
        assert "metadata" in vec
        assert vec["metadata"]["trips_today"] == 10.0
        assert vec["metadata"]["driver_name"] == "Alice"

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_write_batch_deduplication(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index

        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_feature_view()
        entity_key = _make_entity_key(1)

        data = [
            (
                entity_key,
                {"trips_today": ValueProto(float_val=5.0)},
                datetime(2024, 1, 1, 10, 0, 0),
                None,
            ),
            (
                entity_key,
                {"trips_today": ValueProto(float_val=15.0)},
                datetime(2024, 1, 1, 14, 0, 0),
                None,
            ),
        ]

        store.online_write_batch(config, fv, data, progress=None)

        call_kwargs = mock_index.upsert.call_args
        vectors = call_kwargs.kwargs.get("vectors") or call_kwargs[1].get("vectors")
        assert len(vectors) == 1
        assert vectors[0]["metadata"]["trips_today"] == 15.0


class TestPineconeOnlineStoreRead:
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_online_read(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index

        entity_key = _make_entity_key(1)
        entity_key_str = serialize_entity_key(
            entity_key, entity_key_serialization_version=3
        ).hex()

        now_ts = int(datetime(2024, 1, 1, 12, 0, 0).timestamp() * 1e6)

        mock_record = MagicMock()
        mock_record.metadata = {
            "event_ts": now_ts,
            "created_ts": 0,
            "entity_key": entity_key_str,
            "trips_today": 10.0,
            "driver_name": "Alice",
        }
        mock_record.values = [0.0, 0.0, 0.0]

        mock_response = MagicMock()
        mock_response.vectors = {entity_key_str: mock_record}
        mock_index.fetch.return_value = mock_response

        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_feature_view()

        result = store.online_read(
            config, fv, [entity_key], requested_features=["trips_today"]
        )

        assert len(result) == 1
        ts, features = result[0]
        assert ts is not None
        assert features is not None
        assert "trips_today" in features

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_online_read_missing_entity(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index

        mock_response = MagicMock()
        mock_response.vectors = {}
        mock_index.fetch.return_value = mock_response

        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_feature_view()
        entity_key = _make_entity_key(999)

        result = store.online_read(config, fv, [entity_key])

        assert len(result) == 1
        assert result[0] == (None, None)


class TestPineconeOnlineStoreUpdate:
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_client"
    )
    def test_update_creates_index(self, mock_get_client):
        import sys
        import types

        mock_pinecone_module = types.ModuleType("pinecone")
        mock_pinecone_module.ServerlessSpec = MagicMock()
        sys.modules["pinecone"] = mock_pinecone_module

        try:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.list_indexes.return_value = []

            mock_desc = MagicMock()
            mock_desc.status = {"ready": True}
            mock_client.describe_index.return_value = mock_desc

            store = PineconeOnlineStore()
            config = _make_config()
            fv = _make_feature_view()

            store.update(config, [], [fv], [], [], partial=False)

            mock_client.create_index.assert_called_once()
            call_kwargs = mock_client.create_index.call_args.kwargs
            assert call_kwargs["name"] == "test-index"
            assert call_kwargs["dimension"] == 3
            assert call_kwargs["metric"] == "cosine"
        finally:
            del sys.modules["pinecone"]

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_client"
    )
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_update_skips_existing_index(self, mock_get_index, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        existing_idx = MagicMock()
        existing_idx.name = "test-index"
        mock_client.list_indexes.return_value = [existing_idx]
        mock_get_index.return_value = MagicMock()

        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_feature_view()

        store.update(config, [], [fv], [], [], partial=False)

        mock_client.create_index.assert_not_called()


class TestPineconeOnlineStoreTeardown:
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_teardown_deletes_namespace(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index

        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_feature_view()

        store.teardown(config, [fv], [])

        mock_index.delete.assert_called_once()
        call_kwargs = mock_index.delete.call_args.kwargs
        assert call_kwargs["delete_all"] is True
        assert call_kwargs["namespace"] == "test_project_driver_stats"


class TestPineconeRetrieveDocumentsV2:
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_retrieve_online_documents_v2(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index

        entity_key = EntityKeyProto()
        entity_key.join_keys.append("item_id")
        entity_key.entity_values.append(ValueProto(int64_val=42))
        entity_key_str = serialize_entity_key(
            entity_key, entity_key_serialization_version=3
        ).hex()

        now_ts = int(datetime(2024, 1, 1, 12, 0, 0).timestamp() * 1e6)

        mock_match = MagicMock()
        mock_match.metadata = {
            "event_ts": now_ts,
            "entity_key": entity_key_str,
            "sentence_chunks": "New York City",
        }
        mock_match.values = [0.1, 0.2, 0.3]
        mock_match.score = 0.95

        mock_response = MagicMock()
        mock_response.matches = [mock_match]
        mock_index.query.return_value = mock_response

        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_vector_feature_view()

        results = store.retrieve_online_documents_v2(
            config=config,
            table=fv,
            requested_features=["vector", "sentence_chunks"],
            embedding=[0.1, 0.2, 0.3],
            top_k=3,
            distance_metric="cosine",
        )

        assert len(results) == 1
        ts, entity, features = results[0]
        assert ts is not None
        assert features is not None
        assert "distance" in features
        assert features["distance"].float_val == pytest.approx(0.95)
        assert "sentence_chunks" in features

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_retrieve_requires_embedding(self, mock_get_index):
        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_vector_feature_view()

        with pytest.raises(ValueError, match="requires a query embedding"):
            store.retrieve_online_documents_v2(
                config=config,
                table=fv,
                requested_features=["vector"],
                embedding=None,
                top_k=3,
                query_string="some text",
            )

    def test_retrieve_vector_not_enabled(self):
        store = PineconeOnlineStore()
        config = _make_config()
        config.online_store.vector_enabled = False
        fv = _make_vector_feature_view()

        with pytest.raises(ValueError, match="not enabled"):
            store.retrieve_online_documents_v2(
                config=config,
                table=fv,
                requested_features=["vector"],
                embedding=[0.1, 0.2, 0.3],
                top_k=3,
            )
