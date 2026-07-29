"""Unit tests for the Pinecone online store."""

import base64
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from feast import Entity, FeatureView
from feast.field import Field
from feast.filter_models import ComparisonFilter, CompoundFilter
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.pinecone_online_store.pinecone import (
    PineconeFilterTranslator,
    PineconeOnlineStore,
    PineconeOnlineStoreConfig,
    _extract_vector,
    _metadata_to_proto_value,
    _proto_value_to_metadata,
    _table_id,
)
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Array, Bool, Bytes, Float32, Float64, Int32, Int64, String
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

    def test_retrieve_requires_embedding_or_query_string(self):
        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_vector_feature_view()

        with pytest.raises(ValueError, match="Either embedding or query_string"):
            store.retrieve_online_documents_v2(
                config=config,
                table=fv,
                requested_features=["vector"],
                embedding=None,
                top_k=3,
            )

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_retrieve_with_filters_and_query_string(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index
        mock_response = MagicMock()
        mock_response.matches = []
        mock_index.query.return_value = mock_response

        store = PineconeOnlineStore()
        config = _make_config()
        fv = _make_vector_feature_view()
        filters = ComparisonFilter(type="eq", key="sentence_chunks", value="nyc")

        store.retrieve_online_documents_v2(
            config=config,
            table=fv,
            requested_features=["vector", "sentence_chunks"],
            embedding=[0.1, 0.2, 0.3],
            top_k=3,
            query_string="nyc",
            filters=filters,
        )

        query_kwargs = mock_index.query.call_args.kwargs
        pinecone_filter = query_kwargs["filter"]
        assert "$and" in pinecone_filter
        assert len(pinecone_filter["$and"]) == 2

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_retrieve_entity_fields_and_invalid_entity_key(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index

        mock_match = MagicMock()
        mock_match.metadata = {
            "event_ts": int(datetime(2024, 1, 1, 12, 0, 0).timestamp() * 1e6),
            "entity_key": "not-valid-hex",
            "item_id": 42,
            "sentence_chunks": "hello",
        }
        mock_match.values = [0.1, 0.2, 0.3]
        mock_match.score = 0.8
        mock_response = MagicMock()
        mock_response.matches = [mock_match]
        mock_index.query.return_value = mock_response

        store = PineconeOnlineStore()
        results = store.retrieve_online_documents_v2(
            config=_make_config(),
            table=_make_vector_feature_view(),
            requested_features=["vector", "item_id", "sentence_chunks"],
            embedding=[0.1, 0.2, 0.3],
            top_k=1,
        )

        assert len(results) == 1
        ts, entity_key, features = results[0]
        assert ts is not None
        assert entity_key is None
        assert features is not None
        assert features["item_id"].int64_val == 42
        assert "vector" in features


# ---------------------------------------------------------------------------
# Filter translator
# ---------------------------------------------------------------------------


class TestPineconeFilterTranslator:
    def test_translate_none(self):
        assert PineconeFilterTranslator().translate(None) is None

    def test_translate_eq(self):
        result = PineconeFilterTranslator().translate(
            ComparisonFilter(type="eq", key="city", value="nyc")
        )
        assert result == {"city": {"$eq": "nyc"}}

    def test_translate_numeric_ops(self):
        for op, pinecone_op in [
            ("ne", "$ne"),
            ("gt", "$gt"),
            ("gte", "$gte"),
            ("lt", "$lt"),
            ("lte", "$lte"),
        ]:
            result = PineconeFilterTranslator().translate(
                ComparisonFilter(type=op, key="score", value=10)
            )
            assert result == {"score": {pinecone_op: 10}}

    def test_translate_in_and_nin(self):
        assert PineconeFilterTranslator().translate(
            ComparisonFilter(type="in", key="city", value=["nyc", "la"])
        ) == {"city": {"$in": ["nyc", "la"]}}
        assert PineconeFilterTranslator().translate(
            ComparisonFilter(type="nin", key="city", value=["nyc"])
        ) == {"city": {"$nin": ["nyc"]}}

    def test_translate_in_requires_list(self):
        with pytest.raises(ValueError, match="requires a list"):
            PineconeFilterTranslator().translate(
                ComparisonFilter(type="in", key="city", value="nyc")
            )

    def test_translate_compound_and_or(self):
        compound_and = CompoundFilter(
            type="and",
            filters=[
                ComparisonFilter(type="eq", key="a", value=1),
                ComparisonFilter(type="eq", key="b", value=2),
            ],
        )
        assert PineconeFilterTranslator().translate(compound_and) == {
            "$and": [{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]
        }

        compound_or = CompoundFilter(
            type="or",
            filters=[
                ComparisonFilter(type="eq", key="a", value=1),
                ComparisonFilter(type="eq", key="b", value=2),
            ],
        )
        assert PineconeFilterTranslator().translate(compound_or) == {
            "$or": [{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]
        }

    def test_translate_compound_empty_and_single(self):
        assert (
            PineconeFilterTranslator().translate(CompoundFilter(type="and", filters=[]))
            == {}
        )
        single = CompoundFilter(
            type="and",
            filters=[ComparisonFilter(type="eq", key="a", value=1)],
        )
        assert PineconeFilterTranslator().translate(single) == {"a": {"$eq": 1}}


# ---------------------------------------------------------------------------
# Client / namespace helpers
# ---------------------------------------------------------------------------


class TestPineconeClientHelpers:
    def test_get_api_key_from_config(self):
        store = PineconeOnlineStore()
        assert (
            store._get_api_key(_make_config()) == "test-api-key"
        )  # pragma: allowlist secret

    def test_get_api_key_from_env(self, monkeypatch):
        store = PineconeOnlineStore()
        config = _make_config()
        config.online_store.api_key = None
        monkeypatch.setenv("PINECONE_API_KEY", "env-key")  # pragma: allowlist secret
        assert store._get_api_key(config) == "env-key"  # pragma: allowlist secret

    def test_get_api_key_missing(self, monkeypatch):
        store = PineconeOnlineStore()
        config = _make_config()
        config.online_store.api_key = None
        monkeypatch.delenv("PINECONE_API_KEY", raising=False)
        with pytest.raises(ValueError, match="API key is required"):
            store._get_api_key(config)

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_client"
    )
    def test_get_index_caches(self, mock_get_client):
        mock_client = MagicMock()
        mock_index = MagicMock()
        mock_client.Index.return_value = mock_index
        mock_get_client.return_value = mock_client

        store = PineconeOnlineStore()
        config = _make_config()
        assert store._get_index(config) is mock_index
        assert store._get_index(config) is mock_index
        mock_client.Index.assert_called_once_with("test-index")

    def test_get_namespace_custom(self):
        store = PineconeOnlineStore()
        config = _make_config()
        config.online_store.namespace = "custom-ns"
        assert store._get_namespace(config, _make_feature_view()) == "custom-ns"

    def test_plan_returns_empty(self):
        store = PineconeOnlineStore()
        assert store.plan(_make_config(), RegistryProto()) == []


# ---------------------------------------------------------------------------
# Additional write/read/update edge cases
# ---------------------------------------------------------------------------


class TestPineconeWriteReadEdgeCases:
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_write_batch_with_vector_and_progress(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index
        progress = MagicMock()

        store = PineconeOnlineStore()
        fv = _make_vector_feature_view()
        entity_key = EntityKeyProto()
        entity_key.join_keys.append("item_id")
        entity_key.entity_values.append(ValueProto(int64_val=1))
        vector_val = ValueProto()
        vector_val.float_list_val.val.extend([0.1, 0.2, 0.3])

        store.online_write_batch(
            _make_config(),
            fv,
            [
                (
                    entity_key,
                    {
                        "vector": vector_val,
                        "sentence_chunks": ValueProto(string_val="doc"),
                    },
                    datetime(2024, 1, 1, 12, 0, 0),
                    datetime(2024, 1, 1, 12, 0, 1),
                )
            ],
            progress=progress,
        )

        progress.assert_called_once_with(1)
        vectors = mock_index.upsert.call_args.kwargs["vectors"]
        assert vectors[0]["values"] == pytest.approx([0.1, 0.2, 0.3])
        assert vectors[0]["metadata"]["created_ts"] > 0

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_online_read_fetch_error(self, mock_get_index):
        mock_index = MagicMock()
        mock_index.fetch.side_effect = RuntimeError("boom")
        mock_get_index.return_value = mock_index

        store = PineconeOnlineStore()
        result = store.online_read(
            _make_config(), _make_feature_view(), [_make_entity_key(1)]
        )
        assert result == [(None, None)]

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_online_read_dict_response_and_vector(self, mock_get_index):
        mock_index = MagicMock()
        mock_get_index.return_value = mock_index

        entity_key = EntityKeyProto()
        entity_key.join_keys.append("item_id")
        entity_key.entity_values.append(ValueProto(int64_val=1))
        entity_key_str = serialize_entity_key(
            entity_key, entity_key_serialization_version=3
        ).hex()
        now_ts = int(datetime(2024, 1, 1, 12, 0, 0).timestamp() * 1e6)

        # Response object without .vectors attribute forces the .get() path.
        # Record must not be a plain dict (dict.values is a method).
        mock_record = MagicMock()
        mock_record.metadata = {
            "event_ts": now_ts,
            "sentence_chunks": "doc",
        }
        mock_record.values = [0.1, 0.2, 0.3]

        class FetchResponse(dict):
            def get(self, key, default=None):
                return super().get(key, default)

        fetch_response = FetchResponse({"vectors": {entity_key_str: mock_record}})
        # Ensure hasattr(fetch_response, "vectors") is False
        assert not hasattr(fetch_response, "vectors")
        mock_index.fetch.return_value = fetch_response

        store = PineconeOnlineStore()
        result = store.online_read(
            _make_config(),
            _make_vector_feature_view(),
            [entity_key],
            requested_features=["vector", "sentence_chunks"],
        )
        ts, features = result[0]
        assert ts is not None
        assert features is not None
        assert "vector" in features
        assert features["sentence_chunks"].string_val == "doc"

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_index"
    )
    def test_delete_namespace_swallows_errors(self, mock_get_index):
        mock_index = MagicMock()
        mock_index.delete.side_effect = RuntimeError("delete failed")
        mock_get_index.return_value = mock_index

        store = PineconeOnlineStore()
        store._delete_namespace(_make_config(), _make_feature_view())

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._get_client"
    )
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.PineconeOnlineStore._delete_namespace"
    )
    def test_update_deletes_tables(self, mock_delete, mock_get_client):
        mock_client = MagicMock()
        existing = MagicMock()
        existing.name = "test-index"
        mock_client.list_indexes.return_value = [existing]
        mock_get_client.return_value = mock_client

        store = PineconeOnlineStore()
        fv = _make_feature_view()
        store.update(_make_config(), [fv], [], [], [], partial=False)
        mock_delete.assert_called_once()

    def test_wait_for_index_ready_object_status(self):
        mock_client = MagicMock()
        status = MagicMock()
        status.ready = True
        desc = MagicMock()
        desc.status = status
        mock_client.describe_index.return_value = desc

        PineconeOnlineStore._wait_for_index_ready(mock_client, "idx")
        mock_client.describe_index.assert_called_once_with("idx")

    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.time.sleep",
        return_value=None,
    )
    @patch(
        "feast.infra.online_stores.pinecone_online_store.pinecone.time.time",
        side_effect=[0, 1, 400, 401],
    )
    def test_wait_for_index_ready_timeout(self, _mock_time, _mock_sleep):
        mock_client = MagicMock()
        mock_client.describe_index.return_value = MagicMock(status={"ready": False})
        # Force dict-status path with ready=False
        mock_client.describe_index.return_value.status = {"ready": False}
        PineconeOnlineStore._wait_for_index_ready(mock_client, "idx")
        mock_client.describe_index.assert_called()


# ---------------------------------------------------------------------------
# Extended proto conversion coverage
# ---------------------------------------------------------------------------


class TestProtoConversionsExtended:
    def test_extract_vector_int_lists(self):
        int32_val = ValueProto()
        int32_val.int32_list_val.val.extend([1, 2])
        assert _extract_vector(int32_val) == [1.0, 2.0]

        int64_val = ValueProto()
        int64_val.int64_list_val.val.extend([3, 4])
        assert _extract_vector(int64_val) == [3.0, 4.0]

    def test_proto_value_to_metadata_more_types(self):
        assert _proto_value_to_metadata(ValueProto(int32_val=7)) == 7
        assert _proto_value_to_metadata(ValueProto(double_val=1.5)) == 1.5

        float_list = ValueProto()
        float_list.float_list_val.val.extend([1.0, 2.0])
        assert _proto_value_to_metadata(float_list) == [1.0, 2.0]

        double_list = ValueProto()
        double_list.double_list_val.val.extend([1.0])
        assert _proto_value_to_metadata(double_list) == [1.0]

        int32_list = ValueProto()
        int32_list.int32_list_val.val.extend([1])
        assert _proto_value_to_metadata(int32_list) == [1]

        int64_list = ValueProto()
        int64_list.int64_list_val.val.extend([2])
        assert _proto_value_to_metadata(int64_list) == [2]

        bytes_val = ValueProto(bytes_val=b"abc")
        assert _proto_value_to_metadata(bytes_val) == base64.b64encode(b"abc").decode(
            "utf-8"
        )

    def test_metadata_to_proto_value_with_feast_types(self):
        assert _metadata_to_proto_value(5, Int64).int64_val == 5
        assert _metadata_to_proto_value(5, Int32).int32_val == 5
        assert abs(_metadata_to_proto_value(1.5, Float32).float_val - 1.5) < 1e-6
        assert abs(_metadata_to_proto_value(1.5, Float64).double_val - 1.5) < 1e-6
        assert _metadata_to_proto_value(True, Bool).bool_val is True
        assert _metadata_to_proto_value("x", String).string_val == "x"

        encoded = base64.b64encode(b"hi").decode("utf-8")
        assert _metadata_to_proto_value(encoded, Bytes).bytes_val == b"hi"
        assert _metadata_to_proto_value(b"raw", Bytes).bytes_val == b"raw"

        float_list = _metadata_to_proto_value([1.0, 2.0], Array(Float32))
        assert list(float_list.float_list_val.val) == [1.0, 2.0]

    def test_metadata_to_proto_value_fallback_list_and_other(self):
        non_numeric = _metadata_to_proto_value(["a", "b"], None)
        assert non_numeric.string_val == "['a', 'b']"
        other = _metadata_to_proto_value({"k": "v"}, None)
        assert other.string_val == "{'k': 'v'}"


# ---------------------------------------------------------------------------
# Repo configuration helper
# ---------------------------------------------------------------------------


class TestPineconeRepoConfiguration:
    def test_full_repo_configs(self):
        from feast.infra.online_stores.pinecone_online_store.pinecone_repo_configuration import (
            FULL_REPO_CONFIGS,
        )

        assert len(FULL_REPO_CONFIGS) == 1
        assert FULL_REPO_CONFIGS[0].online_store == "pinecone"

    def test_online_store_creator(self):
        from tests.universal.feature_repos.universal.online_store.pinecone import (
            PineconeOnlineStoreCreator,
        )

        creator = PineconeOnlineStoreCreator("proj")
        store_cfg = creator.create_online_store()
        assert store_cfg["type"] == "pinecone"
        assert store_cfg["index_name"] == "feast-test-proj"
        creator.teardown()
