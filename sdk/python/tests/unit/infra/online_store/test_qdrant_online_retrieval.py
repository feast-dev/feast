"""Unit tests for Qdrant retrieve_online_documents_v2."""

# ruff: noqa: E402

import base64
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("qdrant_client")

from feast import Entity, FeatureView, Field, FileSource, RepoConfig  # noqa: E402
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.qdrant_online_store.qdrant import (  # noqa: E402
    QdrantOnlineStore,
    QdrantOnlineStoreConfig,
    _decode_feature_value,
    _entity_key_bytes_from_payload,
    _parse_timestamp,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList as FloatListProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType


def _make_repo_config(**kwargs) -> RepoConfig:
    online_store = QdrantOnlineStoreConfig(
        host="localhost",
        port=6333,
        similarity="cosine",
        **kwargs,
    )
    return RepoConfig(
        project="test_project",
        registry="data/registry.db",
        provider="local",
        online_store=online_store,
        entity_key_serialization_version=3,
    )


def _make_document_fv() -> FeatureView:
    item = Entity(name="item_id", join_keys=["item_id"], value_type=ValueType.INT64)
    return FeatureView(
        name="documents",
        entities=[item],
        schema=[
            Field(name="embedding", dtype=Array(Float32), vector_index=True),
            Field(name="text_field", dtype=String),
            Field(name="item_id", dtype=Int64),
        ],
        source=FileSource(path="fake.parquet", timestamp_field="event_timestamp"),
    )


def _entity_key(item_id: int = 1) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["item_id"],
        entity_values=[ValueProto(int64_val=item_id)],
    )


def test_entity_key_bytes_from_payload_handles_bytes_and_base64():
    raw = b"\x01\x02\x03"
    assert _entity_key_bytes_from_payload(raw) == raw
    encoded = base64.b64encode(raw).decode("utf-8")
    assert _entity_key_bytes_from_payload(encoded) == raw


def test_decode_feature_value_round_trip():
    original = ValueProto(string_val="hello")
    encoded = base64.b64encode(original.SerializeToString()).decode("utf-8")
    decoded = _decode_feature_value(encoded)
    assert decoded.string_val == "hello"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2024-01-01T12:00:00.000000", datetime(2024, 1, 1, 12, 0, 0)),
        ("2024-01-01T12:00:00", datetime(2024, 1, 1, 12, 0, 0)),
        (datetime(2024, 6, 1, 8, 30, 15), datetime(2024, 6, 1, 8, 30, 15)),
    ],
)
def test_parse_timestamp_handles_fractional_and_whole_seconds(raw, expected):
    assert _parse_timestamp(raw) == expected


def test_parse_timestamp_returns_none_for_missing_value():
    assert _parse_timestamp(None) is None


def test_build_v2_result_includes_requested_features_and_distance():
    entity_key = _entity_key(7)
    entity_key_bin = serialize_entity_key(
        entity_key, entity_key_serialization_version=3
    )
    title_proto = ValueProto(string_val="doc title")
    features_by_entity = {entity_key_bin: {"text_field": title_proto}}
    payload = {"timestamp": datetime(2024, 1, 1, 12, 0, 0)}

    ts, entity_proto, feature_dict = QdrantOnlineStore()._build_v2_result(
        entity_key_bin,
        payload,
        requested_features=["text_field", "embedding"],
        features_by_entity=features_by_entity,
        distance=0.91,
        text_rank=None,
        entity_key_serialization_version=3,
    )

    assert ts == payload["timestamp"]
    assert entity_proto is not None
    assert entity_proto.entity_values[0].int64_val == 7
    assert feature_dict is not None
    assert feature_dict["text_field"].string_val == "doc title"
    assert feature_dict["embedding"] == ValueProto()
    assert feature_dict["distance"].float_val == pytest.approx(0.91)


def test_retrieve_online_documents_v1_decodes_base64_entity_key():
    config = _make_repo_config()
    table = _make_document_fv()
    store = QdrantOnlineStore()
    entity_key = _entity_key(42)
    entity_key_bin = serialize_entity_key(
        entity_key, entity_key_serialization_version=3
    )

    text_proto = ValueProto(string_val="doc body")
    encoded_text = base64.b64encode(text_proto.SerializeToString()).decode("utf-8")
    hit_point = MagicMock()
    hit_point.payload = {
        "entity_key": base64.b64encode(entity_key_bin).decode("ascii"),
        "feature_value": encoded_text,
        "timestamp": "2024-01-01T12:00:00",
    }
    hit_point.score = 0.95
    hit_point.vector = {"": [0.1, 0.2, 0.3]}

    mock_client = MagicMock()
    mock_client.query_points.return_value = MagicMock(points=[hit_point])

    with patch.object(store, "_get_client", return_value=mock_client):
        results = store.retrieve_online_documents(
            config=config,
            table=table,
            requested_features=["text_field"],
            embedding=[0.1, 0.2, 0.3],
            top_k=1,
        )

    assert len(results) == 1
    ts, entity_proto, feature_proto, vector_proto, distance_proto = results[0]
    assert entity_proto is not None
    assert entity_proto.entity_values[0].int64_val == 42
    assert feature_proto is not None
    assert feature_proto.string_val == "doc body"
    assert distance_proto is not None
    assert distance_proto.float_val == pytest.approx(0.95)
    assert ts == datetime(2024, 1, 1, 12, 0, 0)


def test_retrieve_online_documents_v2_dense_search_joins_features():
    config = _make_repo_config()
    table = _make_document_fv()
    store = QdrantOnlineStore()
    entity_key = _entity_key(1)
    entity_key_bin = serialize_entity_key(
        entity_key, entity_key_serialization_version=3
    )

    import base64

    text_proto = ValueProto(string_val="hello world")
    encoded_text = base64.b64encode(text_proto.SerializeToString()).decode("utf-8")
    hit_payload = {
        "entity_key": base64.b64encode(entity_key_bin).decode("ascii"),
        "feature_name": "embedding",
        "feature_value": encoded_text,
        "timestamp": "2024-01-01T12:00:00.000000",
    }
    hit_point = MagicMock()
    hit_point.payload = hit_payload
    hit_point.score = 0.88

    scroll_point = MagicMock()
    scroll_point.payload = {
        "entity_key": base64.b64encode(entity_key_bin).decode("ascii"),
        "feature_name": "text_field",
        "feature_value": encoded_text,
    }

    mock_client = MagicMock()
    mock_client.query_points.return_value = MagicMock(points=[hit_point])
    mock_client.scroll.return_value = ([scroll_point], None)

    with patch.object(store, "_get_client", return_value=mock_client):
        results = store.retrieve_online_documents_v2(
            config=config,
            table=table,
            requested_features=["text_field", "embedding"],
            embedding=[0.1, 0.2, 0.3],
            top_k=1,
        )

    assert len(results) == 1
    ts, entity_proto, feature_dict = results[0]
    assert entity_proto is not None
    assert feature_dict is not None
    assert feature_dict["text_field"].string_val == "hello world"
    assert feature_dict["distance"].float_val == pytest.approx(0.88)
    assert ts == datetime(2024, 1, 1, 12, 0, 0)

    query_kwargs = mock_client.query_points.call_args.kwargs
    assert query_kwargs["limit"] == 1
    assert query_kwargs["query"] == [0.1, 0.2, 0.3]


def test_retrieve_online_documents_v2_requires_text_search_for_query_string():
    config = _make_repo_config(text_search_enabled=False)
    table = _make_document_fv()
    store = QdrantOnlineStore()

    with pytest.raises(ValueError, match="text_search_enabled"):
        store.retrieve_online_documents_v2(
            config=config,
            table=table,
            requested_features=["text_field"],
            embedding=None,
            top_k=1,
            query_string="hello",
        )


def test_retrieve_online_documents_v2_hybrid_uses_rrf_prefetch():
    config = _make_repo_config(text_search_enabled=True)
    table = _make_document_fv()
    store = QdrantOnlineStore()
    entity_key_bin = serialize_entity_key(
        _entity_key(2), entity_key_serialization_version=3
    )

    hit_point = MagicMock()
    hit_point.payload = {
        "entity_key": base64.b64encode(entity_key_bin).decode("ascii"),
        "feature_name": "embedding",
        "feature_value": "",
        "timestamp": "2024-01-01T12:00:00.000000",
    }
    hit_point.score = 0.75

    mock_client = MagicMock()
    mock_client.query_points.return_value = MagicMock(points=[hit_point])
    mock_client.scroll.return_value = ([], None)

    with patch.object(store, "_get_client", return_value=mock_client):
        store.retrieve_online_documents_v2(
            config=config,
            table=table,
            requested_features=["text_field"],
            embedding=[0.1, 0.2, 0.3],
            top_k=2,
            query_string="hello",
        )

    from qdrant_client import models

    query_kwargs = mock_client.query_points.call_args.kwargs
    assert len(query_kwargs["prefetch"]) == 2
    assert isinstance(query_kwargs["query"], models.FusionQuery)
    assert query_kwargs["limit"] == 4


def test_retrieve_online_documents_v2_hybrid_dedupes_entities_and_keeps_best_scores():
    config = _make_repo_config(text_search_enabled=True)
    table = _make_document_fv()
    store = QdrantOnlineStore()
    entity_key_a = serialize_entity_key(
        _entity_key(1), entity_key_serialization_version=3
    )
    entity_key_b = serialize_entity_key(
        _entity_key(2), entity_key_serialization_version=3
    )

    def _hit_point(entity_key_bin: bytes, feature_name: str, score: float):
        point = MagicMock()
        point.payload = {
            "entity_key": base64.b64encode(entity_key_bin).decode("ascii"),
            "feature_name": feature_name,
            "feature_value": "",
            "timestamp": "2024-01-01T12:00:00.000000",
        }
        point.score = score
        return point

    # Entity A appears twice (embedding then text_field with lower score).
    # Entity B appears once. With limit=top_k only one unique entity might
    # survive; with dedup + truncate we expect both up to top_k=2.
    points = [
        _hit_point(entity_key_a, "embedding", 0.9),
        _hit_point(entity_key_a, "text_field", 0.7),
        _hit_point(entity_key_b, "embedding", 0.8),
    ]

    mock_client = MagicMock()
    mock_client.query_points.return_value = MagicMock(points=points)
    mock_client.scroll.return_value = ([], None)

    with patch.object(store, "_get_client", return_value=mock_client):
        results = store.retrieve_online_documents_v2(
            config=config,
            table=table,
            requested_features=["text_field"],
            embedding=[0.1, 0.2, 0.3],
            top_k=2,
            query_string="hello",
        )

    assert len(results) == 2
    _, entity_a, features_a = results[0]
    _, entity_b, features_b = results[1]
    assert entity_a is not None
    assert entity_b is not None
    assert entity_a.entity_values[0].int64_val == 1
    assert entity_b.entity_values[0].int64_val == 2
    assert features_a is not None
    assert features_b is not None
    # In hybrid mode, distance holds the fused RRF score (first/best per entity).
    # text_rank is not populated because RRF doesn't preserve individual sub-scores.
    assert features_a["distance"].float_val == pytest.approx(0.9)
    assert "text_rank" not in features_a
    assert features_b["distance"].float_val == pytest.approx(0.8)


def test_online_write_batch_uses_document_for_sparse_vectors():
    from qdrant_client import models

    config = _make_repo_config(text_search_enabled=True)
    table = _make_document_fv()
    store = QdrantOnlineStore()
    entity_key = _entity_key(1)
    text_proto = ValueProto(string_val="hello world")
    embedding_proto = ValueProto(float_list_val=FloatListProto(val=[0.1, 0.2, 0.3]))

    mock_client = MagicMock()
    with patch.object(store, "_get_client", return_value=mock_client):
        store.online_write_batch(
            config=config,
            table=table,
            data=[
                (
                    entity_key,
                    {
                        "text_field": text_proto,
                        "embedding": embedding_proto,
                    },
                    datetime(2024, 1, 1, 12, 0, 0),
                    None,
                )
            ],
            progress=None,
        )

    points = mock_client.upload_points.call_args.kwargs["points"]
    text_point = next(p for p in points if p.payload["feature_name"] == "text_field")
    sparse_vector = text_point.vector["sparse"]
    assert isinstance(sparse_vector, models.Document)
    assert sparse_vector.text == "hello world"
    assert sparse_vector.model == "Qdrant/bm25"


def test_fetch_features_by_entity_keys_groups_points():
    store = QdrantOnlineStore()
    entity_key_bin = serialize_entity_key(
        _entity_key(3), entity_key_serialization_version=3
    )
    import base64

    text_proto = ValueProto(string_val="chunk")
    encoded = base64.b64encode(text_proto.SerializeToString()).decode("utf-8")

    point = MagicMock()
    point.payload = {
        "entity_key": base64.b64encode(entity_key_bin).decode("ascii"),
        "feature_name": "text_field",
        "feature_value": encoded,
    }
    mock_client = MagicMock()
    mock_client.scroll.return_value = ([point], None)

    features = store._fetch_features_by_entity_keys(
        mock_client,
        "documents",
        [entity_key_bin],
        ["text_field"],
    )

    assert entity_key_bin in features
    assert features[entity_key_bin]["text_field"].string_val == "chunk"
