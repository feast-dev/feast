"""Unit tests for the Milvus online store, run against Milvus Lite or a mocked client."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from pymilvus import DataType, MilvusClient
from pymilvus.client.types import LoadState

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.milvus_online_store.milvus import (
    PLACEHOLDER_VECTOR_DIM,
    PLACEHOLDER_VECTOR_FIELD,
    MilvusOnlineStore,
    MilvusOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64, String
from feast.value_type import ValueType

MILVUS_MODULE = "feast.infra.online_stores.milvus_online_store.milvus"


def _lite_config(tmp_path: Path, **online_store: Any) -> RepoConfig:
    return RepoConfig(
        project="test_milvus",
        provider="local",
        registry=str(tmp_path / "registry.db"),
        online_store={
            "type": "milvus",
            "path": str(tmp_path / "online_store.db"),
            "embedding_dim": 2,
            **online_store,
        },
        entity_key_serialization_version=3,
        repo_path=tmp_path,
    )


def _mock_config(**online_store: Any) -> MagicMock:
    config = MagicMock()
    config.project = "test_milvus"
    config.entity_key_serialization_version = 3
    config.registry.enable_online_feature_view_versioning = False
    config.provider = "local"
    config.repo_path = None
    config.online_store = MilvusOnlineStoreConfig(**online_store)
    return config


def _mock_client(mock_client_cls: MagicMock, has_collection: bool) -> MagicMock:
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.has_collection.return_value = has_collection
    mock_client.prepare_index_params.side_effect = MilvusClient.prepare_index_params
    return mock_client


def _created_indexes(mock_client: MagicMock) -> Dict[str, Dict[str, Any]]:
    """Map field name -> index definition for indexes passed to the client."""
    index_params = []
    for call in (
        mock_client.create_collection.call_args,
        mock_client.create_index.call_args,
    ):
        if call is not None and call.kwargs.get("index_params") is not None:
            index_params.extend(call.kwargs["index_params"])
    return {index.field_name: index.to_dict() for index in index_params}


def _driver_entity() -> Entity:
    return Entity(name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64)


def _scalar_feature_view(name: str = "driver_stats") -> FeatureView:
    return FeatureView(
        name=name,
        entities=[_driver_entity()],
        ttl=timedelta(days=1),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips_today", dtype=Float32),
            Field(name="city", dtype=String),
        ],
    )


def _entity_key(driver_id: int) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["driver_id"], entity_values=[ValueProto(int64_val=driver_id)]
    )


def _write_rows(
    store: MilvusOnlineStore,
    config: RepoConfig,
    fv: FeatureView,
    rows: Dict[int, Dict[str, ValueProto]],
) -> None:
    now = datetime.now(timezone.utc)
    store.online_write_batch(
        config,
        fv,
        [(_entity_key(k), dict(v), now, now) for k, v in rows.items()],
        progress=None,
    )


def _read(
    store: MilvusOnlineStore,
    config: RepoConfig,
    fv: FeatureView,
    driver_ids: List[int],
    features: List[str],
) -> List[Optional[Dict[str, ValueProto]]]:
    results = store.online_read(
        config, fv, [_entity_key(d) for d in driver_ids], features
    )
    return [values for _, values in results]


def test_scalar_feature_view_round_trip_with_placeholder(tmp_path: Path) -> None:
    config = _lite_config(tmp_path)
    fv = _scalar_feature_view()
    store = MilvusOnlineStore()
    store.update(config, [], [fv], [], [], partial=False)

    _write_rows(
        store,
        config,
        fv,
        {
            1: {
                "trips_today": ValueProto(float_val=3.0),
                "city": ValueProto(string_val="Paris"),
            },
            2: {
                "trips_today": ValueProto(float_val=5.0),
                "city": ValueProto(string_val="Rome"),
            },
        },
    )
    rows = _read(store, config, fv, [1, 2, 3], ["trips_today", "city"])

    assert rows[0] is not None and rows[0]["city"].string_val == "Paris"
    assert rows[1] is not None and rows[1]["trips_today"].float_val == 5.0
    assert rows[2] is None

    collection = store.client.describe_collection("test_milvus_driver_stats")
    placeholder = next(
        f for f in collection["fields"] if f["name"] == PLACEHOLDER_VECTOR_FIELD
    )
    assert placeholder["params"]["dim"] == PLACEHOLDER_VECTOR_DIM


@patch(f"{MILVUS_MODULE}.MilvusClient")
def test_placeholder_vector_is_indexed_and_valid(mock_client_cls: MagicMock) -> None:
    mock_client = _mock_client(mock_client_cls, has_collection=False)

    store = MilvusOnlineStore()
    store._get_or_create_collection(_mock_config(), _scalar_feature_view())

    schema = mock_client.create_collection.call_args.kwargs["schema"]
    placeholder = next(f for f in schema.fields if f.name == PLACEHOLDER_VECTOR_FIELD)
    assert placeholder.dtype == DataType.FLOAT_VECTOR
    # Milvus servers reject vectors with fewer than two dimensions.
    assert placeholder.params["dim"] >= 2

    assert PLACEHOLDER_VECTOR_FIELD in _created_indexes(mock_client)


@patch(f"{MILVUS_MODULE}.MilvusClient")
def test_placeholder_values_are_finite_and_match_collection_dim(
    mock_client_cls: MagicMock,
) -> None:
    mock_client = _mock_client(mock_client_cls, has_collection=True)
    # A collection created by an older Feast version, with a 1-dim placeholder.
    mock_client.describe_collection.return_value = {
        "collection_name": "test_milvus_driver_stats",
        "fields": [
            {"name": "driver_id_pk", "type": DataType.VARCHAR, "params": {}},
            {"name": "driver_id", "type": DataType.VARCHAR, "params": {}},
            {"name": "event_ts", "type": DataType.INT64, "params": {}},
            {"name": "created_ts", "type": DataType.INT64, "params": {}},
            {"name": "trips_today", "type": DataType.VARCHAR, "params": {}},
            {"name": "city", "type": DataType.VARCHAR, "params": {}},
            {
                "name": PLACEHOLDER_VECTOR_FIELD,
                "type": DataType.FLOAT_VECTOR,
                "params": {"dim": 1},
            },
        ],
    }

    store = MilvusOnlineStore()
    config = _mock_config()
    _write_rows(
        store,
        config,
        _scalar_feature_view(),
        {
            1: {
                "trips_today": ValueProto(float_val=1.0),
                "city": ValueProto(string_val="Oslo"),
            }
        },
    )

    data = mock_client.upsert.call_args.kwargs["data"]
    assert data[0][PLACEHOLDER_VECTOR_FIELD] == [0.0]


def _existing_collection_description() -> Dict[str, Any]:
    return {
        "collection_name": "test_milvus_driver_stats",
        "fields": [
            {"name": "driver_id_pk", "type": DataType.VARCHAR, "params": {}},
            {"name": "driver_id", "type": DataType.VARCHAR, "params": {}},
            {"name": "event_ts", "type": DataType.INT64, "params": {}},
            {"name": "created_ts", "type": DataType.INT64, "params": {}},
            {"name": "trips_today", "type": DataType.VARCHAR, "params": {}},
            {"name": "city", "type": DataType.VARCHAR, "params": {}},
            {
                "name": PLACEHOLDER_VECTOR_FIELD,
                "type": DataType.FLOAT_VECTOR,
                "params": {"dim": PLACEHOLDER_VECTOR_DIM},
            },
        ],
    }


@pytest.mark.parametrize(
    "load_state, expected_loads",
    [(LoadState.Loaded, 0), (LoadState.NotLoad, 1)],
)
@patch(f"{MILVUS_MODULE}.MilvusClient")
def test_existing_collection_is_loaded_at_most_once(
    mock_client_cls: MagicMock, load_state: LoadState, expected_loads: int
) -> None:
    mock_client = _mock_client(mock_client_cls, has_collection=True)
    mock_client.describe_collection.return_value = _existing_collection_description()
    mock_client.get_load_state.return_value = {"state": load_state}
    mock_client.query.return_value = []

    store = MilvusOnlineStore()
    config = _mock_config()
    fv = _scalar_feature_view()
    for _ in range(5):
        store.online_read(config, fv, [_entity_key(1)], ["city"])

    assert mock_client.load_collection.call_count == expected_loads
    assert mock_client.query.call_count == 5


@patch(f"{MILVUS_MODULE}.MilvusClient")
def test_new_collection_is_created_with_indexes_so_it_loads(
    mock_client_cls: MagicMock,
) -> None:
    mock_client = _mock_client(mock_client_cls, has_collection=False)

    store = MilvusOnlineStore()
    store._get_or_create_collection(_mock_config(), _scalar_feature_view())

    # MilvusClient.create_collection loads the collection when it is given
    # index params; creating indexes separately would leave it unloaded.
    assert mock_client.create_collection.call_args.kwargs["index_params"]
    mock_client.create_index.assert_not_called()
    mock_client.load_collection.assert_not_called()


def test_load_collection_not_called_per_query(tmp_path: Path) -> None:
    config = _lite_config(tmp_path)
    fv = _scalar_feature_view()
    store = MilvusOnlineStore()
    store.update(config, [], [fv], [], [], partial=False)
    _write_rows(
        store,
        config,
        fv,
        {
            1: {
                "trips_today": ValueProto(float_val=1.0),
                "city": ValueProto(string_val="Oslo"),
            }
        },
    )

    assert store.client is not None
    with patch.object(
        store.client, "load_collection", wraps=store.client.load_collection
    ) as load_spy:
        for _ in range(3):
            _read(store, config, fv, [1], ["city"])
            store.retrieve_online_documents_v2(
                config, fv, ["city"], embedding=None, top_k=1, query_string="Oslo"
            )

    assert load_spy.call_count == 0


@pytest.mark.parametrize(
    "online_store, expected_kwargs",
    [
        # Existing configs build exactly the same client arguments as before.
        ({}, {"uri": "http://localhost:19530", "token": ""}),
        (
            {
                "host": "https://milvus.internal",
                "port": 443,
                "username": "u",
                "password": "p",
            },
            {"uri": "https://milvus.internal:443", "token": "u:p"},
        ),
        # token takes precedence over username/password.
        (
            {"username": "u", "password": "p", "token": "api-key"},
            {"uri": "http://localhost:19530", "token": "api-key"},
        ),
        # uri takes precedence over host/port.
        (
            {
                "uri": "https://in01-abc.zillizcloud.com",
                "host": "http://ignored",
                "token": "k",
            },
            {"uri": "https://in01-abc.zillizcloud.com", "token": "k"},
        ),
        (
            {
                "uri": "https://in01-abc.zillizcloud.com",
                "token": "k",
                "db_name": "catalog",
            },
            {
                "uri": "https://in01-abc.zillizcloud.com",
                "token": "k",
                "db_name": "catalog",
            },
        ),
    ],
)
@patch(f"{MILVUS_MODULE}.MilvusClient")
def test_remote_client_arguments(
    mock_client_cls: MagicMock,
    online_store: Dict[str, Any],
    expected_kwargs: Dict[str, Any],
) -> None:
    config = _mock_config(**online_store)
    config.provider = "gcp"

    MilvusOnlineStore()._connect(config)

    mock_client_cls.assert_called_once_with(**expected_kwargs)


@patch(f"{MILVUS_MODULE}.MilvusClient")
def test_uri_takes_precedence_over_lite_path(mock_client_cls: MagicMock) -> None:
    config = _mock_config(path="online_store.db", uri="http://milvus:19530", token="k")

    MilvusOnlineStore()._connect(config)

    mock_client_cls.assert_called_once_with(uri="http://milvus:19530", token="k")


@patch(f"{MILVUS_MODULE}.MilvusClient")
def test_lite_path_used_without_uri(mock_client_cls: MagicMock) -> None:
    config = _mock_config(path="/tmp/online_store.db", token="ignored")

    MilvusOnlineStore()._connect(config)

    mock_client_cls.assert_called_once_with("/tmp/online_store.db")
