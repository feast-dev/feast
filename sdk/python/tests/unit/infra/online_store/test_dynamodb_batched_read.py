"""Unit tests for batching DynamoDB online reads across feature views."""

from datetime import timedelta
from typing import List
from unittest.mock import patch

import pytest
from moto import mock_dynamodb
from pydantic import ValidationError

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.online_stores.dynamodb import (
    MAX_BATCHED_RESULT_ROWS,
    DynamoDBOnlineStore,
    DynamoDBOnlineStoreConfig,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Int64
from feast.utils import _utc_now
from feast.value_type import ValueType
from tests.utils.dynamo_table_creator import create_test_table

PROJECT = "test_aws"
REGION = "us-west-2"

DRIVER = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
CUSTOMER = Entity(
    name="customer", join_keys=["customer_id"], value_type=ValueType.INT64
)
JOIN_KEY_MAP = {"driver": "driver_id", "customer": "customer_id"}


def _feature_view(
    name: str, features: List[str], entity: Entity = DRIVER
) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name=feature, dtype=Int64) for feature in features],
    )


def _config(**online_store) -> RepoConfig:
    return RepoConfig(
        registry="s3://test_registry/registry.db",
        project=PROJECT,
        provider="aws",
        online_store=DynamoDBOnlineStoreConfig(region=REGION, **online_store),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )


def _write(store, config, fv: FeatureView, join_key: str, rows: dict) -> None:
    """Create the view's table and write ``{entity value: {feature: value}}``."""
    create_test_table(PROJECT, fv.name, REGION)
    store.online_write_batch(
        config,
        fv,
        [
            (
                EntityKeyProto(
                    join_keys=[join_key],
                    entity_values=[ValueProto(int64_val=entity_value)],
                ),
                {name: ValueProto(int64_val=value) for name, value in values.items()},
                _utc_now(),
                None,
            )
            for entity_value, values in rows.items()
        ],
        None,
    )


@pytest.fixture
def dynamodb():
    # The moto decorator does not await coroutines, so the async tests get the
    # mock from a fixture instead.
    with mock_dynamodb():
        yield


def _drivers(*ids) -> dict:
    return {"driver_id": [ValueProto(int64_val=i) for i in ids]}


def _args(config, grouped_refs, join_key_values, response):
    return dict(
        config=config,
        grouped_refs=grouped_refs,
        join_key_values=join_key_values,
        entity_name_to_join_key_map=JOIN_KEY_MAP,
        online_features_response=response,
        full_feature_names=True,
        include_feature_view_version_metadata=False,
    )


class _AsyncClient:
    """Serves the async client calls from the moto-backed sync client."""

    def __init__(self, client):
        self._client = client

    async def batch_get_item(self, **kwargs):
        return self._client.batch_get_item(**kwargs)


class _CountingClient:
    """Wraps a client and records the RequestItems of every batch_get_item call."""

    def __init__(self, client, calls: List):
        self._client = client
        self._calls = calls

    def batch_get_item(self, **kwargs):
        self._calls.append(kwargs["RequestItems"])
        return self._client.batch_get_item(**kwargs)


def _read_sync(store, config, grouped_refs, join_key_values, generic=False):
    response = GetOnlineFeaturesResponse()
    read = (
        OnlineStore._read_features_per_fv
        if generic
        else type(store)._read_features_per_fv
    )
    read(store, **_args(config, grouped_refs, join_key_values, response))
    return response


async def _read_async(store, config, grouped_refs, join_key_values, generic=False):
    response = GetOnlineFeaturesResponse()
    read = (
        OnlineStore._read_features_per_fv_async
        if generic
        else type(store)._read_features_per_fv_async
    )
    client = _AsyncClient(store._get_dynamodb_client(REGION))

    async def get_client(*args, **kwargs):
        return client

    with patch.object(store, "_get_aiodynamodb_client", get_client):
        await read(store, **_args(config, grouped_refs, join_key_values, response))
    return response


async def _read_sync_awaitable(*args, **kwargs):
    """Await-compatible wrapper so both overrides share one test body."""
    return _read_sync(*args, **kwargs)


READERS = [
    pytest.param(_read_sync_awaitable, id="sync"),
    pytest.param(_read_async, id="async"),
]


def _values(response: GetOnlineFeaturesResponse) -> dict:
    """Map each feature name to its (values, statuses) across entity rows."""
    return {
        name: (
            [value.int64_val for value in vector.values],
            list(vector.statuses),
        )
        for name, vector in zip(response.metadata.feature_names.val, response.results)
    }


@pytest.mark.usefixtures("dynamodb")
@pytest.mark.parametrize("read", READERS)
async def test_views_read_in_one_request_match_generic_path(read):
    """Views sharing an entity id AND a feature name come back from one request.

    Distinct feature names would make a cross-wired table invisible, since the
    response is keyed by feature name.
    """
    config = _config()
    store = DynamoDBOnlineStore()
    fv_a = _feature_view("fv_a", ["feat"])
    fv_b = _feature_view("fv_b", ["feat"])
    fv_c = _feature_view("fv_c", ["visits"], entity=CUSTOMER)
    _write(store, config, fv_a, "driver_id", {1: {"feat": 11}, 2: {"feat": 12}})
    # Driver 2 only exists in fv_a, so fv_b must report it as not found.
    _write(store, config, fv_b, "driver_id", {1: {"feat": 21}})
    _write(store, config, fv_c, "customer_id", {7: {"visits": 70}})

    grouped_refs = [(fv_a, ["feat"]), (fv_b, ["feat"]), (fv_c, ["visits"])]
    join_key_values = {
        **_drivers(1, 2, 1),
        "customer_id": [ValueProto(int64_val=i) for i in (7, 8, 7)],
    }

    calls: List = []
    store._dynamodb_client = _CountingClient(store._get_dynamodb_client(REGION), calls)
    batched = await read(store, config, grouped_refs, join_key_values)
    assert len(calls) == 1
    assert set(calls[0]) == {"test_aws.fv_a", "test_aws.fv_b", "test_aws.fv_c"}

    generic = await read(store, config, grouped_refs, join_key_values, generic=True)
    assert batched == generic

    found, missing = FieldStatus.PRESENT, FieldStatus.NOT_FOUND
    assert _values(batched) == {
        "fv_a__feat": ([11, 12, 11], [found, found, found]),
        "fv_b__feat": ([21, 0, 21], [found, missing, found]),
        "fv_c__visits": ([70, 0, 70], [found, missing, found]),
    }


@pytest.mark.usefixtures("dynamodb")
@pytest.mark.parametrize("read", READERS)
async def test_keys_are_packed_across_views_up_to_batch_size(read):
    """Requests are filled with keys of several tables, never above batch_size."""
    config = _config(batch_size=3)
    store = DynamoDBOnlineStore()
    fv_a = _feature_view("fv_a", ["feat"])
    fv_b = _feature_view("fv_b", ["feat"])
    _write(store, config, fv_a, "driver_id", {i: {"feat": i} for i in range(1, 5)})
    _write(store, config, fv_b, "driver_id", {i: {"feat": -i} for i in range(1, 5)})

    calls: List = []
    store._dynamodb_client = _CountingClient(store._get_dynamodb_client(REGION), calls)
    response = await read(
        store,
        config,
        [(fv_a, ["feat"]), (fv_b, ["feat"])],
        _drivers(1, 2, 3, 4),
    )

    # 8 keys at 3 per request; the second request mixes both tables.
    assert [
        sum(len(table["Keys"]) for table in request.values()) for request in calls
    ] == [3, 3, 2]
    assert set(calls[1]) == {"test_aws.fv_a", "test_aws.fv_b"}
    assert _values(response) == {
        "fv_a__feat": ([1, 2, 3, 4], [FieldStatus.PRESENT] * 4),
        "fv_b__feat": ([-1, -2, -3, -4], [FieldStatus.PRESENT] * 4),
    }


@pytest.mark.usefixtures("dynamodb")
@pytest.mark.parametrize("read", READERS)
async def test_views_sharing_a_table_read_each_key_once(read):
    """One view under two aliases maps to one table and must not repeat its keys.

    BatchGetItem rejects a request that lists the same key twice for a table, so
    the aliases are read once, projecting the union of their features.
    """
    config = _config()
    store = DynamoDBOnlineStore()
    fv = _feature_view("fv", ["feat_x", "feat_y", "feat_z"])
    _write(
        store,
        config,
        fv,
        "driver_id",
        {1: {"feat_x": 1, "feat_y": 2, "feat_z": 3}},
    )

    calls: List = []
    store._dynamodb_client = _CountingClient(store._get_dynamodb_client(REGION), calls)
    response = await read(
        store,
        config,
        [
            (fv.with_name("first"), ["feat_x"]),
            (fv.with_name("second"), ["feat_y"]),
        ],
        _drivers(1),
    )

    assert len(calls) == 1
    assert list(calls[0]) == ["test_aws.fv"]
    request = calls[0]["test_aws.fv"]
    assert len(request["Keys"]) == 1
    assert set(request["ExpressionAttributeNames"].values()) == {
        "entity_id",
        "event_ts",
        "values",
        "feat_x",
        "feat_y",
    }
    assert _values(response) == {
        "first__feat_x": ([1], [FieldStatus.PRESENT]),
        "second__feat_y": ([2], [FieldStatus.PRESENT]),
    }


@pytest.mark.usefixtures("dynamodb")
@pytest.mark.parametrize("read", READERS)
async def test_requested_features_are_projected_per_table(read):
    config = _config()
    store = DynamoDBOnlineStore()
    fv_a = _feature_view("fv_a", ["kept", "dropped"])
    fv_b = _feature_view("fv_b", ["other"])
    _write(store, config, fv_a, "driver_id", {1: {"kept": 1, "dropped": 2}})
    _write(store, config, fv_b, "driver_id", {1: {"other": 3}})

    calls: List = []
    store._dynamodb_client = _CountingClient(store._get_dynamodb_client(REGION), calls)
    await read(store, config, [(fv_a, ["kept"]), (fv_b, ["other"])], _drivers(1))

    projected = {
        table_name: set(request["ExpressionAttributeNames"].values())
        - {"entity_id", "event_ts", "values"}
        for table_name, request in calls[0].items()
    }
    assert projected == {"test_aws.fv_a": {"kept"}, "test_aws.fv_b": {"other"}}


@pytest.mark.parametrize(
    "online_store, n_views, n_rows",
    [
        pytest.param({}, 1, 1, id="single_view"),
        pytest.param({"max_batched_result_rows": 0}, 2, 1, id="disabled"),
        pytest.param({"max_batched_result_rows": 5}, 2, 3, id="over_limit"),
    ],
)
@pytest.mark.parametrize("read", READERS)
async def test_generic_path_reads_each_view(read, online_store, n_views, n_rows):
    """Outside the gate, every view is read by its own online_read call."""
    config = _config(**online_store)
    store = DynamoDBOnlineStore()
    grouped_refs = [
        (_feature_view(f"fv_{i}", ["feat"]), ["feat"]) for i in range(n_views)
    ]
    read_tables: List[str] = []

    def online_read(self, config, table, entity_keys, requested_features=None):
        read_tables.append(table.name)
        return [(None, None)] * len(entity_keys)

    async def online_read_async(self, *args, **kwargs):
        return online_read(self, *args, **kwargs)

    with (
        patch.object(DynamoDBOnlineStore, "online_read", online_read),
        patch.object(DynamoDBOnlineStore, "online_read_async", online_read_async),
        patch.object(
            DynamoDBOnlineStore,
            "_to_batched_request_items",
            side_effect=AssertionError,
        ),
    ):
        await read(store, config, grouped_refs, _drivers(*range(n_rows)))

    assert read_tables == [f"fv_{i}" for i in range(n_views)]


def test_batching_at_the_limit():
    grouped_refs = [(None, []), (None, [])]
    config = _config(max_batched_result_rows=4)
    assert DynamoDBOnlineStore._should_batch(config, grouped_refs, _drivers(1, 2))
    assert not DynamoDBOnlineStore._should_batch(
        config, grouped_refs, _drivers(1, 2, 3)
    )


def test_max_batched_result_rows_config():
    assert (
        DynamoDBOnlineStoreConfig(region=REGION).max_batched_result_rows
        == MAX_BATCHED_RESULT_ROWS
    )
    assert (
        DynamoDBOnlineStoreConfig(
            region=REGION, max_batched_result_rows=0
        ).max_batched_result_rows
        == 0
    )
    with pytest.raises(ValidationError):
        DynamoDBOnlineStoreConfig(region=REGION, max_batched_result_rows=-1)
