"""Unit tests for batching PostgreSQL online reads across feature views."""

import contextlib
from datetime import datetime, timedelta
from typing import List, Tuple
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.postgres_online_store.postgres import (
    MAX_BATCHED_RESULT_ROWS,
    PostgreSQLOnlineStore,
)
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig, RepoConfig
from feast.types import Int64
from feast.value_type import ValueType

DRIVER = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
CUSTOMER = Entity(
    name="customer", join_keys=["customer_id"], value_type=ValueType.INT64
)
JOIN_KEY_MAP = {"driver": "driver_id", "customer": "customer_id"}
TS = datetime(2026, 1, 1)


def _feature_view(name: str, feature: str, entity: Entity = DRIVER) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name=feature, dtype=Int64)],
    )


def _config(**online_store) -> RepoConfig:
    return RepoConfig(
        project="proj",
        provider="local",
        registry=RegistryConfig(path="registry.db"),
        online_store={
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "test",
            "db_schema": "public",
            "user": "root",
            "password": "test",  # pragma: allowlist secret
            **online_store,
        },
        entity_key_serialization_version=3,
    )


def _int(value: int) -> bytes:
    return ValueProto(int64_val=value).SerializeToString()


class _FakeCursor:
    """Records executed statements and replays canned rows by iteration."""

    def __init__(self, rows: List[Tuple], executed: List):
        self._rows = rows
        self._executed = executed

    def execute(self, query, params=None):
        self._executed.append((query, params))

    def __iter__(self):
        return iter(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeConnection:
    def __init__(self, rows: List[Tuple], executed: List):
        self._rows = rows
        self._executed = executed

    def cursor(self):
        return _FakeCursor(self._rows, self._executed)


@contextlib.contextmanager
def _fake_conn(rows: List[Tuple], executed: List):
    yield _FakeConnection(rows, executed)


class _AsyncFakeCursor(_FakeCursor):
    async def execute(self, query, params=None):  # type: ignore[override]
        self._executed.append((query, params))

    def __aiter__(self):
        async def gen():
            for row in self._rows:
                yield row

        return gen()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class _AsyncFakeConnection(_FakeConnection):
    def cursor(self):  # type: ignore[override]
        return _AsyncFakeCursor(self._rows, self._executed)


@contextlib.asynccontextmanager
async def _fake_conn_async(rows: List[Tuple], executed: List):
    yield _AsyncFakeConnection(rows, executed)


def _args(grouped_refs, join_key_values, response, config=None):
    return dict(
        config=config or _config(),
        grouped_refs=grouped_refs,
        join_key_values=join_key_values,
        entity_name_to_join_key_map=JOIN_KEY_MAP,
        online_features_response=response,
        full_feature_names=False,
        include_feature_view_version_metadata=False,
    )


def _drivers(*ids) -> dict:
    return {"driver_id": [ValueProto(int64_val=i) for i in ids]}


def _read(store, grouped_refs, rows, join_key_values=None, config=None):
    """Drive the sync override against a fake connection."""
    executed: List = []
    response = GetOnlineFeaturesResponse()
    with patch.object(
        PostgreSQLOnlineStore,
        "_get_conn",
        lambda self, cfg, autocommit=False: _fake_conn(rows, executed),
    ):
        store._read_features_per_fv(
            **_args(grouped_refs, join_key_values or _drivers(1, 2), response, config)
        )
    return executed, response


async def _read_async(store, grouped_refs, rows, join_key_values=None, config=None):
    """Drive the async override against a fake connection."""
    executed: List = []
    response = GetOnlineFeaturesResponse()
    with patch.object(
        PostgreSQLOnlineStore,
        "_get_conn_async",
        lambda self, cfg, autocommit=False: _fake_conn_async(rows, executed),
    ):
        await store._read_features_per_fv_async(
            **_args(grouped_refs, join_key_values or _drivers(1, 2), response, config)
        )
    return executed, response


async def _read_sync(store, grouped_refs, rows, join_key_values=None, config=None):
    """Await-compatible wrapper so both overrides share one test body."""
    return _read(store, grouped_refs, rows, join_key_values, config)


READERS = [pytest.param(_read_sync, id="sync"), pytest.param(_read_async, id="async")]


@pytest.mark.parametrize("read", READERS)
async def test_views_read_in_one_query_and_demultiplexed_by_tag(read):
    """Two views sharing an entity key AND a feature name come back in one query.

    Distinct feature names make a demux failure invisible: _process_rows keys values
    by feature name, so a cross-contaminated bucket still yields the right value.
    """
    store = PostgreSQLOnlineStore()
    grouped_refs = [
        (_feature_view("fv_a", "feat"), ["feat"]),
        (_feature_view("fv_b", "feat"), ["feat"]),
    ]
    reads = store._prepare_batched_reads(
        _config(), grouped_refs, _drivers(1, 2), JOIN_KEY_MAP
    )
    shared_key = reads[0].keys[0]
    assert shared_key == reads[1].keys[0]
    rows = [
        (0, shared_key, "feat", _int(11), TS),
        (1, shared_key, "feat", _int(22), TS),
    ]

    executed, response = await read(store, grouped_refs, rows)

    assert len(executed) == 1
    statement = executed[0][0].as_string(None)
    assert statement.count("UNION ALL") == 1
    for table in ("proj_fv_a", "proj_fv_b"):
        assert table in statement
    assert response.results[0].values[0].int64_val == 11
    assert response.results[1].values[0].int64_val == 22


def test_per_view_entity_bookkeeping_is_not_shared():
    """Views on different entities must each use their own idxs and output_len.

    Every view here resolves a different number of unique entities, so reusing one
    view's index mapping for another shows up as a wrong or misplaced value.
    """
    store = PostgreSQLOnlineStore()
    fv_driver = _feature_view("fv_driver", "d_feat", DRIVER)
    fv_customer = _feature_view("fv_customer", "c_feat", CUSTOMER)
    grouped_refs = [(fv_driver, ["d_feat"]), (fv_customer, ["c_feat"])]
    # Three request rows; the driver view sees 3 distinct keys, the customer view 2.
    join_key_values = {
        "driver_id": [ValueProto(int64_val=i) for i in (1, 2, 3)],
        "customer_id": [ValueProto(int64_val=i) for i in (7, 7, 8)],
    }

    reads = store._prepare_batched_reads(
        _config(), grouped_refs, join_key_values, JOIN_KEY_MAP
    )
    assert len(reads[0].keys) == 3
    assert len(reads[1].keys) == 2

    rows = [(0, reads[0].keys[i], "d_feat", _int(10 + i), TS) for i in range(3)] + [
        (1, reads[1].keys[i], "c_feat", _int(70 + i), TS) for i in range(2)
    ]

    _, response = _read(store, grouped_refs, rows, join_key_values)

    assert [v.int64_val for v in response.results[0].values] == [10, 11, 12]
    # customer 7 is requested twice and must fan back out to both output positions
    assert [v.int64_val for v in response.results[1].values] == [70, 70, 71]


@pytest.mark.parametrize("read", READERS)
@pytest.mark.parametrize(
    ("views", "entity_rows", "online_store", "exp_batched"),
    [
        (2, 2, {}, True),
        (2, MAX_BATCHED_RESULT_ROWS, {}, False),
        (1, 2, {}, False),
        (2, 2, {"max_batched_result_rows": 4}, True),
        (2, 2, {"max_batched_result_rows": 3}, False),
        (2, 2, {"max_batched_result_rows": 0}, False),
    ],
    ids=[
        "under_default_limit",
        "over_default_limit",
        "single_view",
        "at_configured_limit",
        "over_configured_limit",
        "zero_disables_batching",
    ],
)
async def test_when_batching_applies(
    read, views, entity_rows, online_store, exp_batched
):
    """Batching needs at least two views and a request within the row limit.

    A batched request issues one statement and never calls online_read; the generic
    path issues none itself and reads each view through online_read instead.
    """
    store = PostgreSQLOnlineStore()
    names = [f"fv_{i}" for i in range(views)]
    grouped_refs = [(_feature_view(n, f"feat_{n}"), [f"feat_{n}"]) for n in names]
    per_view_reads: List[str] = []

    def one_row_per_key(self, config, table, entity_keys, requested_features=None):
        per_view_reads.append(table.name)
        return [(None, None)] * len(entity_keys)

    async def one_row_per_key_async(self, *args, **kwargs):
        return one_row_per_key(self, *args, **kwargs)

    with (
        patch.object(PostgreSQLOnlineStore, "online_read", one_row_per_key),
        patch.object(PostgreSQLOnlineStore, "online_read_async", one_row_per_key_async),
    ):
        executed, _ = await read(
            store,
            grouped_refs,
            [],
            _drivers(*range(entity_rows)),
            _config(**online_store),
        )

    assert len(executed) == (1 if exp_batched else 0)
    assert sorted(per_view_reads) == ([] if exp_batched else names)


def test_max_batched_result_rows_config():
    """Defaults to 2048, and rejects negatives rather than silently never batching."""
    assert _config().online_store.max_batched_result_rows == 2048
    with pytest.raises(ValidationError):
        _config(max_batched_result_rows=-1)


@pytest.mark.parametrize(
    ("requested", "exp_feature_filter", "exp_params"),
    [(["feat_a"], True, 2), ([], False, 1)],
    ids=["with_requested_features", "without_requested_features"],
)
def test_query_shape_with_and_without_requested_features(
    requested, exp_feature_filter, exp_params
):
    """Omitting requested features must drop the filter, not pass an empty one."""
    store = PostgreSQLOnlineStore()
    config = _config()
    reads = store._prepare_batched_reads(
        config,
        [(_feature_view("fv_a", "feat_a"), requested)],
        _drivers(1),
        JOIN_KEY_MAP,
    )

    query, params = store._construct_batched_query_and_params(config, reads)
    where_clause = query.as_string(None).split("WHERE")[1]

    assert ("feature_name" in where_clause) is exp_feature_filter
    assert len(params) == exp_params
