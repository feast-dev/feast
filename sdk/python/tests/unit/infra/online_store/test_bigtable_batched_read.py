"""Unit tests for batching Bigtable online reads across feature views."""

import re
from datetime import timedelta
from typing import Dict, List, Tuple
from unittest.mock import patch

import pytest

pytest.importorskip("google.cloud.bigtable")

from pydantic import ValidationError  # noqa: E402

from feast import Entity, FeatureView  # noqa: E402
from feast.field import Field  # noqa: E402
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig  # noqa: E402
from feast.infra.online_stores.bigtable import (  # noqa: E402
    MAX_BATCHED_FILTER_BYTES,
    MAX_BATCHED_RESULT_ROWS,
    BigtableOnlineStore,
    BigtableOnlineStoreConfig,
)
from feast.infra.online_stores.online_store import OnlineStore  # noqa: E402
from feast.protos.feast.serving.ServingService_pb2 import (  # noqa: E402
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.EntityKey_pb2 import (  # noqa: E402
    EntityKey as EntityKeyProto,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto  # noqa: E402
from feast.repo_config import RepoConfig  # noqa: E402
from feast.types import Int64  # noqa: E402
from feast.utils import _utc_now  # noqa: E402
from feast.value_type import ValueType  # noqa: E402

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
        project="test_bigtable",
        provider="gcp",
        online_store=BigtableOnlineStoreConfig(
            instance="test-instance", **online_store
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )


class _Cell:
    def __init__(self, value: bytes):
        self.value = value


class _Row:
    def __init__(self, row_key: bytes, columns: Dict[bytes, bytes]):
        self.row_key = row_key
        # A fresh dict per read, as the client builds one per response.
        self.cells = {
            "features": {name: [_Cell(value)] for name, value in columns.items()}
        }


class _FakeBigtable:
    """Serves read_rows from in-memory tables and records every call."""

    def __init__(self):
        self.tables: Dict[str, Dict[bytes, Dict[bytes, bytes]]] = {}
        self.calls: List[Tuple[str, List[bytes]]] = []
        # (row key, column) of every feature cell returned, event_ts aside.
        self.cells: List[Tuple[bytes, bytes]] = []

    def instance(self, instance_id):
        return self

    def table(self, table_name):
        fake = self

        class _Table:
            def read_rows(self, row_set, filter_=None):
                fake.calls.append((table_name, list(row_set.row_keys)))
                pattern = re.compile(filter_.regex) if filter_ is not None else None
                stored = fake.tables.get(table_name, {})
                for row_key in row_set.row_keys:
                    if row_key in stored:
                        columns = {
                            name: value
                            for name, value in stored[row_key].items()
                            if pattern is None or pattern.match(name)
                        }
                        fake.cells.extend(
                            (row_key, name) for name in columns if name != b"event_ts"
                        )
                        yield _Row(row_key, columns)

        return _Table()


def _write(store, config, fake, fv: FeatureView, join_key: str, rows: dict) -> None:
    """Store ``{entity value: {feature: value}}`` the way online_write_batch does."""
    table = fake.tables.setdefault(
        store._get_table_name(config=config, feature_view=fv), {}
    )
    event_ts = _utc_now().isoformat().encode()
    for entity_value, values in rows.items():
        entity_key = EntityKeyProto(
            join_keys=[join_key], entity_values=[ValueProto(int64_val=entity_value)]
        )
        row_key = store._compute_row_key(
            entity_key=entity_key, feature_view_name=fv.name, config=config
        )
        table[row_key] = {
            **{
                name.encode(): ValueProto(int64_val=value).SerializeToString()
                for name, value in values.items()
            },
            b"event_ts": event_ts,
        }


def _drivers(*ids) -> dict:
    return {"driver_id": [ValueProto(int64_val=i) for i in ids]}


def _read(store, config, fake, grouped_refs, join_key_values, generic=False):
    """Run one read and return the response and the read_rows calls it made."""
    fake.calls.clear()
    fake.cells.clear()
    response = GetOnlineFeaturesResponse()
    read = OnlineStore._read_features_per_fv if generic else store._read_features_per_fv
    with patch.object(BigtableOnlineStore, "_get_client", return_value=fake):
        if generic:
            read(
                store,
                config,
                grouped_refs,
                join_key_values,
                JOIN_KEY_MAP,
                response,
                True,
                False,
            )
        else:
            read(
                config,
                grouped_refs,
                join_key_values,
                JOIN_KEY_MAP,
                response,
                True,
                False,
            )
    return response, list(fake.calls)


def _values(response) -> Dict[str, list]:
    names = response.metadata.feature_names.val
    return {
        name: [
            (value.int64_val if status == FieldStatus.PRESENT else None)
            for value, status in zip(vector.values, vector.statuses)
        ]
        for name, vector in zip(names, response.results)
    }


@pytest.fixture
def setup():
    store, config, fake = BigtableOnlineStore(), _config(), _FakeBigtable()
    return store, config, fake


def test_views_on_same_entities_share_one_read_rows_call(setup):
    store, config, fake = setup
    views = [_feature_view(f"fv{i}", [f"f{i}"]) for i in range(3)]
    for i, fv in enumerate(views):
        _write(store, config, fake, fv, "driver_id", {1: {f"f{i}": 10 + i}})
    grouped_refs = [(fv, [f"f{i}"]) for i, fv in enumerate(views)]

    response, calls = _read(store, config, fake, grouped_refs, _drivers(1, 2))
    generic, generic_calls = _read(
        store, config, fake, grouped_refs, _drivers(1, 2), generic=True
    )

    assert len(calls) == 1 and len(calls[0][1]) == 6
    assert len(generic_calls) == 3
    assert response == generic
    assert _values(response) == {
        "fv0__f0": [10, None],
        "fv1__f1": [11, None],
        "fv2__f2": [12, None],
    }


def test_views_on_different_entities_read_once_per_table(setup):
    store, config, fake = setup
    by_driver = _feature_view("by_driver", ["trips"])
    also_by_driver = _feature_view("also_by_driver", ["rating"])
    by_customer = _feature_view("by_customer", ["orders"], entity=CUSTOMER)
    _write(store, config, fake, by_driver, "driver_id", {1: {"trips": 5}})
    _write(store, config, fake, also_by_driver, "driver_id", {1: {"rating": 4}})
    _write(store, config, fake, by_customer, "customer_id", {7: {"orders": 3}})
    grouped_refs = [
        (by_driver, ["trips"]),
        (by_customer, ["orders"]),
        (also_by_driver, ["rating"]),
    ]
    join_key_values = {
        **_drivers(1, 1),
        "customer_id": [ValueProto(int64_val=7), ValueProto(int64_val=8)],
    }

    response, calls = _read(store, config, fake, grouped_refs, join_key_values)
    generic, _ = _read(store, config, fake, grouped_refs, join_key_values, generic=True)

    assert len(calls) == 2
    assert response == generic
    assert _values(response) == {
        "by_driver__trips": [5, 5],
        "by_customer__orders": [3, None],
        "also_by_driver__rating": [4, 4],
    }


def test_same_feature_name_does_not_leak_across_views(setup):
    store, config, fake = setup
    fv_a = _feature_view("fv_a", ["score", "extra"])
    fv_b = _feature_view("fv_b", ["score"])
    _write(store, config, fake, fv_a, "driver_id", {1: {"score": 1, "extra": 9}})
    _write(store, config, fake, fv_b, "driver_id", {2: {"score": 2}})
    grouped_refs = [(fv_a, ["score"]), (fv_b, ["score"])]

    response, calls = _read(store, config, fake, grouped_refs, _drivers(1, 2))

    assert len(calls) == 1
    assert _values(response) == {"fv_a__score": [1, None], "fv_b__score": [None, 2]}
    assert (
        response
        == _read(store, config, fake, grouped_refs, _drivers(1, 2), generic=True)[0]
    )


def test_one_view_under_two_aliases_reads_its_rows_once(setup):
    store, config, fake = setup
    fv = _feature_view("fv", ["a", "b"])
    _write(store, config, fake, fv, "driver_id", {1: {"a": 1, "b": 2}})
    grouped_refs = [(fv.with_name("first"), ["a"]), (fv.with_name("second"), ["b"])]

    response, calls = _read(store, config, fake, grouped_refs, _drivers(1, 3))

    assert len(calls) == 1 and len(calls[0][1]) == 2
    assert _values(response) == {"first__a": [1, None], "second__b": [2, None]}
    assert (
        response
        == _read(store, config, fake, grouped_refs, _drivers(1, 3), generic=True)[0]
    )


def test_single_view_keeps_the_generic_path(setup):
    store, config, fake = setup
    fv = _feature_view("fv", ["a"])
    _write(store, config, fake, fv, "driver_id", {1: {"a": 1}})

    with patch.object(
        OnlineStore, "_read_features_per_fv", autospec=True
    ) as generic_read:
        _read(store, config, fake, [(fv, ["a"])], _drivers(1))

    generic_read.assert_called_once()


@pytest.mark.parametrize("limit, expected_calls", [(0, 2), (3, 2), (4, 1)])
def test_max_batched_result_rows_limits_batching(setup, limit, expected_calls):
    store, _, fake = setup
    config = _config(max_batched_result_rows=limit)
    views = [_feature_view("fv_x", ["x"]), _feature_view("fv_y", ["y"])]
    grouped_refs = [(views[0], ["x"]), (views[1], ["y"])]

    # 2 entity rows x 2 feature views = 4 result rows.
    _, calls = _read(store, config, fake, grouped_refs, _drivers(1, 2))

    assert len(calls) == expected_calls


@pytest.mark.parametrize("extra_bytes, expected_calls", [(0, 1), (1, 2)])
def test_filter_over_the_size_limit_reads_one_view_at_a_time(
    setup, extra_bytes, expected_calls
):
    store, config, fake = setup
    # The filter of the batched read names the features of both views: 12 bytes of
    # regex syntax, then 570 bytes and a separator for each of the 28 names.
    names = [f"{i:02d}".ljust(570, "x") for i in range(28)]
    names[-1] += "x" * extra_bytes
    assert (
        len(BigtableOnlineStore._column_filter_regex(names))
        == MAX_BATCHED_FILTER_BYTES + extra_bytes
    )
    views = [_feature_view("fv_a", names[:14]), _feature_view("fv_b", names[14:])]
    grouped_refs = [(views[0], names[:14]), (views[1], names[14:])]
    for fv, features in grouped_refs:
        _write(
            store,
            config,
            fake,
            fv,
            "driver_id",
            {1: {name: n for n, name in enumerate(features)}},
        )

    response, calls = _read(store, config, fake, grouped_refs, _drivers(1, 2))
    generic, _ = _read(store, config, fake, grouped_refs, _drivers(1, 2), generic=True)

    assert len(calls) == expected_calls
    assert response == generic
    assert _values(response)[f"fv_b__{names[14]}"] == [0, None]


@pytest.mark.parametrize(
    "requested_a, requested_b, expected_calls",
    [
        # Both views store "score" and "extra". The shared filter names every
        # requested feature, so it would also return fv_a's "extra" and fv_b's "score".
        (["score"], ["extra"], 2),
        # fv_b is read without a column filter, which would return fv_a's "extra" too.
        (["score"], [], 2),
        (["score"], ["score"], 1),
        (["score", "extra"], ["extra", "score"], 1),
    ],
)
def test_unrequested_columns_are_not_read(
    setup, requested_a, requested_b, expected_calls
):
    store, config, fake = setup
    fv_a = _feature_view("fv_a", ["score", "extra"])
    fv_b = _feature_view("fv_b", ["score", "extra"])
    _write(store, config, fake, fv_a, "driver_id", {1: {"score": 1, "extra": 9}})
    _write(store, config, fake, fv_b, "driver_id", {1: {"score": 2, "extra": 8}})
    grouped_refs = [(fv_a, requested_a), (fv_b, requested_b)]

    response, calls = _read(store, config, fake, grouped_refs, _drivers(1, 2))
    cells = sorted(fake.cells)
    generic, _ = _read(store, config, fake, grouped_refs, _drivers(1, 2), generic=True)

    assert len(calls) == expected_calls
    assert cells == sorted(fake.cells)
    assert response == generic


def test_max_batched_result_rows_default_and_validation():
    config = BigtableOnlineStoreConfig(instance="test-instance")
    assert config.max_batched_result_rows == MAX_BATCHED_RESULT_ROWS == 2048
    with pytest.raises(ValidationError):
        BigtableOnlineStoreConfig(instance="test-instance", max_batched_result_rows=-1)
