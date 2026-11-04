from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List
from unittest.mock import patch

import pytest

pytest.importorskip("google.cloud.bigtable")

from concurrent import futures  # noqa: E402

import feast.infra.online_stores.bigtable as bigtable_module  # noqa: E402
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig  # noqa: E402
from feast.infra.online_stores.bigtable import (  # noqa: E402
    BigtableOnlineStore,
    BigtableOnlineStoreConfig,
)
from feast.repo_config import RepoConfig  # noqa: E402


@dataclass
class MockFeatureView:
    name: str
    features: List[object] = field(default_factory=list)


def _make_repo_config(**online_store_kwargs) -> RepoConfig:
    return RepoConfig(
        registry="s3://test_registry/registry.db",
        project="test_bigtable",
        provider="local",
        online_store=BigtableOnlineStoreConfig(
            instance="test-instance",
            project_id="test-project",
            **online_store_kwargs,
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )


def _rows(n: int):
    # Contents are irrelevant: _write_rows_to_bt is mocked, only the slicing
    # performed by online_write_batch is under test.
    return [(None, {}, datetime.now(timezone.utc), None) for _ in range(n)]


def _run_write(store, config, feature_view, data, captured_chunk_sizes):
    def _capture(rows_to_write, **_):
        captured_chunk_sizes.append(len(rows_to_write))

    with (
        patch.object(BigtableOnlineStore, "_get_client"),
        patch.object(BigtableOnlineStore, "_get_table_name", return_value="tbl"),
        patch.object(BigtableOnlineStore, "_write_rows_to_bt", side_effect=_capture),
    ):
        store.online_write_batch(config, feature_view, data, None)


def test_defaults_match_legacy_constants():
    config = BigtableOnlineStoreConfig(instance="test-instance")
    assert config.mutations_per_write == bigtable_module.MUTATIONS_PER_OP == 50_000
    assert (
        config.write_concurrency
        == bigtable_module.BIGTABLE_CLIENT_CONNECTION_POOL_SIZE
        == 10
    )


@pytest.mark.parametrize("field_name", ["mutations_per_write", "write_concurrency"])
@pytest.mark.parametrize("bad_value", [0, -1])
def test_positive_int_validation(field_name, bad_value):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        BigtableOnlineStoreConfig(instance="test-instance", **{field_name: bad_value})


def test_online_write_batch_respects_mutations_per_write():
    # 1 feature -> columns_per_row = 2 -> rows_per_write = max(1, 10 // 2) = 5
    store = BigtableOnlineStore()
    config = _make_repo_config(mutations_per_write=10)
    feature_view = MockFeatureView(name="fv", features=[object()])

    captured: List[int] = []
    _run_write(store, config, feature_view, _rows(12), captured)

    assert captured == [5, 5, 2]


def test_rows_per_write_floors_to_one_for_wide_feature_view():
    # 10 features -> columns_per_row = 11 -> 2 // 11 = 0 -> floored to 1
    store = BigtableOnlineStore()
    config = _make_repo_config(mutations_per_write=2)
    feature_view = MockFeatureView(name="fv", features=[object() for _ in range(10)])

    captured: List[int] = []
    _run_write(store, config, feature_view, _rows(3), captured)

    assert captured == [1, 1, 1]


def test_write_concurrency_sets_thread_pool_size():
    store = BigtableOnlineStore()
    config = _make_repo_config(write_concurrency=3)
    feature_view = MockFeatureView(name="fv", features=[object()])

    with (
        patch.object(BigtableOnlineStore, "_get_client"),
        patch.object(BigtableOnlineStore, "_get_table_name", return_value="tbl"),
        patch.object(BigtableOnlineStore, "_write_rows_to_bt"),
        patch(
            "feast.infra.online_stores.bigtable.futures.ThreadPoolExecutor",
            wraps=futures.ThreadPoolExecutor,
        ) as mock_pool,
    ):
        store.online_write_batch(config, feature_view, _rows(4), None)

    mock_pool.assert_called_once_with(max_workers=3)
