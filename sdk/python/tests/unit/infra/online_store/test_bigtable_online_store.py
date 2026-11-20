from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("google.cloud.bigtable")

from concurrent import futures  # noqa: E402

from google.cloud.bigtable.column_family import (  # noqa: E402
    GarbageCollectionRule,
    MaxVersionsGCRule,
)

import feast.infra.online_stores.bigtable as bigtable_module  # noqa: E402
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig  # noqa: E402
from feast.infra.online_stores.bigtable import (  # noqa: E402
    BigtableOnlineStore,
    BigtableOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import (  # noqa: E402
    EntityKey as EntityKeyProto,
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


@pytest.mark.parametrize(
    "field_name",
    [
        "mutations_per_write",
        "write_concurrency",
        "keys_per_request",
        "max_concurrent_chunks",
        "read_timeout_ms",
        "max_inflight_reads",
    ],
)
@pytest.mark.parametrize("bad_value", [0, -1])
def test_positive_int_validation(field_name, bad_value):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        BigtableOnlineStoreConfig(instance="test-instance", **{field_name: bad_value})


@pytest.mark.parametrize("bad_value", [-1, 2.5])
def test_max_inflight_wait_ms_validation(bad_value: object) -> None:
    """Accept a max_inflight_wait_ms of 0 and reject negative and fractional values."""
    from pydantic import ValidationError

    assert (
        BigtableOnlineStoreConfig(
            instance="test-instance", max_inflight_wait_ms=0
        ).max_inflight_wait_ms
        == 0
    )
    with pytest.raises(ValidationError):
        BigtableOnlineStoreConfig(
            instance="test-instance",
            max_inflight_wait_ms=bad_value,  # type: ignore[arg-type]
        )


def test_keys_per_request_upper_bound() -> None:
    """Accept keys_per_request of 100 and reject 101."""
    from pydantic import ValidationError

    assert (
        BigtableOnlineStoreConfig(
            instance="test-instance", keys_per_request=100
        ).keys_per_request
        == 100
    )
    with pytest.raises(ValidationError):
        BigtableOnlineStoreConfig(instance="test-instance", keys_per_request=101)


@pytest.mark.parametrize(
    "field_name",
    [
        "keys_per_request",
        "max_concurrent_chunks",
        "read_timeout_ms",
        "max_inflight_reads",
        "max_inflight_wait_ms",
        "adaptive_chunking",
        "disable_metrics",
        "initial_tablets",
        "write_app_profile",
    ],
)
def test_go_reader_fields_accept_null(field_name: str) -> None:
    """Treat a null Go reader field as unset, so the Go feature server's default applies."""
    config = BigtableOnlineStoreConfig(
        instance="test-instance",
        **{field_name: None},  # type: ignore[arg-type]
    )
    assert getattr(config, field_name) is None

    # A bare `key:` in feature_store.yaml reaches the config as None in a dict.
    yaml_config = RepoConfig(
        registry="s3://test_registry/registry.db",
        project="test_bigtable",
        provider="local",
        online_store={"type": "bigtable", "instance": "i", field_name: None},
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )
    assert getattr(yaml_config.online_store, field_name) is None


def test_max_versions_alongside_go_reader_fields() -> None:
    """Keep max_versions when the Go reader fields are set beside it."""
    config = BigtableOnlineStoreConfig(
        instance="test-instance",
        max_versions=1,
        app_profile="serve",
        keys_per_request=25,
        max_concurrent_chunks=8,
        read_timeout_ms=150,
        max_inflight_reads=300,
    )
    assert config.max_versions == 1
    assert config.keys_per_request == 25


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


def test_bigtable_config_accepts_go_reader_fields() -> None:
    """Accept the Go reader fields from kwargs and from a YAML-style dict."""
    from pydantic import ValidationError

    config = _make_repo_config(
        app_profile="serve",
        keys_per_request=25,
        max_concurrent_chunks=8,
        read_timeout_ms=150,
        max_inflight_reads=300,
        max_inflight_wait_ms=15,
        adaptive_chunking=True,
        disable_metrics=True,
    )
    online_store = config.online_store
    assert online_store.instance == "test-instance"
    assert online_store.app_profile == "serve"
    assert online_store.keys_per_request == 25
    assert online_store.max_concurrent_chunks == 8
    assert online_store.read_timeout_ms == 150
    assert online_store.max_inflight_reads == 300
    assert online_store.max_inflight_wait_ms == 15
    assert online_store.adaptive_chunking is True
    assert online_store.disable_metrics is True

    with pytest.raises(ValidationError):
        _make_repo_config(keys_per_request=101)

    # feature_store.yaml reaches the config as a dict, so check that path as well.
    yaml_config = RepoConfig(
        registry="s3://test_registry/registry.db",
        project="test_bigtable",
        provider="local",
        online_store={
            "type": "bigtable",
            "instance": "i",
            "app_profile": "serve",
            "keys_per_request": 25,
            "max_concurrent_chunks": 8,
            "read_timeout_ms": 150,
            "max_inflight_reads": 300,
            "max_inflight_wait_ms": 0,
            "adaptive_chunking": True,
            "disable_metrics": False,
        },
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )
    assert isinstance(yaml_config.online_store, BigtableOnlineStoreConfig)
    assert yaml_config.online_store.keys_per_request == 25
    assert yaml_config.online_store.adaptive_chunking is True
    assert yaml_config.online_store.max_inflight_reads == 300
    assert yaml_config.online_store.max_inflight_wait_ms == 0
    assert yaml_config.online_store.disable_metrics is False

    defaults = BigtableOnlineStoreConfig(instance="test-instance")
    assert defaults.app_profile is None
    assert defaults.keys_per_request is None
    assert defaults.max_concurrent_chunks is None
    assert defaults.read_timeout_ms is None
    assert defaults.max_inflight_reads is None
    assert defaults.max_inflight_wait_ms is None
    assert defaults.adaptive_chunking is None
    assert defaults.disable_metrics is None


@pytest.mark.parametrize("field_name", ["adaptive_chunking", "disable_metrics"])
@pytest.mark.parametrize("bad_value", ["true", 1])
def test_go_reader_switches_reject_non_bool(field_name: str, bad_value: object) -> None:
    """Reject a quoted or numeric switch, as the Go feature server does."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        BigtableOnlineStoreConfig(
            instance="test-instance",
            **{field_name: bad_value},  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "field_name",
    [
        "keys_per_request",
        "max_concurrent_chunks",
        "max_inflight_reads",
        "max_inflight_wait_ms",
        "read_timeout_ms",
    ],
)
@pytest.mark.parametrize("bad_value", ["5", True, 2**31])
def test_go_reader_numbers_reject_what_go_rejects(
    field_name: str, bad_value: object
) -> None:
    """Reject a quoted, boolean or above-MaxInt32 number, as the Go feature server does."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        BigtableOnlineStoreConfig(
            instance="test-instance",
            **{field_name: bad_value},  # type: ignore[arg-type]
        )


def test_go_reader_numbers_accept_max_int32() -> None:
    """Accept MaxInt32, the largest value the Go feature server reads."""
    config = BigtableOnlineStoreConfig(
        instance="test-instance", read_timeout_ms=2**31 - 1, max_inflight_wait_ms=0
    )

    assert config.read_timeout_ms == 2**31 - 1
    assert config.max_inflight_wait_ms == 0


@dataclass
class _View:
    """A feature view stand-in with what update() and _get_table_name read."""

    name: str
    entities: List[str]


class _FakeTable:
    """A Bigtable table handle that records create and column family create."""

    def __init__(
        self, exists: bool, gc_rule: Optional[GarbageCollectionRule] = None
    ) -> None:
        self._exists = exists
        self.families: Dict[str, MagicMock] = (
            {} if gc_rule is None else {"features": MagicMock(gc_rule=gc_rule)}
        )
        self.create = MagicMock()
        self.created_rules: List[Optional[GarbageCollectionRule]] = []

    def exists(self) -> bool:
        """Report whether the table exists."""
        return self._exists

    def list_column_families(self) -> Dict[str, MagicMock]:
        """Return the table's column families by name."""
        return self.families

    def column_family(
        self, name: str, gc_rule: Optional[GarbageCollectionRule] = None
    ) -> MagicMock:
        """Return a family handle whose create() records gc_rule."""
        family = MagicMock()
        family.create.side_effect = lambda: self.created_rules.append(gc_rule)
        return family


def _run_update(config: RepoConfig, views: List[_View], table: _FakeTable) -> None:
    """Run BigtableOnlineStore.update() for views against a client that returns table."""
    client = MagicMock()
    client.instance.return_value.table.return_value = table
    with patch.object(BigtableOnlineStore, "_get_client", return_value=client):
        BigtableOnlineStore().update(
            config,
            tables_to_delete=[],
            tables_to_keep=views,  # type: ignore[arg-type]
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )


def _versions(n: int = 2) -> MaxVersionsGCRule:
    """Return the max-versions rule update() creates for max_versions n."""
    return MaxVersionsGCRule(n)


def test_initial_split_keys() -> None:
    """Split a table into evenly spaced two-hex-digit prefixes."""
    split = BigtableOnlineStore._initial_split_keys
    assert split(None) == []
    assert split(2) == [b"80"]
    assert split(4) == [b"40", b"80", b"c0"]
    assert split(16) == [f"{i:x}0".encode() for i in range(1, 16)]
    keys = split(256)
    assert len(keys) == 255
    assert keys == sorted(set(keys))
    assert keys[0] == b"01" and keys[-1] == b"ff"


@pytest.mark.parametrize("bad_value", [0, 1, 257])
def test_initial_tablets_bounds(bad_value: int) -> None:
    """Reject initial_tablets outside 2 to 256."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        BigtableOnlineStoreConfig(instance="test-instance", initial_tablets=bad_value)


def test_update_creates_unsplit_table_by_default() -> None:
    """Keep today's behavior: no split keys and a max-versions rule."""
    table = _FakeTable(exists=False)
    _run_update(_make_repo_config(), [_View("fv", ["user"])], table)
    table.create.assert_called_once_with()
    assert table.created_rules == [_versions()]


def test_update_presplits_new_tables() -> None:
    """Pass the split keys to create() for a new table."""
    table = _FakeTable(exists=False)
    _run_update(_make_repo_config(initial_tablets=4), [_View("fv", ["user"])], table)
    table.create.assert_called_once_with(initial_split_keys=[b"40", b"80", b"c0"])
    assert table.created_rules == [_versions()]


def test_update_leaves_existing_table_unsplit() -> None:
    """Never re-create or split a table that already exists."""
    table = _FakeTable(exists=True, gc_rule=_versions())
    _run_update(_make_repo_config(initial_tablets=4), [_View("fv", ["user"])], table)
    table.create.assert_not_called()
    assert table.created_rules == []


def test_initial_tablets_from_yaml() -> None:
    """Accept initial_tablets from a YAML-style dict; unset by default."""
    yaml_config = RepoConfig(
        registry="s3://test_registry/registry.db",
        project="test_bigtable",
        provider="local",
        online_store={"type": "bigtable", "instance": "i", "initial_tablets": 16},
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )
    assert yaml_config.online_store.initial_tablets == 16
    assert BigtableOnlineStoreConfig(instance="i").initial_tablets is None


@pytest.mark.parametrize(
    "online_store_kwargs, want_profile",
    [
        ({}, None),
        # app_profile alone never moves writes off the instance default.
        ({"app_profile": "serve"}, None),
        ({"app_profile": "serve", "write_app_profile": "batch"}, "batch"),
        ({"write_app_profile": "batch"}, "batch"),
    ],
)
def test_writes_use_write_app_profile(
    online_store_kwargs: Dict[str, str], want_profile: Optional[str]
) -> None:
    """Write through write_app_profile only, else the instance default."""
    config = _make_repo_config(**online_store_kwargs)
    client = MagicMock()
    with (
        patch.object(BigtableOnlineStore, "_get_client", return_value=client),
        patch.object(BigtableOnlineStore, "_get_table_name", return_value="tbl"),
        patch.object(BigtableOnlineStore, "_write_rows_to_bt"),
    ):
        BigtableOnlineStore().online_write_batch(
            config,
            MockFeatureView(name="fv", features=[object()]),  # type: ignore[arg-type]
            _rows(1),
            None,
        )
    client.instance.return_value.table.assert_called_once_with(
        "tbl", app_profile_id=want_profile
    )


@pytest.mark.parametrize(
    "online_store_kwargs, want_profile",
    [
        ({}, None),
        ({"app_profile": "serve", "write_app_profile": "batch"}, "serve"),
    ],
)
def test_reads_use_app_profile(
    online_store_kwargs: Dict[str, str], want_profile: Optional[str]
) -> None:
    """Read through app_profile, never write_app_profile."""
    config = _make_repo_config(**online_store_kwargs)
    client = MagicMock()
    client.instance.return_value.table.return_value.read_rows.return_value = []
    with (
        patch.object(BigtableOnlineStore, "_get_client", return_value=client),
        patch.object(BigtableOnlineStore, "_get_table_name", return_value="tbl"),
        patch.object(BigtableOnlineStore, "_compute_row_key", return_value=b"k#fv"),
    ):
        got = BigtableOnlineStore().online_read(
            config,
            MockFeatureView(name="fv"),  # type: ignore[arg-type]
            [EntityKeyProto()],
            ["f"],
        )
    assert got == [(None, None)]
    client.instance.return_value.table.assert_called_once_with(
        "tbl", app_profile_id=want_profile
    )


def test_write_settings_from_yaml() -> None:
    """Accept write_app_profile from a YAML-style dict; unset by default."""
    yaml_config = RepoConfig(
        registry="s3://test_registry/registry.db",
        project="test_bigtable",
        provider="local",
        online_store={
            "type": "bigtable",
            "instance": "i",
            "write_app_profile": "batch",
        },
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )
    assert yaml_config.online_store.write_app_profile == "batch"
    defaults = BigtableOnlineStoreConfig(instance="i")
    assert defaults.write_app_profile is None
