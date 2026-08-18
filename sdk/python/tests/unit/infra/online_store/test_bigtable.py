import asyncio
from dataclasses import dataclass, field
from typing import List
from unittest.mock import AsyncMock, MagicMock

import pytest

from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.bigtable import (
    BigtableOnlineStore,
    BigtableOnlineStoreConfig,
)
from feast.repo_config import RepoConfig

REGISTRY = "file://test_registry/registry.db"
PROJECT = "test_project"
PROVIDER = "gcp"
INSTANCE = "feature-store-bt-dev"
GCP_PROJECT_ID = "feature-store-dev-bf34"


@dataclass
class MockFeatureView:
    name: str
    entities: List[str] = field(default_factory=list)
    features: List[object] = field(default_factory=list)


class _AsyncTableContextManager:
    """Minimal async context manager standing in for BigtableDataClientAsync.get_table()."""

    def __init__(self, table):
        self._table = table

    async def __aenter__(self):
        return self._table

    async def __aexit__(self, *exc_info):
        return False


class _EmptyAsyncIterator:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


def _repo_config(app_profile_id=None) -> RepoConfig:
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=BigtableOnlineStoreConfig(
            instance=INSTANCE,
            project_id=GCP_PROJECT_ID,
            app_profile_id=app_profile_id,
        ),
        offline_store=FileOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )


@pytest.fixture
def feature_view():
    return MockFeatureView(name="test_fv")


@pytest.fixture
def store():
    return BigtableOnlineStore()


def test_bigtable_online_store_config_defaults_app_profile_to_none():
    config = BigtableOnlineStoreConfig(instance=INSTANCE)
    assert config.app_profile_id is None


def test_bigtable_online_store_config_accepts_configured_app_profile():
    config = BigtableOnlineStoreConfig(instance=INSTANCE, app_profile_id="test-app-profile")
    assert config.app_profile_id == "test-app-profile"


@pytest.mark.parametrize("app_profile_id", [None, "test-app-profile"])
def test_online_read_passes_app_profile_to_instance_table(
    store, feature_view, app_profile_id
):
    repo_config = _repo_config(app_profile_id=app_profile_id)

    mock_bt_table = MagicMock()
    mock_bt_table.read_rows.return_value = []
    mock_bt_instance = MagicMock()
    mock_bt_instance.table.return_value = mock_bt_table
    mock_client = MagicMock()
    mock_client.instance.return_value = mock_bt_instance
    store._get_client = MagicMock(return_value=mock_client)

    store.online_read(config=repo_config, table=feature_view, entity_keys=[])

    _, kwargs = mock_bt_instance.table.call_args
    assert kwargs["app_profile_id"] == app_profile_id


@pytest.mark.parametrize("app_profile_id", [None, "test-app-profile"])
def test_online_write_batch_passes_app_profile_to_instance_table(
    store, feature_view, app_profile_id
):
    repo_config = _repo_config(app_profile_id=app_profile_id)

    mock_bt_table = MagicMock()
    mock_bt_instance = MagicMock()
    mock_bt_instance.table.return_value = mock_bt_table
    mock_client = MagicMock()
    mock_client.instance.return_value = mock_bt_instance
    store._get_client = MagicMock(return_value=mock_client)

    store.online_write_batch(
        config=repo_config, table=feature_view, data=[], progress=None
    )

    _, kwargs = mock_bt_instance.table.call_args
    assert kwargs["app_profile_id"] == app_profile_id


@pytest.mark.parametrize("app_profile_id", [None, "test-app-profile"])
def test_online_read_async_passes_app_profile_to_get_table(
    store, feature_view, app_profile_id
):
    repo_config = _repo_config(app_profile_id=app_profile_id)

    mock_bt_table = MagicMock()
    mock_bt_table.read_rows = AsyncMock(return_value=[])
    mock_client = MagicMock()
    mock_client.get_table = MagicMock(
        return_value=_AsyncTableContextManager(mock_bt_table)
    )
    store._get_client_async = MagicMock(return_value=mock_client)

    asyncio.run(
        store.online_read_async(config=repo_config, table=feature_view, entity_keys=[])
    )

    _, kwargs = mock_client.get_table.call_args
    assert kwargs["app_profile_id"] == app_profile_id


@pytest.mark.parametrize("app_profile_id", [None, "test-app-profile"])
def test_online_read_async_v2_sets_request_app_profile_id(
    store, feature_view, app_profile_id
):
    repo_config = _repo_config(app_profile_id=app_profile_id)

    mock_client = MagicMock()
    mock_client.read_rows = AsyncMock(return_value=_EmptyAsyncIterator())
    store._get_client_async_v2 = MagicMock(return_value=mock_client)

    asyncio.run(
        store.online_read_async_v2(
            config=repo_config, table=feature_view, entity_keys=[]
        )
    )

    _, kwargs = mock_client.read_rows.call_args
    assert kwargs["request"].app_profile_id == (app_profile_id or "")
