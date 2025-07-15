from unittest.mock import MagicMock, patch

import pytest

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.hybrid_offline_store import (
    HybridOfflineStore,
    HybridOfflineStoreConfig,
)
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.repo_config import RepoConfig

driver = Entity(name="driver_id", description="driver id")


@pytest.fixture
def mock_repo_config():
    return RepoConfig(
        registry="/tmp/registry.db",
        project="test_project",
        provider="local",
        offline_store=HybridOfflineStoreConfig(
            offline_stores=[
                HybridOfflineStoreConfig.OfflineStoresWithConfig(
                    type="file",
                    conf={},
                ),
                HybridOfflineStoreConfig.OfflineStoresWithConfig(
                    type="snowflake.offline",
                    conf={"database": "db", "schema": "public"},
                ),
            ]
        ),
        online_store=None,
    )


@patch("feast.infra.offline_stores.hybrid_offline_store.get_offline_store_from_config")
def test_file_source_routing(mock_get_offline_store_from_config, mock_repo_config):
    HybridOfflineStore._instance = None

    mock_file_store = MagicMock()
    mock_snowflake_store = MagicMock()

    def side_effect(conf):
        if conf.__class__.__name__ == "DaskOfflineStoreConfig":
            return mock_file_store
        elif conf.__class__.__name__ == "SnowflakeOfflineStoreConfig":
            return mock_snowflake_store
        raise ValueError(f"Unexpected config class: {conf.__class__.__name__}")

    mock_get_offline_store_from_config.side_effect = side_effect

    hybrid_store = HybridOfflineStore()

    feature_view_1 = FeatureView(
        name="my_feature_1",
        entities=[driver],
        schema=[],
        source=FileSource(path="data.parquet"),
    )

    store1 = hybrid_store._get_offline_store_for_feature_view(
        feature_view_1, mock_repo_config
    )
    assert store1 is mock_file_store

    feature_view_2 = FeatureView(
        name="my_feature_2",
        entities=[driver],
        schema=[],
        source=SnowflakeSource(database="db", schema="public", table="table"),
    )

    store2 = hybrid_store._get_offline_store_for_feature_view(
        feature_view_2, mock_repo_config
    )
    assert store2 is mock_snowflake_store
