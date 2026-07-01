from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from feast.infra.offline_stores.contrib.spark_offline_store.databricks_uc import (
    DatabricksUCOfflineStore,
    DatabricksUCOfflineStoreConfig,
    get_databricks_session,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.offline_stores.iceberg.catalog_config import IcebergCatalogConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig


def _catalog_config(**overrides) -> IcebergCatalogConfig:
    defaults = {
        "type": "rest",
        "endpoint": "https://adb-12345.azuredatabricks.net/api/2.1/unity-catalog/iceberg",
        "warehouse": "main",
        "namespace": "default",
    }
    return IcebergCatalogConfig(**{**defaults, **overrides})


def test_config_parsing():
    config_dict = {
        "type": "databricks_uc",
        "catalog": {
            "type": "rest",
            "endpoint": "https://adb-12345.azuredatabricks.net/api/2.1/unity-catalog/iceberg",
            "warehouse": "main",
            "namespace": "default",
            "token_env_var": "DATABRICKS_TOKEN",
        },
        "spark_conf": {"spark.sql.shuffle.partitions": "10"},
    }
    config = DatabricksUCOfflineStoreConfig(**config_dict)
    assert config.type == "databricks_uc"
    assert config.catalog.warehouse == "main"
    assert config.catalog.namespace == "default"
    assert config.catalog.token_env_var == "DATABRICKS_TOKEN"
    assert config.spark_conf == {"spark.sql.shuffle.partitions": "10"}


def test_config_with_uc_registration():
    config_dict = {
        "type": "databricks_uc",
        "catalog": {
            "type": "rest",
            "endpoint": "https://adb-12345.azuredatabricks.net/api/2.1/unity-catalog/iceberg",
            "warehouse": "prod_ml",
            "namespace": "features",
        },
        "uc_registration": {
            "enabled": True,
            "catalog": "override_cat",
            "uc_schema": "override_sch",
        },
    }
    config = DatabricksUCOfflineStoreConfig(**config_dict)
    assert config.uc_registration is not None
    assert config.uc_registration.enabled is True
    assert config.uc_registration.catalog == "override_cat"
    assert config.uc_registration.uc_schema == "override_sch"


def test_config_forbidden_extra():
    with pytest.raises(ValidationError):
        DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            catalog=_catalog_config(),
            invalid_key="some_val",
        )


@patch("pyspark.sql.SparkSession.getActiveSession")
def test_get_databricks_session_active(mock_get_active):
    mock_session = MagicMock()
    mock_get_active.return_value = mock_session

    config = DatabricksUCOfflineStoreConfig(
        type="databricks_uc",
        catalog=_catalog_config(warehouse="my_catalog", namespace="my_schema"),
    )

    session = get_databricks_session(config)

    assert session == mock_session
    mock_session.conf.set.assert_called_once_with(
        "spark.sql.parser.quotedRegexColumnNames", "true"
    )
    mock_session.sql.assert_any_call("USE CATALOG `my_catalog`")
    mock_session.sql.assert_any_call("USE SCHEMA `my_schema`")


@patch("pyspark.sql.SparkSession.getActiveSession")
@patch("pyspark.sql.SparkSession.builder")
def test_get_databricks_session_new_remote(mock_builder, mock_get_active):
    mock_get_active.return_value = None
    mock_session = MagicMock()
    mock_builder.remote.return_value.config.return_value.getOrCreate.return_value = (
        mock_session
    )

    config = DatabricksUCOfflineStoreConfig(
        type="databricks_uc",
        catalog=_catalog_config(
            endpoint="https://adb-12345.azuredatabricks.net/api/2.1/unity-catalog/iceberg",
            warehouse="main",
            namespace="default",
        ),
        spark_conf={"spark.some.option": "value"},
    )

    session = get_databricks_session(config)

    assert session == mock_session
    mock_builder.remote.assert_called_once_with(
        "sc://adb-12345.azuredatabricks.net:443/"
    )


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.databricks_uc.get_databricks_session"
)
@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark.SparkOfflineStore.get_historical_features"
)
def test_get_historical_features_delegation(mock_parent_features, mock_get_session):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    repo_config = RepoConfig(
        registry="file:///tmp/registry.db",
        project="test",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            catalog=_catalog_config(
                endpoint="https://adb-123.databricks.com/api/2.1/unity-catalog/iceberg",
            ),
        ),
    )

    feature_views = []
    feature_refs = ["fv:f1"]
    entity_df = MagicMock()
    registry = MagicMock()

    DatabricksUCOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=feature_views,
        feature_refs=feature_refs,
        entity_df=entity_df,
        registry=registry,
        project="test",
    )

    mock_get_session.assert_called_once_with(repo_config.offline_store)
    mock_parent_features.assert_called_once_with(
        config=repo_config,
        feature_views=feature_views,
        feature_refs=feature_refs,
        entity_df=entity_df,
        registry=registry,
        project="test",
        full_feature_names=False,
    )


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.databricks_uc.get_databricks_session"
)
@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark.SparkOfflineStore.pull_latest_from_table_or_query"
)
def test_pull_latest_from_table_or_query_delegation(
    mock_parent_pull_latest, mock_get_session
):
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    repo_config = RepoConfig(
        registry="file:///tmp/registry.db",
        project="test",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=DatabricksUCOfflineStoreConfig(
            type="databricks_uc",
            catalog=_catalog_config(
                endpoint="https://adb-123.databricks.com/api/2.1/unity-catalog/iceberg",
            ),
        ),
    )

    data_source = SparkSource(
        name="test_source",
        path="catalog.schema.table",
        file_format="parquet",
        timestamp_field="ts",
    )

    start_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2023, 1, 2, tzinfo=timezone.utc)

    DatabricksUCOfflineStore.pull_latest_from_table_or_query(
        config=repo_config,
        data_source=data_source,
        join_key_columns=["id"],
        feature_name_columns=["val"],
        timestamp_field="ts",
        created_timestamp_column=None,
        start_date=start_date,
        end_date=end_date,
    )

    mock_get_session.assert_called_once_with(repo_config.offline_store)
    mock_parent_pull_latest.assert_called_once_with(
        config=repo_config,
        data_source=data_source,
        join_key_columns=["id"],
        feature_name_columns=["val"],
        timestamp_field="ts",
        created_timestamp_column=None,
        start_date=start_date,
        end_date=end_date,
    )
