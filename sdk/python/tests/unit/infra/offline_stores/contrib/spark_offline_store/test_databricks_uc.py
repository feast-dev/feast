from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from feast.infra.offline_stores.contrib.spark_offline_store.databricks_uc import (
    DatabricksUCOfflineStore,
    DatabricksUCOfflineStoreConfig,
    get_databricks_session,
)
from feast.infra.offline_stores.contrib.spark_offline_store.iceberg_source import (
    IcebergRestCatalogSource,
    UnityCatalogSource,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig

# ==============================================================================
# DatabricksUCOfflineStore (deprecated) — existing tests
# ==============================================================================


def test_config_parsing():
    config_dict = {
        "type": "databricks_uc",
        "workspace_host": "adb-12345.azuredatabricks.net",
        "token": "dapi123456",
        "cluster_id": "0123-4567-abcde",
        "default_catalog": "main",
        "default_schema": "default",
        "spark_conf": {"spark.sql.shuffle.partitions": "10"},
    }
    config = DatabricksUCOfflineStoreConfig(**config_dict)
    assert config.type == "databricks_uc"
    assert config.workspace_host == "adb-12345.azuredatabricks.net"
    assert config.token == "dapi123456"
    assert config.cluster_id == "0123-4567-abcde"
    assert config.default_catalog == "main"
    assert config.default_schema == "default"
    assert config.spark_conf == {"spark.sql.shuffle.partitions": "10"}


def test_config_forbidden_extra():
    with pytest.raises(ValidationError):
        DatabricksUCOfflineStoreConfig(type="databricks_uc", invalid_key="some_val")


@patch("pyspark.sql.SparkSession.getActiveSession")
def test_get_databricks_session_active(mock_get_active):
    mock_session = MagicMock()
    mock_get_active.return_value = mock_session

    config = DatabricksUCOfflineStoreConfig(
        type="databricks_uc",
        default_catalog="my_catalog",
        default_schema="my_schema",
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
        workspace_host="https://adb-12345.azuredatabricks.net",
        token="dapi123",
        cluster_id="0123-4567-abcde",
        spark_conf={"spark.some.option": "value"},
    )

    session = get_databricks_session(config)

    assert session == mock_session
    mock_builder.remote.assert_called_once_with(
        "sc://adb-12345.azuredatabricks.net:443/;token=dapi123;x-databricks-cluster-id=0123-4567-abcde"
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
            workspace_host="adb-123.databricks.com",
            cluster_id="123",
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
            workspace_host="adb-123.databricks.com",
            cluster_id="123",
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


# ==============================================================================
# IcebergRestCatalogSource tests
# ==============================================================================


def test_iceberg_rest_catalog_source_create():
    source = IcebergRestCatalogSource(
        name="test_source",
        endpoint="https://iceberg.example.com/catalog",
        warehouse="my_catalog",
        namespace="my_schema",
        table="my_table",
        timestamp_field="event_ts",
    )
    assert source.name == "test_source"
    assert source.endpoint == "https://iceberg.example.com/catalog"
    assert source.warehouse == "my_catalog"
    assert source.namespace == "my_schema"
    assert source._table == "my_table"
    assert source.full_table_name == "`my_catalog`.`my_schema`.`my_table`"
    assert source.token_env_var == "ICEBERG_REST_TOKEN"
    assert source.credential_vending is True
    assert source.timestamp_field == "event_ts"


def test_iceberg_rest_catalog_source_get_table_query_string():
    source = IcebergRestCatalogSource(
        name="tsrc",
        endpoint="https://example.com/catalog",
        warehouse="catalog",
        namespace="schema",
        table="table",
    )
    assert source.get_table_query_string() == "`catalog`.`schema`.`table`"


def test_iceberg_rest_catalog_source_proto_roundtrip():
    source = IcebergRestCatalogSource(
        name="test_iceberg",
        endpoint="https://iceberg.example.com/catalog",
        warehouse="my_catalog",
        namespace="my_schema",
        table="my_table",
        timestamp_field="event_ts",
        token_env_var="MY_TOKEN_VAR",
        credential_vending=False,
        description="An Iceberg REST source",
        tags={"env": "test"},
        owner="me@example.com",
    )

    proto = source.to_proto()
    restored = IcebergRestCatalogSource.from_proto(proto)

    assert restored == source
    assert restored.name == source.name
    assert restored.endpoint == source.endpoint
    assert restored.warehouse == source.warehouse
    assert restored.namespace == source.namespace
    assert restored._table == source._table
    assert restored.timestamp_field == source.timestamp_field
    assert restored.token_env_var == source.token_env_var
    assert restored.credential_vending == source.credential_vending
    assert restored.description == source.description
    assert restored.tags == source.tags
    assert restored.owner == source.owner


def test_iceberg_rest_catalog_source_proto_roundtrip_without_timestamp():
    source = IcebergRestCatalogSource(
        name="no_ts",
        endpoint="https://example.com/catalog",
        warehouse="c",
        namespace="s",
        table="t",
    )

    proto = source.to_proto()
    restored = IcebergRestCatalogSource.from_proto(proto)

    assert restored.name == "no_ts"
    assert restored.timestamp_field == ""


# ==============================================================================
# UnityCatalogSource tests
# ==============================================================================


def test_unity_catalog_source_create():
    source = UnityCatalogSource(
        name="uc_source",
        endpoint="https://adb-12345.azuredatabricks.net",
        warehouse="main",
        namespace="default",
        table="my_features",
        timestamp_field="event_ts",
        cluster_id="0123-4567-abcde",
    )
    assert source.name == "uc_source"
    assert source.endpoint == "https://adb-12345.azuredatabricks.net"
    assert source.warehouse == "main"
    assert source.namespace == "default"
    assert source._table == "my_features"
    assert source.cluster_id == "0123-4567-abcde"
    assert source.token_env_var == "DATABRICKS_TOKEN"
    assert source.register_as_feature_table is True
    assert source.sync_lineage is True
    assert source.full_table_name == "`main`.`default`.`my_features`"


def test_unity_catalog_source_proto_roundtrip():
    source = UnityCatalogSource(
        name="test_uc",
        endpoint="https://adb-12345.azuredatabricks.net",
        warehouse="main",
        namespace="default",
        table="features",
        timestamp_field="event_ts",
        token_env_var="DATABRICKS_TOKEN",
        credential_vending=True,
        cluster_id="0123-4567-abcde",
        register_as_feature_table=True,
        sync_lineage=False,
        description="UC test source",
        tags={"team": "ml"},
        owner="ml@example.com",
    )

    proto = source.to_proto()
    restored = UnityCatalogSource.from_proto(proto)

    assert restored == source
    assert restored.name == source.name
    assert restored.endpoint == source.endpoint
    assert restored.warehouse == source.warehouse
    assert restored.namespace == source.namespace
    assert restored._table == source._table
    assert restored.cluster_id == source.cluster_id
    assert restored.register_as_feature_table == source.register_as_feature_table
    assert restored.sync_lineage == source.sync_lineage
    assert restored.description == source.description
    assert restored.tags == source.tags


def test_unity_catalog_source_inherits_iceberg_methods():
    uc = UnityCatalogSource(
        name="uc",
        endpoint="https://adb-xxx.databricks.net",
        warehouse="main",
        namespace="default",
        table="features",
    )
    assert isinstance(uc, IcebergRestCatalogSource)
    assert uc.get_table_query_string() == "`main`.`default`.`features`"
    assert uc.path is None
    assert uc.file_format is None
    assert uc.query is None
    assert uc.date_partition_column_format == "%Y-%m-%d"


# ==============================================================================
# SparkOfflineStore source type acceptance tests
# ==============================================================================


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark._resolve_spark_session_for_source"
)
@patch("pyspark.sql.SparkSession")
def test_spark_offline_store_accepts_iceberg_source(
    mock_spark_session, mock_resolve_session
):
    from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
        SparkOfflineStore,
    )

    mock_session = MagicMock()
    mock_resolve_session.return_value = mock_session

    source = IcebergRestCatalogSource(
        name="ice_src",
        endpoint="https://example.com/catalog",
        warehouse="c",
        namespace="s",
        table="t",
        timestamp_field="ts",
    )

    entity_df = MagicMock()
    registry = MagicMock()

    fv = MagicMock()
    fv.batch_source = source
    fv.ttl = MagicMock()
    fv.projection = MagicMock()
    fv.projection.name_to_use.return_value = "fv1"

    repo_config = RepoConfig(
        registry="file:///tmp/registry.db",
        project="test",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=None,
    )

    with pytest.raises(Exception):
        SparkOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["fv1:f1"],
            entity_df=entity_df,
            registry=registry,
            project="test",
        )


def test_spark_offline_store_rejects_unsupported_source():
    from feast.data_source import DataSource
    from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
        SparkOfflineStore,
    )

    class UnsupportedSource(DataSource):
        def source_type(self):
            pass

        @staticmethod
        def source_datatype_to_feast_value_type():
            pass

        def get_table_query_string(self):
            return "unsupported"

        @staticmethod
        def from_proto(data_source):
            pass

        def _to_proto_impl(self):
            pass

    repo_config = RepoConfig(
        registry="file:///tmp/registry.db",
        project="test",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=None,
    )

    with pytest.raises(AssertionError, match="SparkOfflineStore requires"):
        SparkOfflineStore.pull_latest_from_table_or_query(
            config=repo_config,
            data_source=UnsupportedSource(name="bad"),
            join_key_columns=[],
            feature_name_columns=[],
            timestamp_field="ts",
            created_timestamp_column=None,
            start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2023, 1, 2, tzinfo=timezone.utc),
        )


# ==============================================================================
# get_databricks_connect_session (utils) tests
# ==============================================================================


@patch("pyspark.sql.SparkSession.getActiveSession")
def test_get_databricks_connect_session_active(mock_get_active):
    from feast.infra.offline_stores.contrib.spark_offline_store.utils import (
        get_databricks_connect_session,
    )

    mock_session = MagicMock()
    mock_session.conf.get.return_value = "sc://adb-123.databricks.net:443/"
    mock_get_active.return_value = mock_session

    session = get_databricks_connect_session(
        host="adb-123.databricks.net",
        token="dapi123",
        cluster_id="123-abc",
        catalog="my_catalog",
        schema="my_schema",
    )

    assert session == mock_session
    mock_session.conf.set.assert_called_once_with(
        "spark.sql.parser.quotedRegexColumnNames", "true"
    )
    mock_session.sql.assert_any_call("USE CATALOG `my_catalog`")
    mock_session.sql.assert_any_call("USE SCHEMA `my_schema`")


@patch("pyspark.sql.SparkSession.getActiveSession")
@patch("pyspark.sql.SparkSession.builder")
def test_get_databricks_connect_session_new_remote(mock_builder, mock_get_active):
    from feast.infra.offline_stores.contrib.spark_offline_store.utils import (
        get_databricks_connect_session,
    )

    mock_get_active.return_value = None
    mock_session = MagicMock()
    mock_builder.remote.return_value.config.return_value.getOrCreate.return_value = (
        mock_session
    )

    session = get_databricks_connect_session(
        host="https://adb-12345.azuredatabricks.net",
        token="dapi123",
        cluster_id="0123-4567-abcde",
        spark_conf={"spark.some.option": "value"},
    )

    assert session == mock_session
    mock_builder.remote.assert_called_once_with(
        "sc://adb-12345.azuredatabricks.net:443/;token=dapi123;x-databricks-cluster-id=0123-4567-abcde"
    )


@patch("pyspark.sql.SparkSession.getActiveSession")
def test_get_databricks_connect_session_none_passed(mock_get_active):
    from feast.infra.offline_stores.contrib.spark_offline_store.utils import (
        get_databricks_connect_session,
    )

    mock_session = MagicMock()
    mock_get_active.return_value = mock_session

    session = get_databricks_connect_session()

    assert session == mock_session
    mock_session.conf.set.assert_called_once_with(
        "spark.sql.parser.quotedRegexColumnNames", "true"
    )
    mock_session.sql.assert_not_called()
