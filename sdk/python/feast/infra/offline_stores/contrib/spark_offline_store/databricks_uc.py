import logging
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow
import pyspark
from pydantic import StrictStr
from pyspark import SparkConf
from pyspark.sql import SparkSession

from feast import FeatureView
from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
    SparkOfflineStoreConfig,
)
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import RepoConfig

logger = logging.getLogger(__name__)


class DatabricksUCOfflineStoreConfig(SparkOfflineStoreConfig):
    type: StrictStr = "databricks_uc"
    """Offline store type selector"""

    workspace_host: Optional[StrictStr] = None
    """Databricks workspace host (e.g. adb-xxxx.azuredatabricks.net)"""

    token: Optional[StrictStr] = None
    """Databricks Personal Access Token (PAT)"""

    cluster_id: Optional[StrictStr] = None
    """Databricks Cluster ID to connect to for Databricks Connect"""

    default_catalog: Optional[StrictStr] = None
    """Default catalog name to use in Unity Catalog"""

    default_schema: Optional[StrictStr] = None
    """Default schema name to use in Unity Catalog"""


def get_databricks_session(
    store_config: DatabricksUCOfflineStoreConfig,
) -> SparkSession:
    # Check if there is already an active session
    spark_session = SparkSession.getActiveSession()
    if not spark_session:
        workspace_host = store_config.workspace_host
        token = store_config.token
        cluster_id = store_config.cluster_id

        # Clean host URL if it starts with https://
        if workspace_host:
            if workspace_host.startswith("https://"):
                workspace_host = workspace_host[8:]
            elif workspace_host.startswith("http://"):
                workspace_host = workspace_host[7:]

        if workspace_host and cluster_id:
            # Databricks Connect V2 initialization (Spark Connect URI format)
            conn_str = f"sc://{workspace_host}:443/"
            params = []
            if token:
                params.append(f"token={token}")
            params.append(f"x-databricks-cluster-id={cluster_id}")
            if params:
                conn_str = f"{conn_str};{';'.join(params)}"

            try:
                from databricks.connect import DatabricksSession

                builder = DatabricksSession.builder.remote(conn_str)
            except ImportError:
                # Fallback to standard PySpark remote connect if databricks-connect not installed
                builder = SparkSession.builder.remote(conn_str)
        else:
            try:
                from databricks.connect import DatabricksSession

                builder = DatabricksSession.builder
            except ImportError:
                builder = SparkSession.builder

        spark_conf = store_config.spark_conf
        if spark_conf:
            builder = builder.config(
                conf=SparkConf().setAll([(k, v) for k, v in spark_conf.items()])
            )

        spark_session = builder.getOrCreate()

    # Apply configuration defaults
    spark_session.conf.set("spark.sql.parser.quotedRegexColumnNames", "true")

    if store_config.default_catalog:
        spark_session.sql(f"USE CATALOG `{store_config.default_catalog}`")
    if store_config.default_schema:
        spark_session.sql(f"USE SCHEMA `{store_config.default_schema}`")

    return spark_session


class DatabricksUCOfflineStore(SparkOfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        # Initialize/Retrieve the Databricks Spark Session so it's registered as active
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.pull_latest_from_table_or_query(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str, pyspark.sql.DataFrame]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        **kwargs,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            **kwargs,
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.pull_all_from_table_or_query(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.offline_write_batch(
            config=config,
            feature_view=feature_view,
            table=table,
            progress=progress,
        )

    @staticmethod
    def compute_monitoring_metrics(
        config: RepoConfig,
        data_source: DataSource,
        feature_columns: List[Tuple[str, str]],
        timestamp_field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        histogram_bins: int = 20,
        top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.compute_monitoring_metrics(
            config=config,
            data_source=data_source,
            feature_columns=feature_columns,
            timestamp_field=timestamp_field,
            start_date=start_date,
            end_date=end_date,
            histogram_bins=histogram_bins,
            top_n=top_n,
        )

    @staticmethod
    def get_monitoring_max_timestamp(
        config: RepoConfig,
        data_source: DataSource,
        timestamp_field: str,
    ) -> Optional[datetime]:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.get_monitoring_max_timestamp(
            config=config,
            data_source=data_source,
            timestamp_field=timestamp_field,
        )

    @staticmethod
    def ensure_monitoring_tables(config: RepoConfig) -> None:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.ensure_monitoring_tables(config=config)

    @staticmethod
    def save_monitoring_metrics(
        config: RepoConfig,
        metric_type: str,
        metrics: List[Dict[str, Any]],
    ) -> None:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.save_monitoring_metrics(
            config=config,
            metric_type=metric_type,
            metrics=metrics,
        )

    @staticmethod
    def query_monitoring_metrics(
        config: RepoConfig,
        project: str,
        metric_type: str,
        filters: Optional[Dict[str, Any]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.query_monitoring_metrics(
            config=config,
            project=project,
            metric_type=metric_type,
            filters=filters,
            start_date=start_date,
            end_date=end_date,
        )

    @staticmethod
    def clear_monitoring_baseline(
        config: RepoConfig,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> None:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)

        return SparkOfflineStore.clear_monitoring_baseline(
            config=config,
            project=project,
            feature_view_name=feature_view_name,
            feature_name=feature_name,
            data_source_type=data_source_type,
        )

    @staticmethod
    def validate_data_source(
        config: RepoConfig,
        data_source: DataSource,
    ):
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)
        data_source.validate(config=config)

    @staticmethod
    def get_table_column_names_and_types_from_data_source(
        config: RepoConfig,
        data_source: DataSource,
    ) -> Iterable[Tuple[str, str]]:
        assert isinstance(config.offline_store, DatabricksUCOfflineStoreConfig)
        get_databricks_session(config.offline_store)
        return data_source.get_table_column_names_and_types(config=config)
