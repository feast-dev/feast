import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Union

import ibis
import pandas as pd
import pyarrow
from ibis.expr.types import Table
from pydantic import StrictStr

from feast.data_format import DeltaFormat, ParquetFormat
from feast.data_source import DataSource
from feast.errors import SavedDatasetLocationAlreadyExists
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.ibis import (
    get_historical_features_ibis,
    offline_write_batch_ibis,
    pull_all_from_table_or_query_ibis,
    pull_latest_from_table_or_query_ibis,
    write_logged_features_ibis,
)
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

logger = logging.getLogger(__name__)

# Track whether S3 has been configured for the current DuckDB connection
_s3_configured = False


def _configure_duckdb_for_s3(config: "DuckDBOfflineStoreConfig") -> None:
    """
    Configure the DuckDB connection for S3 access.

    This function configures DuckDB's HTTPFS extension with S3 settings.
    It's designed to be called once before any S3 operations.

    Args:
        config: DuckDB offline store configuration containing S3 settings.
    """
    global _s3_configured

    # Check if any S3 settings are configured
    has_s3_settings = any(
        [
            config.s3_url_style,
            config.s3_endpoint,
            config.s3_access_key_id,
            config.s3_secret_access_key,
            config.s3_region,
            config.s3_use_ssl is not None,
        ]
    )

    if not has_s3_settings:
        return

    if _s3_configured:
        return

    try:
        # Get the default DuckDB connection from ibis
        con = ibis.get_backend()

        # Install and load httpfs extension for S3 support
        con.raw_sql("INSTALL httpfs;")
        con.raw_sql("LOAD httpfs;")

        # Configure S3 settings
        if config.s3_url_style:
            con.raw_sql(f"SET s3_url_style='{config.s3_url_style}';")
            logger.debug(f"DuckDB S3 url_style set to '{config.s3_url_style}'")

        if config.s3_endpoint:
            con.raw_sql(f"SET s3_endpoint='{config.s3_endpoint}';")
            logger.debug(f"DuckDB S3 endpoint set to '{config.s3_endpoint}'")

        if config.s3_access_key_id:
            con.raw_sql(f"SET s3_access_key_id='{config.s3_access_key_id}';")
            logger.debug("DuckDB S3 access_key_id configured")

        if config.s3_secret_access_key:
            con.raw_sql(f"SET s3_secret_access_key='{config.s3_secret_access_key}';")
            logger.debug("DuckDB S3 secret_access_key configured")

        if config.s3_region:
            con.raw_sql(f"SET s3_region='{config.s3_region}';")
            logger.debug(f"DuckDB S3 region set to '{config.s3_region}'")

        if config.s3_use_ssl is not None:
            ssl_value = "true" if config.s3_use_ssl else "false"
            con.raw_sql(f"SET s3_use_ssl={ssl_value};")
            logger.debug(f"DuckDB S3 use_ssl set to {ssl_value}")

        _s3_configured = True
        logger.info("DuckDB S3 configuration completed successfully")

    except Exception as e:
        logger.warning(f"Failed to configure DuckDB for S3: {e}")
        # Don't raise - let the operation continue and potentially fail with a more specific error


def _is_s3_path(path: str) -> bool:
    """Check if the given path is an S3 path."""
    return path.startswith("s3://") or path.startswith("s3a://")


def _read_data_source(data_source: DataSource, repo_path: str) -> Table:
    """
    Read data from a FileSource into an ibis Table.

    Note: S3 configuration must be set up before calling this function
    by calling _configure_duckdb_for_s3() from the DuckDBOfflineStore methods.
    """
    assert isinstance(data_source, FileSource)

    if isinstance(data_source.file_format, ParquetFormat):
        return ibis.read_parquet(data_source.path)
    elif isinstance(data_source.file_format, DeltaFormat):
        storage_options = {
            "AWS_ENDPOINT_URL": data_source.s3_endpoint_override,
        }

        return ibis.read_delta(data_source.path, storage_options=storage_options)


def _write_data_source(
    table: Table,
    data_source: DataSource,
    repo_path: str,
    mode: str = "append",
    allow_overwrite: bool = False,
):
    assert isinstance(data_source, FileSource)

    file_options = data_source.file_options

    absolute_path = FileSource.get_uri_for_file_path(
        repo_path=repo_path, uri=file_options.uri
    )

    if (
        mode == "overwrite"
        and not allow_overwrite
        and os.path.exists(str(absolute_path))
    ):
        raise SavedDatasetLocationAlreadyExists(location=file_options.uri)

    if isinstance(data_source.file_format, ParquetFormat):
        if mode == "overwrite":
            table = table.to_pyarrow()

            filesystem, path = FileSource.create_filesystem_and_path(
                str(absolute_path),
                file_options.s3_endpoint_override,
            )

            if path.endswith(".parquet"):
                pyarrow.parquet.write_table(table, where=path, filesystem=filesystem)
            else:
                # otherwise assume destination is directory
                pyarrow.parquet.write_to_dataset(
                    table, root_path=path, filesystem=filesystem
                )
        elif mode == "append":
            table = table.to_pyarrow()
            prev_table = ibis.read_parquet(file_options.uri).to_pyarrow()
            if table.schema != prev_table.schema:
                table = table.cast(prev_table.schema)
            new_table = pyarrow.concat_tables([table, prev_table])
            ibis.memtable(new_table).to_parquet(file_options.uri)
    elif isinstance(data_source.file_format, DeltaFormat):
        storage_options = {
            "AWS_ENDPOINT_URL": str(data_source.s3_endpoint_override),
        }

        if mode == "append":
            from deltalake import DeltaTable

            prev_schema = (
                DeltaTable(file_options.uri, storage_options=storage_options)
                .schema()
                .to_pyarrow()
            )
            table = table.cast(ibis.Schema.from_pyarrow(prev_schema))
            write_mode = "append"
        elif mode == "overwrite":
            write_mode = (
                "overwrite"
                if allow_overwrite and os.path.exists(file_options.uri)
                else "error"
            )

        table.to_delta(
            file_options.uri, mode=write_mode, storage_options=storage_options
        )


class DuckDBOfflineStoreConfig(FeastConfigBaseModel):
    """Configuration for DuckDB offline store.

    Attributes:
        type: Offline store type selector. Must be "duckdb".
        staging_location: Optional S3 path for staging data during remote exports.
        staging_location_endpoint_override: Custom S3 endpoint for staging location.
        s3_url_style: S3 URL style - "path" for path-style URLs (required for
            MinIO, LocalStack, etc.) or "vhost" for virtual-hosted style.
            Default is None which uses DuckDB's default (vhost).
        s3_endpoint: Custom S3 endpoint URL (e.g., "localhost:9000" for MinIO).
        s3_access_key_id: AWS access key ID for S3 authentication.
            If not set, uses AWS credential chain.
        s3_secret_access_key: AWS secret access key for S3 authentication.
            If not set, uses AWS credential chain.
        s3_region: AWS region for S3 access (e.g., "us-east-1").
            Required for some S3-compatible providers.
        s3_use_ssl: Whether to use SSL for S3 connections.
            Default is None which uses DuckDB's default (true).

    Example:
        For MinIO or other S3-compatible storage that requires path-style URLs:

        .. code-block:: yaml

            offline_store:
                type: duckdb
                s3_url_style: path
                s3_endpoint: localhost:9000
                s3_access_key_id: minioadmin
                s3_secret_access_key: minioadmin
                s3_region: us-east-1
                s3_use_ssl: false
    """

    type: StrictStr = "duckdb"

    staging_location: Optional[str] = None
    """S3 path for staging data during remote exports."""

    staging_location_endpoint_override: Optional[str] = None
    """Custom S3 endpoint for staging location."""

    # S3 configuration options for DuckDB's HTTPFS extension
    s3_url_style: Optional[Literal["path", "vhost"]] = None
    """S3 URL style - 'path' for path-style or 'vhost' for virtual-hosted style."""

    s3_endpoint: Optional[str] = None
    """Custom S3 endpoint URL (e.g., 'localhost:9000' for MinIO)."""

    s3_access_key_id: Optional[str] = None
    """AWS access key ID. If not set, uses AWS credential chain."""

    s3_secret_access_key: Optional[str] = None
    """AWS secret access key. If not set, uses AWS credential chain."""

    s3_region: Optional[str] = None
    """AWS region (e.g., 'us-east-1'). Required for some S3-compatible providers."""

    s3_use_ssl: Optional[bool] = None
    """Whether to use SSL for S3 connections. Default uses DuckDB's default (true)."""


class DuckDBOfflineStore(OfflineStore):
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
        # Configure S3 settings for DuckDB before reading data
        _configure_duckdb_for_s3(config.offline_store)

        return pull_latest_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_read_data_source,
            data_source_writer=_write_data_source,
            staging_location=config.offline_store.staging_location,
            staging_location_endpoint_override=config.offline_store.staging_location_endpoint_override,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        # Configure S3 settings for DuckDB before reading data
        _configure_duckdb_for_s3(config.offline_store)

        return get_historical_features_ibis(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            data_source_reader=_read_data_source,
            data_source_writer=_write_data_source,
            staging_location=config.offline_store.staging_location,
            staging_location_endpoint_override=config.offline_store.staging_location_endpoint_override,
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
        # Configure S3 settings for DuckDB before reading data
        _configure_duckdb_for_s3(config.offline_store)

        return pull_all_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_read_data_source,
            data_source_writer=_write_data_source,
            staging_location=config.offline_store.staging_location,
            staging_location_endpoint_override=config.offline_store.staging_location_endpoint_override,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        # Configure S3 settings for DuckDB before writing data
        _configure_duckdb_for_s3(config.offline_store)

        offline_write_batch_ibis(
            config=config,
            feature_view=feature_view,
            table=table,
            progress=progress,
            data_source_writer=_write_data_source,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        # Configure S3 settings for DuckDB before writing data
        _configure_duckdb_for_s3(config.offline_store)

        write_logged_features_ibis(
            config=config,
            data=data,
            source=source,
            logging_config=logging_config,
            registry=registry,
        )
