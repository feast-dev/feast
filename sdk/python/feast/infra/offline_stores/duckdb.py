import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Optional, Union

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


def _read_data_source(data_source: DataSource, repo_path: str) -> Table:
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
    type: StrictStr = "duckdb"
    # """ Offline store type selector"""

    staging_location: Optional[str] = None

    staging_location_endpoint_override: Optional[str] = None


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
        write_logged_features_ibis(
            config=config,
            data=data,
            source=source,
            logging_config=logging_config,
            registry=registry,
        )
