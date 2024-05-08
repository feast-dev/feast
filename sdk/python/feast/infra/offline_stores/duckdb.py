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


def _read_data_source(data_source: DataSource) -> Table:
    assert isinstance(data_source, FileSource)

    if isinstance(data_source.file_format, ParquetFormat):
        return ibis.read_parquet(data_source.path)
    elif isinstance(data_source.file_format, DeltaFormat):
        return ibis.read_delta(data_source.path)


def _write_data_source(table: pyarrow.Table, data_source: DataSource):
    assert isinstance(data_source, FileSource)

    file_options = data_source.file_options

    if isinstance(data_source.file_format, ParquetFormat):
        prev_table = ibis.read_parquet(file_options.uri).to_pyarrow()
        if table.schema != prev_table.schema:
            table = table.cast(prev_table.schema)
        new_table = pyarrow.concat_tables([table, prev_table])
        ibis.memtable(new_table).to_parquet(file_options.uri)
    elif isinstance(data_source.file_format, DeltaFormat):
        from deltalake import DeltaTable

        prev_schema = DeltaTable(file_options.uri).schema().to_pyarrow()
        if table.schema != prev_schema:
            table = table.cast(prev_schema)
        ibis.memtable(table).to_delta(file_options.uri, mode="append")


class DuckDBOfflineStoreConfig(FeastConfigBaseModel):
    type: StrictStr = "duckdb"
    # """ Offline store type selector"""


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
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        return pull_all_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_read_data_source,
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
