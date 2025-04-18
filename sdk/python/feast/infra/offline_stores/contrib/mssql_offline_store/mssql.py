from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, List, Literal, Optional, Tuple, Union
from urllib import parse

import ibis
import pandas as pd
import pyarrow
from ibis.expr.types import Table
from pydantic import StrictStr

from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source import (
    MsSqlServerSource,
)
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


def get_ibis_connection(config: RepoConfig):
    connection_params = parse.urlparse(config.offline_store.connection_string)
    additional_kwargs = dict(parse.parse_qsl(connection_params.query))
    return ibis.mssql.connect(
        user=connection_params.username,
        password=connection_params.password,
        host=connection_params.hostname,
        port=connection_params.port,
        database=connection_params.path.strip("/"),
        **additional_kwargs,
    )


def get_table_column_names_and_types(
    config: RepoConfig, data_source: MsSqlServerSource
) -> Iterable[Tuple[str, str]]:
    con = get_ibis_connection(config)

    # assert isinstance(config.offline_store, MsSqlServerOfflineStoreConfig)
    # conn = create_engine(config.offline_store.connection_string)
    # self._mssqlserver_options.connection_str = (
    #     config.offline_store.connection_string
    # )
    name_type_pairs = []
    if len(data_source.table_ref.split(".")) == 2:
        database, table_name = data_source.table_ref.split(".")
        columns_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}' and table_schema = '{database}'
            """
    else:
        columns_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{data_source.table_ref}'
            """

    table_schema = con.sql(columns_query).execute()

    name_type_pairs.extend(
        list(
            zip(
                table_schema["COLUMN_NAME"].to_list(),
                table_schema["DATA_TYPE"].to_list(),
            )
        )
    )
    return name_type_pairs


def _build_data_source_reader(config: RepoConfig):
    con = get_ibis_connection(config)

    def _read_data_source(data_source: DataSource) -> Table:
        assert isinstance(data_source, MsSqlServerSource)
        return con.table(data_source.table_ref)

    return _read_data_source


def _build_data_source_writer(config: RepoConfig):
    con = get_ibis_connection(config)

    def _write_data_source(
        table: Table,
        data_source: DataSource,
        mode: str = "append",
        allow_overwrite: bool = False,
    ):
        assert isinstance(data_source, MsSqlServerSource)
        con.insert(table_name=data_source.table_ref, obj=table.to_pandas())

    return _write_data_source


def mssql_event_expire_timestamp_fn(timestamp_field: str, ttl: timedelta) -> str:
    ttl_seconds = int(ttl.total_seconds())
    return f"DATEADD(ss, {ttl_seconds}, {timestamp_field})"


class MsSqlServerOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for SQL Server"""

    type: Literal["mssql"] = "mssql"
    """ Offline store type selector"""

    connection_string: StrictStr = "mssql+pyodbc://sa:yourStrong(!)Password@localhost:1433/feast_test?driver=ODBC+Driver+17+for+SQL+Server"
    """Connection string containing the host, port, and configuration parameters for SQL Server
     format: SQLAlchemy connection string, e.g. mssql+pyodbc://sa:yourStrong(!)Password@localhost:1433/feast_test?driver=ODBC+Driver+17+for+SQL+Server"""


class MsSqlServerOfflineStore(OfflineStore):
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
            data_source_reader=_build_data_source_reader(config),
            data_source_writer=_build_data_source_writer(config),
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
        # TODO avoid this conversion
        if type(entity_df) == str:
            con = get_ibis_connection(config)
            entity_df = con.sql(entity_df).execute()

        return get_historical_features_ibis(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            data_source_reader=_build_data_source_reader(config),
            data_source_writer=_build_data_source_writer(config),
            event_expire_timestamp_fn=mssql_event_expire_timestamp_fn,
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
            data_source_reader=_build_data_source_reader(config),
            data_source_writer=_build_data_source_writer(config),
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
            data_source_writer=_build_data_source_writer(config),
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
