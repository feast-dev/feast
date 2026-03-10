from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Union

import ibis
import pandas as pd
import pyarrow
from ibis.expr.types import Table
from pydantic import StrictInt, StrictStr, model_validator

from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.oracle_offline_store.oracle_source import (
    OracleSource,
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
    """Create an ibis Oracle connection from the offline store config."""
    offline_config = config.offline_store
    assert isinstance(offline_config, OracleOfflineStoreConfig)

    kwargs = {}
    if offline_config.service_name:
        kwargs["service_name"] = offline_config.service_name
    if offline_config.sid:
        kwargs["sid"] = offline_config.sid
    if offline_config.database:
        kwargs["database"] = offline_config.database
    if offline_config.dsn:
        kwargs["dsn"] = offline_config.dsn

    return ibis.oracle.connect(
        user=offline_config.user,
        password=offline_config.password,
        host=offline_config.host,
        port=offline_config.port,
        **kwargs,
    )


def _read_oracle_table(con, data_source: DataSource) -> Table:
    """Read an Oracle table via ibis.

    Column names are returned exactly as Oracle stores them.  The user is
    expected to reference columns using the same casing shown by Oracle
    (e.g. ``USER_ID`` for unquoted identifiers, ``CamelCase`` for quoted).
    """
    assert isinstance(data_source, OracleSource)
    table = con.table(data_source.table_ref)

    # Cast Oracle DATE columns (ibis date → timestamp) to preserve time.
    casts = {}
    for col_name in table.columns:
        if table[col_name].type().is_date():
            casts[col_name] = table[col_name].cast("timestamp")
    if casts:
        table = table.mutate(**casts)

    return table


def _build_data_source_reader(config: RepoConfig, con=None):
    """Build a reader that returns Oracle-backend ibis tables."""
    if con is None:
        con = get_ibis_connection(config)

    def _read_data_source(data_source: DataSource, repo_path: str = "") -> Table:
        return _read_oracle_table(con, data_source)

    return _read_data_source


def _build_data_source_writer(config: RepoConfig, con=None):
    """Build a function that writes data to an Oracle table via ibis."""
    if con is None:
        con = get_ibis_connection(config)

    def _write_data_source(
        table: Table,
        data_source: DataSource,
        repo_path: str = "",
        mode: str = "append",
        allow_overwrite: bool = False,
    ):
        assert isinstance(data_source, OracleSource)
        table_ref = data_source.table_ref

        if mode == "overwrite":
            if not allow_overwrite:
                raise ValueError(
                    f"Table '{table_ref}' already exists. "
                    f"Set allow_overwrite=True to truncate and replace data."
                )
            con.truncate_table(table_ref)

        con.insert(table_name=table_ref, obj=table.to_pandas())

    return _write_data_source


class OracleOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for Oracle Database"""

    type: Literal["oracle"] = "oracle"
    """Offline store type selector"""

    user: StrictStr
    """Oracle database user"""

    password: StrictStr
    """Oracle database password"""

    host: StrictStr = "localhost"
    """Oracle database host"""

    port: StrictInt = 1521
    """Oracle database port"""

    service_name: Optional[StrictStr] = None
    """Oracle service name (mutually exclusive with sid and dsn)"""

    sid: Optional[StrictStr] = None
    """Oracle SID (mutually exclusive with service_name and dsn)"""

    database: Optional[StrictStr] = None
    """Oracle database name"""

    dsn: Optional[StrictStr] = None
    """Oracle DSN string (mutually exclusive with service_name and sid)"""

    @model_validator(mode="after")
    def _validate_connection_params(self):
        exclusive = [
            f for f in ("service_name", "sid", "dsn") if getattr(self, f) is not None
        ]
        if len(exclusive) > 1:
            raise ValueError(
                f"Only one of 'service_name', 'sid', or 'dsn' may be set, "
                f"but got: {', '.join(exclusive)}"
            )
        return self


class OracleOfflineStore(OfflineStore):
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

        con = get_ibis_connection(config)

        return pull_latest_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_build_data_source_reader(config, con=con),
            data_source_writer=_build_data_source_writer(config, con=con),
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        **kwargs,
    ) -> RetrievalJob:
        if not feature_views:
            raise ValueError("feature_views must not be empty")

        # Single connection reused across the entire call.
        con = get_ibis_connection(config)

        # Handle non-entity retrieval mode (start_date/end_date only)
        if entity_df is None:
            start_date: Optional[datetime] = kwargs.get("start_date")
            end_date: Optional[datetime] = kwargs.get("end_date")

            if end_date is None:
                end_date = datetime.now(tz=timezone.utc)
            elif end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)

            if start_date is None:
                max_ttl_seconds = max(
                    (
                        int(fv.ttl.total_seconds())
                        for fv in feature_views
                        if fv.ttl and isinstance(fv.ttl, timedelta)
                    ),
                    default=0,
                )
                start_date = end_date - timedelta(
                    seconds=max_ttl_seconds if max_ttl_seconds > 0 else 30 * 86400
                )
            elif start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)

            # Build a synthetic entity_df from the feature source data
            all_entities: set = set()
            for fv in feature_views:
                all_entities.update(e.name for e in fv.entity_columns)

            entity_dfs = []
            for fv in feature_views:
                source = fv.batch_source
                table = _read_oracle_table(con, source)
                ts_col = source.timestamp_field
                join_keys = [e.name for e in fv.entity_columns]
                cols = join_keys + [ts_col]
                sub = table.filter(
                    (table[ts_col] >= ibis.literal(start_date))
                    & (table[ts_col] <= ibis.literal(end_date))
                ).select(cols)
                sub = sub.rename({"event_timestamp": ts_col})
                entity_dfs.append(sub.execute())

            entity_df = pd.concat(entity_dfs, ignore_index=True).drop_duplicates()

        # If entity_df is a SQL string, execute it to get a DataFrame
        if isinstance(entity_df, str):
            entity_df = con.sql(entity_df).execute()

        return get_historical_features_ibis(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            data_source_reader=_build_data_source_reader(config, con=con),
            data_source_writer=_build_data_source_writer(config, con=con),
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
        con = get_ibis_connection(config)

        return pull_all_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_build_data_source_reader(config, con=con),
            data_source_writer=_build_data_source_writer(config, con=con),
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
