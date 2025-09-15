import sqlite3
from datetime import datetime
from typing import Any, Callable, List, Optional, Union

import pandas as pd
import pyarrow as pa

from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage

from .sqlite_source import SavedDatasetSQLiteStorage, SQLiteSource


class SQLiteRetrievalJob(RetrievalJob):
    """SQLite implementation of RetrievalJob."""

    def __init__(
        self,
        query: str,
        database: str,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        self.query = query
        self.database = database
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        with sqlite3.connect(self.database) as conn:
            return pd.read_sql_query(self.query, conn)

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        df = self._to_df_internal(timeout)
        return pa.Table.from_pandas(df)

    def to_sql(self) -> str:
        return self.query

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetSQLiteStorage)

        df = self.to_df(timeout=timeout)

        with sqlite3.connect(storage.database) as conn:
            if_exists = "replace" if allow_overwrite else "fail"
            df.to_sql(storage.table, conn, if_exists=if_exists, index=False)


class SQLiteOfflineStore(OfflineStore):
    """SQLite implementation of OfflineStore."""

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
        assert isinstance(data_source, SQLiteSource)

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string:
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )

        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"

        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT
                    {field_string},
                    ROW_NUMBER() OVER ({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS rn
                FROM {from_expression}
                WHERE {timestamp_field} >= '{start_date.isoformat()}'
                    AND {timestamp_field} <= '{end_date.isoformat()}'
            ) ranked
            WHERE rn = 1
        """

        return SQLiteRetrievalJob(
            query=query,
            database=data_source.database,
            full_feature_names=False,
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
    ) -> RetrievalJob:
        if not feature_views:
            raise ValueError("No feature views provided")

        # Get the main data source (assume all feature views use the same SQLite database for simplicity)
        main_source = feature_views[0].source
        assert isinstance(main_source, SQLiteSource)

        # Create entity dataframe table
        if isinstance(entity_df, pd.DataFrame):
            entity_table_name = "temp_entity_df"
            with sqlite3.connect(main_source.database) as conn:
                entity_df.to_sql(
                    entity_table_name, conn, if_exists="replace", index=False
                )
        elif isinstance(entity_df, str):
            entity_table_name = f"({entity_df})"
        else:
            raise ValueError("entity_df must be a pandas DataFrame or SQL query string")

        # Build feature selection
        feature_selections = []
        join_clauses = []

        for i, fv in enumerate(feature_views):
            fv_source = fv.source
            assert isinstance(fv_source, SQLiteSource)

            alias = f"fv_{i}"
            feature_selections.extend(
                [
                    f"{alias}.{feature.name}"
                    + (
                        f" AS {fv.name}__{feature.name}"
                        if full_feature_names
                        else f" AS {feature.name}"
                    )
                    for feature in fv.features
                    if any(
                        f"{fv.name}:{feature.name}" in ref or feature.name in ref
                        for ref in feature_refs
                    )
                ]
            )

            # Join condition based on entity columns
            join_conditions = []
            for entity in fv.entities:
                join_conditions.append(f"entity_df.{entity} = {alias}.{entity}")

            join_condition = " AND ".join(join_conditions)
            timestamp_condition = f"entity_df.{fv.source.timestamp_field or 'event_timestamp'} >= {alias}.{fv.source.timestamp_field or 'event_timestamp'}"

            join_clauses.append(f"""
                LEFT JOIN {fv_source.get_table_query_string()} {alias}
                ON {join_condition} AND {timestamp_condition}
            """)

        query = f"""
            SELECT
                entity_df.*,
                {", ".join(feature_selections)}
            FROM {entity_table_name} entity_df
            {" ".join(join_clauses)}
        """

        return SQLiteRetrievalJob(
            query=query,
            database=main_source.database,
            full_feature_names=full_feature_names,
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
        assert isinstance(data_source, SQLiteSource)

        from_expression = data_source.get_table_query_string()
        field_string = ", ".join(
            join_key_columns + feature_name_columns + [timestamp_field]
        )

        where_clauses = []
        if start_date:
            where_clauses.append(f"{timestamp_field} >= '{start_date.isoformat()}'")
        if end_date:
            where_clauses.append(f"{timestamp_field} <= '{end_date.isoformat()}'")

        where_clause = " AND ".join(where_clauses)
        if where_clause:
            where_clause = f"WHERE {where_clause}"

        query = f"""
            SELECT {field_string}
            FROM {from_expression}
            {where_clause}
        """

        return SQLiteRetrievalJob(
            query=query,
            database=data_source.database,
            full_feature_names=False,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pa.Table, str],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        if isinstance(data, pa.Table):
            df = data.to_pandas()
        else:
            # Assume it's a file path to parquet
            df = pd.read_parquet(data)

        # Write to SQLite
        assert isinstance(logging_config.destination, SQLiteSource)
        with sqlite3.connect(logging_config.destination.database) as conn:
            df.to_sql(
                logging_config.destination.table, conn, if_exists="append", index=False
            )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pa.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(feature_view.source, SQLiteSource)

        df = table.to_pandas()

        with sqlite3.connect(feature_view.source.database) as conn:
            df.to_sql(feature_view.source.table, conn, if_exists="append", index=False)

        if progress:
            progress(len(df))
