import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow
from pydantic import StrictStr

from feast import Entity
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.utils import _get_requested_feature_views_to_features_dict


class SqliteOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for SQLite"""

    type: Literal["sqlite"] = "sqlite"
    """ Offline store type selector"""

    path: Optional[StrictStr] = "data/offline.db"
    """ Path to the SQLite database file """


class SqliteSource(DataSource):
    """
    SQLite data source implementation for reading from a SQLite database.

    Attributes:
        database: Path to the SQLite database file
        query: SQL query to execute against the SQLite database
        timestamp_field: Event timestamp field used for point in time joins
        created_timestamp_column: Timestamp column indicating when the row was created
        field_mapping: Dictionary mapping of field names in this data source to feature names in a feature table
        date_partition_column: Timestamp column used for partitioning
    """

    def __init__(
        self,
        database: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """
        Create a SQLiteSource object.

        Args:
            database: Path to the SQLite database file
            query: SQL query to execute against the SQLite database
            table: Table name to query from
            timestamp_field: Event timestamp field used for point in time joins
            created_timestamp_column: Timestamp column indicating when the row was created
            field_mapping: Dictionary mapping of field names in this data source to feature names in a feature table
            date_partition_column: Timestamp column used for partitioning
            name: Name for the source
        """
        self.database = database
        self.query = query
        self.table = table

        if not self.query and self.table:
            self.query = f"SELECT * FROM {self.table}"

        super().__init__(
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            name=name or "",
        )

    @staticmethod
    def from_proto(data_source_proto: Any):
        """
        Convert data source config proto to SQLiteSource object.

        Args:
            data_source_proto: Proto object

        Returns:
            SqliteSource object
        """
        raise NotImplementedError("Conversion from proto not implemented for SQLiteSource")

    def to_proto(self) -> Any:
        """
        Convert SQLiteSource object to data source config proto.

        Returns:
            Proto object
        """
        raise NotImplementedError("Conversion to proto not implemented for SQLiteSource")

    def validate(self, config: RepoConfig):
        """
        Validate the SQLiteSource configuration.

        Args:
            config: Repo configuration object
        """
        if not self.database:
            raise ValueError("database is required for SQLiteSource")
        if not self.query and not self.table:
            raise ValueError("Either query or table must be specified for SQLiteSource")

    def get_table_query_string(self) -> str:
        """
        Return the table query string to use for querying this data source.

        Returns:
            Table query string
        """
        return self.query if self.query else f"SELECT * FROM {self.table}"


class SqliteRetrievalJob(RetrievalJob):
    """
    Retrieval job for SQLite offline store.
    """

    def __init__(
        self,
        query: str,
        database_path: str,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[Any]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """
        Initialize a SQLiteRetrievalJob object.

        Args:
            query: SQL query to execute against the SQLite database
            database_path: Path to the SQLite database file
            full_feature_names: Whether to include the feature view name as a prefix in the feature names
            on_demand_feature_views: List of on-demand feature views to apply
            metadata: Metadata information about the retrieval
        """
        self.query = query
        self.database_path = database_path
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        """
        Return whether the feature names should be prefixed with the feature view name.

        Returns:
            Boolean indicating whether to use full feature names
        """
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[Any]:
        """
        Return the list of on-demand feature views to apply.

        Returns:
            List of on-demand feature views
        """
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """
        Execute the retrieval query and return the result as a pandas DataFrame.

        Args:
            timeout: Timeout in seconds

        Returns:
            Pandas DataFrame containing the query results
        """
        conn = sqlite3.connect(self.database_path)
        try:
            return pd.read_sql_query(self.query, conn)
        finally:
            conn.close()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        """
        Execute the retrieval query and return the result as a pyarrow Table.

        Args:
            timeout: Timeout in seconds

        Returns:
            PyArrow Table containing the query results
        """
        conn = sqlite3.connect(self.database_path)
        try:
            df = pd.read_sql_query(self.query, conn)
            return pyarrow.Table.from_pandas(df)
        finally:
            conn.close()

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """
        Return metadata information about the retrieval.

        Returns:
            Metadata object
        """
        return self._metadata


class SqliteOfflineStore(OfflineStore):
    """
    Offline store implementation for SQLite.
    """

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
        """
        Pull the latest records from a SQLite table or query.

        Args:
            config: Repo configuration object
            data_source: Data source to pull from
            join_key_columns: List of columns to join on
            feature_name_columns: List of feature columns to select
            timestamp_field: Timestamp field used for point in time joins
            created_timestamp_column: Timestamp column indicating when the row was created
            start_date: Start date to pull data from
            end_date: End date to pull data from

        Returns:
            RetrievalJob containing the query results
        """
        assert isinstance(data_source, SqliteSource)
        assert isinstance(config.offline_store, SqliteOfflineStoreConfig)

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamp_columns = [timestamp_field]
        if created_timestamp_column:
            timestamp_columns.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamp_columns) + " DESC"
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_columns
        )

        start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

        query = f"""
            SELECT
                {field_string}
                {f", '{DUMMY_ENTITY_VAL}' AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM ({from_expression})
                WHERE {timestamp_field} BETWEEN '{start_date_str}' AND '{end_date_str}'
            )
            WHERE _feast_row = 1
            """

        database_path = _get_database_path(config)
        return SqliteRetrievalJob(
            query=query,
            database_path=database_path,
            full_feature_names=False,
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
        """
        Pull all records from a SQLite table or query.

        Args:
            config: Repo configuration object
            data_source: Data source to pull from
            join_key_columns: List of columns to join on
            feature_name_columns: List of feature columns to select
            timestamp_field: Timestamp field used for point in time joins
            start_date: Start date to pull data from
            end_date: End date to pull data from

        Returns:
            RetrievalJob containing the query results
        """
        assert isinstance(data_source, SqliteSource)
        assert isinstance(config.offline_store, SqliteOfflineStoreConfig)

        from_expression = data_source.get_table_query_string()

        field_string = ", ".join(
            join_key_columns + feature_name_columns + [timestamp_field]
        )

        start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

        query = f"""
            SELECT {field_string}
            FROM ({from_expression})
            WHERE {timestamp_field} BETWEEN '{start_date_str}' AND '{end_date_str}'
        """

        database_path = _get_database_path(config)
        return SqliteRetrievalJob(
            query=query,
            database_path=database_path,
            full_feature_names=False,
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
        """
        Get historical features from the SQLite offline store.

        Args:
            config: Repo configuration object
            feature_views: List of feature views to fetch from
            feature_refs: List of feature references to fetch
            entity_df: Entity DataFrame or SQL query
            registry: Registry object
            project: Project name
            full_feature_names: Whether to include the feature view name as a prefix in the feature names

        Returns:
            RetrievalJob containing the historical features
        """
        assert isinstance(config.offline_store, SqliteOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, SqliteSource)

        database_path = _get_database_path(config)
        conn = sqlite3.connect(database_path)

        entity_schema = _get_entity_schema(entity_df, conn)

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            conn,
        )

        if isinstance(entity_df, pd.DataFrame):
            entity_table_name = offline_utils.get_temp_entity_table_name()
            _upload_entity_df(entity_df, conn, entity_table_name)
        else:
            entity_table_name = f"({entity_df})"

        expected_join_keys = offline_utils.get_expected_join_keys(
            project, feature_views, registry
        )

        offline_utils.assert_expected_columns_in_entity_df(
            entity_schema, expected_join_keys, entity_df_event_timestamp_col
        )

        query_context = offline_utils.get_feature_view_query_context(
            feature_refs,
            feature_views,
            registry,
            project,
            entity_df_event_timestamp_range,
        )

        query = offline_utils.build_point_in_time_query(
            query_context,
            left_table_query_string=entity_table_name,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            entity_df_columns=entity_schema.keys(),
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )

        conn.close()

        return SqliteRetrievalJob(
            query=query,
            database_path=database_path,
            full_feature_names=full_feature_names,
            on_demand_feature_views=list(_get_requested_feature_views_to_features_dict(
                feature_refs, feature_views, registry.list_on_demand_feature_views(project)
            )[1].keys()),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(entity_schema.keys() - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        """
        Write a batch of features to the SQLite offline store.

        Args:
            config: Repo configuration object
            feature_view: Feature view to write to
            table: PyArrow table containing the features
            progress: Optional progress callback
        """
        assert isinstance(config.offline_store, SqliteOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, SqliteSource)

        database_path = _get_database_path(config)
        conn = sqlite3.connect(database_path)

        df = table.to_pandas()

        table_name = feature_view.batch_source.table

        columns = ", ".join([f'"{col}" TEXT' for col in df.columns])
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")

        df.to_sql(table_name, conn, if_exists="append", index=False)

        conn.commit()
        conn.close()

        if progress:
            progress(len(df))


def _get_database_path(config: RepoConfig) -> str:
    """
    Get the SQLite database path from the config.

    Args:
        config: Repo configuration object

    Returns:
        Path to the SQLite database file
    """
    assert isinstance(config.offline_store, SqliteOfflineStoreConfig)
    
    store_path = config.offline_store.path or "data/offline.db"
    
    if config.repo_path and not Path(store_path).is_absolute():
        db_path = str(config.repo_path / store_path)
    else:
        db_path = store_path
    
    return str(db_path)


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    conn: sqlite3.Connection,
) -> Dict[str, np.dtype]:
    """
    Get the schema of the entity DataFrame.

    Args:
        entity_df: Entity DataFrame or SQL query
        conn: SQLite connection

    Returns:
        Dictionary mapping column names to their numpy dtypes
    """
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        df_sample = pd.read_sql_query(f"{entity_df} LIMIT 1", conn)
        return dict(zip(df_sample.columns, df_sample.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    conn: sqlite3.Connection,
) -> Tuple[datetime, datetime]:
    """
    Get the event timestamp range from the entity DataFrame.

    Args:
        entity_df: Entity DataFrame or SQL query
        entity_df_event_timestamp_col: Name of the event timestamp column
        conn: SQLite connection

    Returns:
        Tuple containing the minimum and maximum event timestamps
    """
    if isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    elif isinstance(entity_df, str):
        query = f"SELECT MIN({entity_df_event_timestamp_col}) AS min, MAX({entity_df_event_timestamp_col}) AS max FROM ({entity_df})"
        result = conn.execute(query).fetchone()
        entity_df_event_timestamp_range = (
            pd.to_datetime(result[0], utc=True).to_pydatetime(),
            pd.to_datetime(result[1], utc=True).to_pydatetime(),
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _upload_entity_df(
    entity_df: pd.DataFrame,
    conn: sqlite3.Connection,
    table_name: str,
):
    """
    Upload the entity DataFrame to a temporary table in the SQLite database.

    Args:
        entity_df: Entity DataFrame
        conn: SQLite connection
        table_name: Name of the temporary table
    """
    entity_df.to_sql(table_name, conn, if_exists="replace", index=False)


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST({{entity}} as TEXT) ||
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS TEXT)
            ) AS {{featureview.name}}__entity_row_unique_id
            {% else %}
            ,CAST({{entity_df_event_timestamp_col}} AS TEXT) AS {{featureview.name}}__entity_row_unique_id
            {% endif %}
        {% endfor %}
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

{{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY
        {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.
 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `timestamp_field`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `timestamp_field`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously
 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.timestamp_field }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
    WHERE {{ featureview.timestamp_field }} <= '{{ featureview.max_event_timestamp }}'
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.timestamp_field }} >= '{{ featureview.min_event_timestamp }}'
    {% endif %}
),

{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= datetime(entity_dataframe.entity_timestamp, '-{{ featureview.ttl }} seconds')
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        {{featureview.name}}__entity_row_unique_id
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY {{featureview.name}}__entity_row_unique_id
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS row_number
        FROM {{ featureview.name }}__base
        {% if featureview.created_timestamp_column %}
            INNER JOIN {{ featureview.name }}__dedup
            USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
        {% endif %}
    )
    WHERE row_number = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT {{ final_output_feature_names | join(', ')}}
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
