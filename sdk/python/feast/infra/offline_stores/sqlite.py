"""SQLite offline store implementation."""

from __future__ import annotations

import contextlib
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    KeysView,
    List,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet
from jinja2 import BaseLoader, Environment
from typing_extensions import Literal

from feast.data_source import DataSource
from feast.errors import (
    InvalidEntityType,
    ZeroColumnQueryResult,
    ZeroRowsQueryResult,
)
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
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

__all__ = ["SQLiteOfflineStore", "SQLiteOfflineStoreConfig"]


class SQLiteOfflineStoreConfig(RepoConfig):
    """SQLite offline store config."""

    type: Literal["sqlite"] = "sqlite"  # type: ignore[valid-type]
    path: str = ":memory:"  # Default to in-memory SQLite

    def __init__(self, **data):
        super().__init__(**data)
        self.offline_store = self  # type: ignore[assignment]
        self.path: str = data.get("path", ":memory:")


def _get_conn(path: Optional[str]) -> sqlite3.Connection:
    """Get a SQLite connection with proper configuration."""
    if path is None:
        raise ValueError("SQLite path cannot be None")
    conn = sqlite3.connect(path)
    # Enable foreign key support
    conn.execute("PRAGMA foreign_keys = ON")
    # Use write-ahead logging for better concurrency
    conn.execute("PRAGMA journal_mode = WAL")
    # Enable extended result codes for better error handling
    conn.execute("PRAGMA extended_result_codes = ON")
    return conn


def df_to_sqlite_table(
    df: pd.DataFrame,
    path: str,
    table_name: str,
) -> None:
    """Save a pandas DataFrame to a SQLite table."""
    with _get_conn(path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)


def get_table_column_names_and_types(
    path: str,
    table_name: str,
) -> Dict[str, np.dtype]:
    """Get column names and types for a SQLite table."""
    with _get_conn(path) as conn:
        # Get schema information
        cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 0")
        return dict(
            (desc[0], _sqlite_type_to_np_type(desc[1])) for desc in cursor.description
        )


def _sqlite_type_to_np_type(sqlite_type: Optional[str]) -> np.dtype:
    """Convert SQLite type to numpy dtype."""
    if not sqlite_type:
        return np.dtype("O")
    type_map = {
        "TEXT": np.dtype("O"),
        "INTEGER": np.dtype("int64"),
        "REAL": np.dtype("float64"),
        "NUMERIC": np.dtype("float64"),
        "BOOLEAN": np.dtype("bool"),
        "TIMESTAMP": np.dtype("datetime64[ns]"),
        "DATETIME": np.dtype("datetime64[ns]"),
    }
    return type_map.get(sqlite_type.upper(), np.dtype("O"))


class SQLiteRetrievalJob(RetrievalJob):
    """SQLite implementation of RetrievalJob."""

    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize SQLiteRetrievalJob."""
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        """Return whether full feature names are used."""
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        """Return list of on demand feature views."""
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """Convert query result to pandas DataFrame."""
        return self._to_arrow_internal().to_pandas()

    def to_sql(self) -> str:
        """Return the SQL query."""
        with self._query_generator() as query:
            return query

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        """Execute query and return result as Arrow table."""
        assert isinstance(self.config.offline_store, SQLiteOfflineStoreConfig)
        with self._query_generator() as query:
            with _get_conn(self.config.offline_store.path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                if not cursor.description:
                    raise ZeroColumnQueryResult(query)
                # Get column names and types
                fields = [
                    (desc[0], _sqlite_type_to_arrow(desc[1]))
                    for desc in cursor.description
                ]
                # Fetch all data
                data = cursor.fetchall()
                schema = pa.schema(fields)
                # Transpose data for Arrow table construction
                data_transposed: List[List[Any]] = []
                for col in range(len(fields)):
                    data_transposed.append([])
                    for row in range(len(data)):
                        data_transposed[col].append(data[row][col])

                return pa.Table.from_arrays(
                    [pa.array(row) for row in data_transposed], schema=schema
                )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """Return metadata about the retrieval."""
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        """Persist the retrieval result."""
        assert isinstance(storage, SavedDatasetSQLiteStorage)
        assert isinstance(storage, SavedDatasetStorage)  # For type checking
        assert hasattr(storage, "sqlite_options")  # For type checking

        df_to_sqlite_table(
            df=self.to_df(),
            path=storage.sqlite_options._path,  # type: ignore
            table_name=storage.sqlite_options._table,  # type: ignore
        )

    def supports_remote_storage_export(self) -> bool:
        """Returns whether this retrieval job supports exporting to remote storage."""
        return False

    def to_remote_storage(self) -> List[str]:
        """Not supported for SQLite."""
        raise NotImplementedError(
            "SQLite offline store does not support exporting to remote storage."
        )


def _sqlite_type_to_arrow(sqlite_type: Optional[str]) -> pa.DataType:
    """Convert SQLite type to Arrow type."""
    if not sqlite_type:
        return pa.string()
    type_map = {
        "TEXT": pa.string(),
        "INTEGER": pa.int64(),
        "REAL": pa.float64(),
        "NUMERIC": pa.float64(),
        "BOOLEAN": pa.bool_(),
        "TIMESTAMP": pa.timestamp("us"),
        "DATETIME": pa.timestamp("us"),
    }
    return type_map.get(sqlite_type.upper(), pa.string())


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST("{{entity}}" as TEXT) ||
                {% endfor %}
                CAST("{{entity_df_event_timestamp_col}}" AS TEXT)
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% else %}
            ,CAST("{{entity_df_event_timestamp_col}}" AS TEXT) AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
        {% endfor %}
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

"{{ featureview.name }}__entity_dataframe" AS (
    SELECT
        {% if featureview.entities %}"{{ featureview.entities | join('", "') }}",{% endif %}
        entity_timestamp,
        "{{featureview.name}}__entity_row_unique_id"
    FROM entity_dataframe
    GROUP BY
        {% if featureview.entities %}"{{ featureview.entities | join('", "')}}",{% endif %}
        entity_timestamp,
        "{{featureview.name}}__entity_row_unique_id"
),

"{{ featureview.name }}__subquery" AS (
    SELECT
        "{{ featureview.timestamp_field }}" as event_timestamp,
        {{ '"' ~ featureview.created_timestamp_column ~ '" as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            "{{ feature }}" as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} AS sub
    WHERE datetime("{{ featureview.timestamp_field }}") <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND datetime("{{ featureview.timestamp_field }}") >= datetime((SELECT MIN(entity_timestamp) FROM entity_dataframe), '-{{ featureview.ttl }} seconds')
    {% endif %}
),

"{{ featureview.name }}__base" AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe."{{featureview.name}}__entity_row_unique_id"
    FROM "{{ featureview.name }}__subquery" AS subquery
    INNER JOIN "{{ featureview.name }}__entity_dataframe" AS entity_dataframe
    ON TRUE
        AND datetime(subquery.event_timestamp) <= datetime(entity_dataframe.entity_timestamp)
        {% if featureview.ttl == 0 %}{% else %}
        AND datetime(subquery.event_timestamp) >= datetime(entity_dataframe.entity_timestamp, '-{{ featureview.ttl }} seconds')
        {% endif %}
        {% for entity in featureview.entities %}
        AND subquery."{{ entity }}" = entity_dataframe."{{ entity }}"
        {% endfor %}
),

{% if featureview.created_timestamp_column %}
"{{ featureview.name }}__dedup" AS (
    SELECT
        "{{featureview.name}}__entity_row_unique_id",
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM "{{ featureview.name }}__base"
    GROUP BY "{{featureview.name}}__entity_row_unique_id", event_timestamp
),
{% endif %}

"{{ featureview.name }}__latest" AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %},
            "{{ feature }}" as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}
        {% endfor %}
    FROM (
        SELECT base.*,
            ROW_NUMBER() OVER(
                PARTITION BY base."{{featureview.name}}__entity_row_unique_id"
                ORDER BY
                    {% if featureview.created_timestamp_column %}
                    dedup.created_timestamp DESC,
                    {% endif %}
                    base.event_timestamp DESC
            ) AS _feast_row
        FROM "{{ featureview.name }}__base" base
        {% if featureview.created_timestamp_column %}
        INNER JOIN "{{ featureview.name }}__dedup" dedup
        ON TRUE
            AND base."{{featureview.name}}__entity_row_unique_id" = dedup."{{featureview.name}}__entity_row_unique_id"
            AND base.event_timestamp = dedup.event_timestamp
            AND base.created_timestamp = dedup.created_timestamp
        {% endif %}
    )
    WHERE _feast_row = 1
),

{% endfor %}

"features_timestamps" AS (
    SELECT
        {{ "\"" ~ final_output_feature_names | join('", "') ~ "\"" }},
        entity_timestamp
    FROM entity_dataframe
    {% for featureview in featureviews %}
    LEFT JOIN "{{ featureview.name }}__latest"
    ON TRUE
        AND entity_dataframe."{{featureview.name}}__entity_row_unique_id" = "{{ featureview.name }}__latest"."{{featureview.name}}__entity_row_unique_id"
    {% endfor %}
)

SELECT * FROM features_timestamps
"""


def _append_alias(field_names: List[str], alias: str) -> List[str]:
    """Append an alias to field names."""
    return [f'{alias}."{field_name}"' for field_name in field_names]


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    config: RepoConfig,
) -> Dict[str, np.dtype]:
    """Get the schema of an entity DataFrame."""
    assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        df_query = f"({entity_df}) AS sub"
        return get_table_column_names_and_types(
            config.offline_store.path,
            df_query,
        )
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    config: RepoConfig,
) -> Tuple[datetime, datetime]:
    """Get the event timestamp range from an entity DataFrame."""
    assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
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
        # If the entity_df is a string (SQL query), determine range from table
        with _get_conn(config.offline_store.path) as conn:
            cursor = conn.cursor()
            query = (
                "SELECT "
                f"MIN(datetime({entity_df_event_timestamp_col})) AS min, "
                f"MAX(datetime({entity_df_event_timestamp_col})) AS max "
                f"FROM ({entity_df}) AS tmp_alias"
            )
            cursor.execute(query)
            res = cursor.fetchone()
            if not res:
                raise ZeroRowsQueryResult(query)
            entity_df_event_timestamp_range = (res[0], res[1])
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _upload_entity_df(
    config: RepoConfig,
    entity_df: Union[pd.DataFrame, str],
    table_name: str,
) -> None:
    """Upload an entity DataFrame to SQLite."""
    assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
    if isinstance(entity_df, pd.DataFrame):
        df_to_sqlite_table(
            df=entity_df,
            path=config.offline_store.path,
            table_name=table_name,
        )
    elif isinstance(entity_df, str):
        with _get_conn(config.offline_store.path) as conn:
            conn.execute(f"CREATE TABLE {table_name} AS ({entity_df})")
    else:
        raise InvalidEntityType(type(entity_df))


def build_point_in_time_query(
    feature_view_query_contexts: List[dict],
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
    entity_df_columns: KeysView[str],
    query_template: str,
    full_feature_names: bool = False,
) -> str:
    """Build point-in-time query between each feature view table and the entity dataframe for SQLite."""
    template = Environment(loader=BaseLoader()).from_string(source=query_template)

    final_output_feature_names = list(entity_df_columns)
    final_output_feature_names.extend(
        [
            (
                f"{fv['name']}__{fv['field_mapping'].get(feature, feature)}"
                if full_feature_names
                else fv["field_mapping"].get(feature, feature)
            )
            for fv in feature_view_query_contexts
            for feature in fv["features"]
        ]
    )

    # Add additional fields to dict
    template_context = {
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv["entities"]]
        ),
        "featureviews": feature_view_query_contexts,
        "full_feature_names": full_feature_names,
        "final_output_feature_names": final_output_feature_names,
    }

    query = template.render(template_context)
    return query


class SQLiteOfflineStore(OfflineStore):
    """SQLite offline store implementation."""

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
        """Gets the latest values of features within a time range.

        Args:
            config: The config for the current feature store.
            data_source: Data source to query from.
            join_key_columns: Columns of the join keys.
            feature_name_columns: Columns of the features.
            timestamp_field: Timestamp column used for point-in-time joins.
            created_timestamp_column: Column indicating when the row was created.
            start_date: Start of the time range.
            end_date: End of the time range.

        Returns:
            RetrievalJob containing the latest feature values.
        """
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        assert isinstance(data_source, SQLiteSource)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(_append_alias(join_key_columns, "a"))
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(_append_alias(timestamps, "a")) + " DESC"
        a_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "a")
        )
        b_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "b")
        )

        # Convert timestamps to SQLite format
        start_date = start_date.astimezone(tz=timezone.utc)
        end_date = end_date.astimezone(tz=timezone.utc)

        query = f"""
            SELECT
                {b_field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {a_field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression} a
                WHERE datetime(a."{timestamp_field}") BETWEEN datetime('{start_date}') AND datetime('{end_date}')
            ) b
            WHERE _feast_row = 1
            """

        return SQLiteRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
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
        """Retrieves historical feature values.

        Args:
            config: The config for the current feature store.
            feature_views: List of feature views to fetch from.
            feature_refs: List of feature references to fetch.
            entity_df: DataFrame containing the entities and timestamps.
            registry: The registry for the current feature store.
            project: The project context.
            full_feature_names: Whether to include the feature view name as a prefix.

        Returns:
            RetrievalJob containing the historical feature values.
        """
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, SQLiteSource)

        entity_schema = _get_entity_schema(entity_df, config)

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            config,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
            table_name = offline_utils.get_temp_entity_table_name()

            _upload_entity_df(config, entity_df, table_name)

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

            query_context_dict = [asdict(context) for context in query_context]

            try:
                yield build_point_in_time_query(
                    query_context_dict,
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )
            finally:
                if table_name:
                    with _get_conn(config.offline_store.path) as conn:
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        return SQLiteRetrievalJob(
            query=query_generator,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(entity_schema.keys() - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
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
        """Gets all values of features within a time range.

        Args:
            config: The config for the current feature store.
            data_source: Data source to query from.
            join_key_columns: Columns of the join keys.
            feature_name_columns: Columns of the features.
            timestamp_field: Timestamp column used for point-in-time joins.
            start_date: Start of the time range.
            end_date: End of the time range.

        Returns:
            RetrievalJob containing all feature values.
        """
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        assert isinstance(data_source, SQLiteSource)
        from_expression = data_source.get_table_query_string()

        field_string = ", ".join(
            join_key_columns + feature_name_columns + [timestamp_field]
        )

        start_date = start_date.astimezone(tz=timezone.utc)
        end_date = end_date.astimezone(tz=timezone.utc)

        query = f"""
            SELECT {field_string}
            FROM {from_expression} AS paftoq_alias
            WHERE datetime("{timestamp_field}") BETWEEN datetime('{start_date}') AND datetime('{end_date}')
        """

        return SQLiteRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pa.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        """Write logged features to SQLite."""
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        if isinstance(data, Path):
            import pyarrow.parquet

            table = pyarrow.parquet.read_table(data)
        else:
            table = data

        df = table.to_pandas()
        df_to_sqlite_table(
            df=df,
            path=config.offline_store.path,
            table_name=str(logging_config.destination),  # Convert destination to str
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pa.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        """Write a batch of features to SQLite."""
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, SQLiteSource)

        df = table.to_pandas()
        source = feature_view.batch_source
        df_to_sqlite_table(
            df=df,
            path=config.offline_store.path,  # Use config path instead of source path
            table_name=source.get_table_query_string(),  # Use public method
        )
        if progress:
            progress(len(df))
