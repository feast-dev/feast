import contextlib
import sqlite3
from dataclasses import asdict
from datetime import datetime
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import numpy as np
import pandas as pd
import pyarrow as pa

from feast.data_source import DataSource
from feast.errors import (
    InvalidEntityType,
    ZeroColumnQueryResult,
    ZeroRowsQueryResult,
)
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import build_point_in_time_query
from feast.infra.offline_stores.sqlite_config import SQLiteOfflineStoreConfig
from feast.infra.offline_stores.sqlite_source import SQLiteSource
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
WITH entity_dataframe AS (
    SELECT *,
        strftime('%Y-%m-%d %H:%M:%f', {{entity_df_event_timestamp_col}}) AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS TEXT) || '_' ||
                {% endfor %}
                strftime('%Y-%m-%d %H:%M:%f', {{entity_df_event_timestamp_col}})
            ) AS {{featureview.name}}__entity_row_unique_id
            {% else %}
            ,strftime('%Y-%m-%d %H:%M:%f', {{entity_df_event_timestamp_col}}) AS {{featureview.name}}__entity_row_unique_id
            {% endif %}
        {% endfor %}
    FROM {{ left_table_query_string }}
)
{% for featureview in featureviews %}
,{{featureview.name}}__base AS (
    SELECT
        {% for entity in featureview.entities %}
            {{entity}},
        {% endfor %}
        {{featureview.timestamp_field}} as event_timestamp,
        {% for feature in featureview.features %}
            {{feature}} as {% if full_feature_names %}{{featureview.name}}__{{feature}}{% else %}{{feature}}{% endif %}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
    WHERE datetime({{featureview.timestamp_field}}) <= datetime((SELECT MAX(entity_timestamp) FROM entity_dataframe))
    {% if featureview.ttl == 0 %}{% else %}
    AND datetime({{featureview.timestamp_field}}) >= datetime((SELECT MIN(entity_timestamp) FROM entity_dataframe), '-{{featureview.ttl}} seconds')
    {% endif %}
)
,{{featureview.name}}__latest AS (
    SELECT
        {% for entity in featureview.entities %}
            {{entity}},
        {% endfor %}
        {% for feature in featureview.features %}
            {{feature}} as {% if full_feature_names %}{{featureview.name}}__{{feature}}{% else %}{{feature}}{% endif %}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {% for entity in featureview.entities %}{{entity}}{% if not loop.last %}, {% endif %}{% endfor %}
                ORDER BY datetime(event_timestamp) DESC
            ) as _feast_row
        FROM {{featureview.name}}__base
    ) t
    WHERE _feast_row = 1
)
{% endfor %}
SELECT entity_dataframe.*
    {% for featureview in featureviews %}
        {% for feature in featureview.features %}
        ,{{featureview.name}}__latest.{% if full_feature_names %}{{featureview.name}}__{{feature}}{% else %}{{feature}}{% endif %}
        {% endfor %}
    {% endfor %}
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN {{featureview.name}}__latest
ON {% for entity in featureview.entities %}
    entity_dataframe.{{entity}} = {{featureview.name}}__latest.{{entity}}
    {% if not loop.last %}AND {% endif %}
{% endfor %}
{% endfor %}
"""

SQLITE_TO_ARROW_TYPES = {
    "INTEGER": pa.int64(),
    "REAL": pa.float32(),
    "FLOAT": pa.float32(),
    "DOUBLE": pa.float32(),
    "TEXT": pa.string(),
    "BLOB": pa.binary(),
    "NUMERIC": pa.float32(),
    "BOOLEAN": pa.bool_(),
    "DATETIME": pa.timestamp("us"),
    "DATE": pa.date32(),
    "TIME": pa.time64("us"),
    "VARCHAR": pa.string(),
    "CHAR": pa.string(),
    "AVG": pa.float32(),  # Special case for aggregated columns
    "COUNT": pa.int64(),  # Special case for count aggregations
    "MAX": pa.float32(),  # Special case for max aggregations
    "MIN": pa.float32(),  # Special case for min aggregations
}


class SQLiteRetrievalJob(RetrievalJob):
    """SQLite retrieval job implementation.

    Handles the execution of point-in-time joins for feature values from a SQLite store.
    """

    def __init__(
        self,
        query: Union[
            str, Callable[[], ContextManager[Union[str, Tuple[str, Tuple[str, str]]]]]
        ],
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
        data_source: Optional[DataSource] = None,
    ):
        """Initialize a SQLite retrieval job.

        Args:
            query: SQL query to execute or a callable that returns a query
            config: Feature store configuration
            full_feature_names: Whether to include the feature view name as a prefix
            on_demand_feature_views: Optional list of on-demand feature views
            metadata: Optional metadata about the retrieval
            data_source: Optional data source for type inference
        """
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[Union[str, Tuple[str, Tuple[str, str]]]]:
                yield query

            self._query_generator = query_generator
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._df = None
        self._data_source = data_source

    def __getitem__(self, item):
        if self._df is None:
            self._df = self.to_df()
        return self._df[item]

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """Convert query results to a pandas DataFrame."""
        arrow_table = self._to_arrow_internal(timeout=timeout)
        # Convert Arrow table to pandas DataFrame with explicit type handling
        df = pd.DataFrame()
        for col_name in arrow_table.column_names:
            col = arrow_table[col_name]
            if pa.types.is_integer(col.type):
                # Convert integer columns to nullable Int64 with proper null handling
                values = col.to_numpy(zero_copy_only=False)
                # Create a pandas Series with Int64 dtype
                df[col_name] = pd.Series(values, dtype=pd.Int64Dtype())
            else:
                df[col_name] = col.to_pandas()
        return df

    def to_sql(self) -> str:
        """Returns the SQL query that will be executed."""
        with self._query_generator() as query_and_params:
            if isinstance(query_and_params, tuple):
                query = query_and_params[0]
            else:
                query = query_and_params
            print(f"Generated SQL query:\n{query}")
            return query

    def _to_arrow_internal(
        self,
        timeout: Optional[int] = None,
        join_key_columns: Optional[List[str]] = None,
    ) -> pa.Table:
        """Convert query results to an Arrow table.

        Args:
            timeout: Optional query timeout in seconds
            join_key_columns: Optional list of join key column names

        Returns:
            An Arrow table containing the query results

        Raises:
            ZeroColumnQueryResult: If the query returns no columns
            ZeroRowsQueryResult: If the query returns no rows
        """
        if join_key_columns is None:
            join_key_columns = []
        with self._query_generator() as query_and_params:
            if not isinstance(self.config.offline_store, SQLiteOfflineStoreConfig):
                raise TypeError("Offline store config must be SQLiteOfflineStoreConfig")
            store_config = self.config.offline_store
            path = getattr(store_config, "path", ":memory:")
            conn = getattr(store_config, "_conn", None)
            if conn is None:
                conn = sqlite3.connect(
                    str(path),
                    timeout=float(getattr(store_config, "connection_timeout", 5.0)),
                )
            cursor = conn.cursor()
            if isinstance(query_and_params, tuple):
                query, params = query_and_params
            else:
                query = query_and_params
                params = None

            # Add debug logging and error handling
            print(f"Executing SQL query:\n{query}")
            if params:
                print(f"With parameters: {params}")

            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                if not cursor.description:
                    print("Warning: No cursor description after query execution")
                    # Try to get column info from the table if available
                    if (
                        hasattr(self, "_data_source")
                        and self._data_source
                        and hasattr(self._data_source, "get_table_query_string")
                    ):
                        table_name = self._data_source.get_table_query_string()
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        table_info = cursor.fetchall()
                        if table_info:
                            print(f"Retrieved column info from table: {table_info}")
                        else:
                            raise ZeroColumnQueryResult(
                                query_and_params
                                if isinstance(query_and_params, str)
                                else query_and_params[0]
                            )
                    else:
                        raise ZeroColumnQueryResult(
                            query_and_params
                            if isinstance(query_and_params, str)
                            else query_and_params[0]
                        )
            except sqlite3.Error as e:
                print(f"SQLite error: {e}")
                raise
            data = cursor.fetchall()
            if not data:
                # Get column types from cursor description for empty table
                field_list: List[Tuple[str, pa.DataType]] = []
                for col in cursor.description:
                    col_name = str(col[0])
                    col_type_empty = str(col[1]).upper()
                    if "INTEGER" in col_type_empty:
                        arrow_type = pa.int64()
                    elif "REAL" in col_type_empty or "NUMERIC" in col_type_empty:
                        arrow_type = pa.float64()
                    elif "BOOLEAN" in col_type_empty:
                        arrow_type = pa.bool_()
                    elif "DATETIME" in col_type_empty:
                        arrow_type = pa.timestamp("us")
                    elif "DATE" in col_type_empty:
                        arrow_type = pa.date32()
                    elif "TIME" in col_type_empty:
                        arrow_type = pa.time64("us")
                    else:
                        arrow_type = pa.string()
                    field_list.append((col_name, arrow_type))
                schema = pa.schema(field_list)
                empty_arrays = [pa.array([], type=field.type) for field in schema]
                return pa.Table.from_arrays(empty_arrays, schema=schema)

            # Create schema based on table info
            schema_fields: List[Tuple[str, pa.DataType]] = []
            print("Using data type inference from cursor description")

            # First pass: collect column info and data
            arrays: List[pa.Array] = []
            for col_idx, col in enumerate(cursor.description):
                col_name = str(col[0])
                print(f"Processing column {col_name}")

                # Extract column data and determine type
                col_data: List[Any] = [row[col_idx] for row in data]
                description_tuple = cursor.description[col_idx]
                col_type_raw: Optional[str] = (
                    description_tuple[1] if description_tuple is not None else None
                )
                col_type: str = str(col_type_raw) if col_type_raw is not None else ""
                col_type_upper: str = col_type.upper()

                # Convert data based on SQLite column type
                if "INTEGER" in col_type_upper or col_name in join_key_columns:
                    # Convert to integers with proper null handling
                    int_data = []
                    for val in col_data:
                        if val is None:
                            int_data.append(None)
                        else:
                            try:
                                if isinstance(val, (int, np.integer)):
                                    int_data.append(int(val))
                                elif isinstance(val, str):
                                    int_data.append(int(float(val.strip())))
                                else:
                                    int_data.append(int(float(val)))
                            except (ValueError, TypeError):
                                int_data.append(None)
                    arrays.append(pa.array(int_data, type=pa.int64()))
                    schema_fields.append((col_name, pa.int64()))
                    continue
                elif any(
                    t in col_type_upper for t in ["REAL", "FLOAT", "DOUBLE", "NUMERIC"]
                ):
                    arrow_type = pa.float64()
                    float_data: List[Optional[float]] = []
                    for val in col_data:
                        if val is None:
                            float_data.append(None)
                        else:
                            try:
                                if isinstance(val, str):
                                    val = val.strip()
                                float_data.append(float(val))
                            except (ValueError, TypeError):
                                float_data.append(None)
                    arrays.append(pa.array(float_data, type=arrow_type))
                    schema_fields.append((col_name, arrow_type))
                    continue
                elif "BOOLEAN" in col_type_upper or col_name.lower().endswith("_bool"):
                    arrow_type = pa.bool_()
                    bool_data: List[Optional[bool]] = []
                    for val in col_data:
                        if val is None:
                            bool_data.append(None)
                        elif isinstance(val, bool):
                            bool_data.append(val)
                        elif isinstance(val, (int, float)):
                            bool_data.append(bool(int(val)))
                        elif isinstance(val, str):
                            val_lower = val.lower().strip()
                            bool_data.append(
                                val_lower in ("1", "true", "t", "yes", "y", "on")
                            )
                        else:
                            bool_data.append(None)
                    arrays.append(pa.array(bool_data, type=arrow_type))
                    schema_fields.append((col_name, arrow_type))
                    continue
                elif any(t in col_type_upper for t in ["DATETIME", "TIMESTAMP"]):
                    arrow_type = pa.timestamp("us")
                    timestamp_data: List[Optional[np.datetime64]] = []
                    for val in col_data:
                        if val is None:
                            timestamp_data.append(None)
                        else:
                            try:
                                if isinstance(val, (float, int)):
                                    dt = pd.Timestamp(val, unit="s")
                                elif isinstance(val, str):
                                    if "T" in val:
                                        dt = pd.to_datetime(val, format="ISO8601")
                                    else:
                                        dt = pd.to_datetime(val)
                                else:
                                    dt = pd.Timestamp(val)
                                timestamp_data.append(dt.to_numpy())
                            except (ValueError, TypeError):
                                timestamp_data.append(None)
                    arrays.append(pa.array(timestamp_data, type=arrow_type))
                    schema_fields.append((col_name, arrow_type))
                    continue
                elif "DATE" in col_type_upper:
                    arrow_type = pa.date32()
                    date_data: List[Optional[np.datetime64]] = []
                    for val in col_data:
                        if val is None:
                            date_data.append(None)
                        else:
                            try:
                                if isinstance(val, str):
                                    dt = pd.to_datetime(val).normalize()
                                else:
                                    dt = pd.Timestamp(val).normalize()
                                date_data.append(dt.to_datetime64())
                            except (ValueError, TypeError):
                                date_data.append(None)
                    arrays.append(pa.array(date_data, type=arrow_type))
                    schema_fields.append((col_name, arrow_type))
                    continue
                elif "TIME" in col_type_upper:
                    arrow_type = pa.time64("us")
                    time_data: List[Optional[np.datetime64]] = [
                        pd.Timestamp(val).to_datetime64() if val is not None else None
                        for val in col_data
                    ]
                    arrays.append(pa.array(time_data, type=arrow_type))
                    schema_fields.append((col_name, arrow_type))
                    continue
                else:
                    arrow_type = pa.string()
                    str_data: List[Optional[str]] = [
                        str(val) if val is not None else None for val in col_data
                    ]
                    arrays.append(pa.array(str_data, type=arrow_type))
                    schema_fields.append((col_name, arrow_type))
                    continue

            schema = pa.schema(schema_fields)
            return pa.Table.from_arrays(arrays, schema=schema)

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        """Persist the retrieval data to SQLite storage.

        Args:
            storage: Storage configuration for the saved dataset
            allow_overwrite: If True, overwrite existing data
            timeout: Optional query timeout in seconds
        """
        df = self.to_df()
        if not isinstance(self.config.offline_store, SQLiteOfflineStoreConfig):
            raise TypeError("Offline store config must be SQLiteOfflineStoreConfig")
        store_config = self.config.offline_store
        path = getattr(store_config, "path", ":memory:")
        conn = sqlite3.connect(
            str(path), timeout=float(getattr(store_config, "connection_timeout", 5.0))
        )
        try:
            table_name = "feast_persisted_df"
            df.to_sql(
                table_name,
                conn,
                if_exists="replace" if allow_overwrite else "fail",
                index=False,
            )
        finally:
            conn.close()


class SQLiteOfflineStore(OfflineStore):
    """SQLite offline store implementation.

    This store provides an implementation of an offline store using SQLite, which is useful
    for local development, testing, and small-scale deployments.

    Example:
        >>> from feast import RepoConfig
        >>> from feast.infra.offline_stores.sqlite import SQLiteOfflineStoreConfig
        >>> config = RepoConfig(
        ...     project="my_project",
        ...     provider="local",
        ...     registry="data/registry.db",
        ...     offline_store=SQLiteOfflineStoreConfig(
        ...         type="sqlite",
        ...         path="feast.db"
        ...     )
        ... )
    """

    def __init__(self):
        """Initialize the SQLite offline store."""
        self._conn = None

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
        if not isinstance(data_source, SQLiteSource):
            raise ValueError(
                f"data_source must be SQLiteSource, not {type(data_source)}"
            )
        """Retrieves the latest feature values for the specified columns.

        Args:
            config: The configuration for the feature store
            data_source: Data source to pull features from
            join_key_columns: Columns of the join keys
            feature_name_columns: Columns of the feature names
            timestamp_field: Name of the timestamp field
            created_timestamp_column: Name of the created timestamp column
            start_date: Starting date of the data to retrieve
            end_date: Ending date of the data to retrieve

        Returns:
            RetrievalJob containing the latest feature values
        """
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        from_expression = data_source.get_table_query_string()

        timestamps = [timestamp_field]
        if created_timestamp_column and created_timestamp_column != timestamp_field:
            timestamps.append(created_timestamp_column)

        # Get column types from data source
        column_types = {
            col_name: col_type
            for col_name, col_type in data_source.get_table_column_names_and_types(
                config
            )
        }

        # Build field string with explicit type casting based on actual column types
        all_columns = join_key_columns + feature_name_columns + timestamps
        field_string = ", ".join(
            [
                f"CAST({col} AS INTEGER) AS {col}"
                if col in join_key_columns
                or (col in column_types and "INTEGER" in column_types[col].upper())
                else f"CAST(CASE WHEN {col} IN (1, '1', 'true', 't', 'yes', 'y') THEN 1 ELSE 0 END AS BOOLEAN) AS {col}"
                if col in column_types and "BOOLEAN" in column_types[col].upper()
                else f"{col} AS {col}"
                for col in dict.fromkeys(all_columns)
            ]
        )

        query = f"""
            WITH latest_records AS (
                SELECT {field_string},
                    ROW_NUMBER() OVER (
                        PARTITION BY {', '.join(join_key_columns)}
                        ORDER BY datetime({timestamp_field}) DESC
                    ) as row_num
                FROM {from_expression}
                WHERE datetime({timestamp_field}) >= datetime('{start_date.isoformat()}')
                AND datetime({timestamp_field}) < datetime('{end_date.isoformat()}')
            )
            SELECT {field_string}
            FROM latest_records
            WHERE row_num = 1
        """

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            yield query

        job = SQLiteRetrievalJob(
            query=query_generator,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
            data_source=data_source,
        )
        df = job.to_df()

        # Convert integer columns based on SQLite type information
        if isinstance(data_source, SQLiteSource):
            column_types = dict(data_source.get_table_column_names_and_types(config))
            for col in df.columns:
                if col in join_key_columns or (col in column_types and "INTEGER" in column_types[col].upper()):
                    try:
                        numeric_series = pd.to_numeric(df[col], errors="coerce")
        # Handle feature columns based on their SQLite types
        for col in feature_name_columns:
            try:
                if df[col].dtype == "object":
                    # Try to convert boolean columns first
                    if all(
                        isinstance(x, (int, type(None))) for x in df[col].dropna()
                    ) and set(df[col].dropna().unique()).issubset({0, 1}):
                        df[col] = df[col].astype("bool")
                    else:
                        # Try to convert numeric columns
                        numeric_col = pd.to_numeric(df[col], errors="coerce")
                        if numeric_col.dtype == "int64":
                            df[col] = pd.Series(numeric_col.values, dtype="Int64")
                        else:
                            df[col] = numeric_col.astype("float64")
            except (ValueError, TypeError):
                pass
                    except (ValueError, TypeError):
                        pass

        # Handle feature columns based on their SQLite types
        for col in feature_name_columns:
            try:
                if df[col].dtype == "object":
                    # Try to convert boolean columns first
                    if all(
                        isinstance(x, (int, type(None))) for x in df[col].dropna()
                    ) and set(df[col].dropna().unique()).issubset({0, 1}):
                        df[col] = df[col].astype("bool")
                    else:
                        # Try to convert numeric columns
                        numeric_col = pd.to_numeric(df[col], errors="coerce")
                        if numeric_col.dtype == "int64":
                            df[col] = pd.Series(numeric_col.values, dtype="Int64")
                        else:
                            df[col] = numeric_col.astype("float64")
            except (ValueError, TypeError):
                pass
                        df[col] = pd.Series(numeric_series, dtype=pd.Int64Dtype())</old_str>
                    except (ValueError, TypeError):
                        pass

        # Handle feature columns based on their SQLite types
        for col in feature_name_columns:
            try:
                if df[col].dtype == "object":
                    # Try to convert boolean columns first
                    if all(
                        isinstance(x, (int, type(None))) for x in df[col].dropna()
                    ) and set(df[col].dropna().unique()).issubset({0, 1}):
                        df[col] = df[col].astype("bool")
                    else:
                        # Try to convert numeric columns
                        numeric_col = pd.to_numeric(df[col], errors="coerce")
                        if numeric_col.dtype == "int64":
                            df[col] = pd.Series(numeric_col.values, dtype="Int64")
                        else:
                            df[col] = numeric_col.astype("float64")
            except (ValueError, TypeError):
                pass

        return job

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
        """Retrieves historical feature values from the SQLite offline store.

        Args:
            config: The configuration for the feature store
            feature_views: List of feature views to fetch features from
            feature_refs: List of feature references to retrieve
            entity_df: Entity DataFrame or SQL query
            registry: Registry instance
            project: Feast project name
            full_feature_names: Whether to include the feature view name as a prefix

        Returns:
            RetrievalJob containing the historical feature values
        """
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, DataSource)

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
                    [
                        offline_utils.FeatureViewQueryContext(**context)
                        for context in query_context_dict
                    ],
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )
            finally:
                if not isinstance(config.offline_store, SQLiteOfflineStoreConfig):
                    raise TypeError(
                        "Offline store config must be SQLiteOfflineStoreConfig"
                    )
                store_config = config.offline_store
                path = getattr(store_config, "path", ":memory:")
                conn_timeout = float(getattr(store_config, "connection_timeout", 5.0))
                conn = sqlite3.connect(str(path), timeout=conn_timeout)
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    conn.commit()
                finally:
                    conn.close()

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


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    config: RepoConfig,
) -> Tuple[datetime, datetime]:
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
        return entity_df_event_timestamp_range
    elif isinstance(entity_df, str):
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        conn = sqlite3.connect(
            str(config.offline_store.path)
            if config.offline_store and config.offline_store.path
            else ":memory:",
            timeout=float(config.offline_store.connection_timeout)
            if config.offline_store
            and hasattr(config.offline_store, "connection_timeout")
            else 5.0,
        )
        try:
            cursor = conn.cursor()
            query = f"""
                SELECT
                    MIN(datetime({entity_df_event_timestamp_col})) AS min_timestamp,
                    MAX(datetime({entity_df_event_timestamp_col})) AS max_timestamp
                FROM ({entity_df})
            """
            cursor.execute(query)
            res = cursor.fetchone()
            if not res or not res[0] or not res[1]:
                raise ZeroRowsQueryResult(query)
            return (
                datetime.fromisoformat(res[0].replace("Z", "+00:00")),
                datetime.fromisoformat(res[1].replace("Z", "+00:00")),
            )
        finally:
            conn.close()
    else:
        raise InvalidEntityType(type(entity_df))


def _upload_entity_df(
    config: RepoConfig, entity_df: Union[pd.DataFrame, str], table_name: str
):
    assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
    path = getattr(config.offline_store, "path", ":memory:")
    conn_timeout = float(getattr(config.offline_store, "connection_timeout", 5.0))
    conn = sqlite3.connect(str(path), timeout=conn_timeout)
    try:
        if isinstance(entity_df, pd.DataFrame):
            entity_df.to_sql(table_name, conn, if_exists="replace", index=False)
        elif isinstance(entity_df, str):
            cursor = conn.cursor()
            cursor.execute(f"CREATE TABLE {table_name} AS {entity_df}")
            conn.commit()
        else:
            raise InvalidEntityType(type(entity_df))
    finally:
        conn.close()


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    config: RepoConfig,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        assert isinstance(config.offline_store, SQLiteOfflineStoreConfig)
        conn = sqlite3.connect(
            str(config.offline_store.path)
            if config.offline_store and config.offline_store.path
            else ":memory:",
            timeout=float(config.offline_store.connection_timeout)
            if config.offline_store
            and hasattr(config.offline_store, "connection_timeout")
            else 5.0,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM ({entity_df}) LIMIT 0")
            return {description[0]: np.dtype("O") for description in cursor.description}
        finally:
            conn.close()
    else:
        raise InvalidEntityType(type(entity_df))


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
WITH entity_dataframe AS (
    SELECT *,
        strftime('%Y-%m-%d %H:%M:%f', {{entity_df_event_timestamp_col}}) AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS TEXT) || '_' ||
                {% endfor %}
                strftime('%Y-%m-%d %H:%M:%f', {{entity_df_event_timestamp_col}})
            ) AS {{featureview.name}}__entity_row_unique_id
            {% else %}
            ,strftime('%Y-%m-%d %H:%M:%f', {{entity_df_event_timestamp_col}}) AS {{featureview.name}}__entity_row_unique_id
            {% endif %}
        {% endfor %}
    FROM {{ left_table_query_string }}
)
{% for featureview in featureviews %}
,{{featureview.name}}__entity_dataframe AS (
    SELECT
        {% if featureview.entities %}{{featureview.entities | join(', ')}},{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY
        {% if featureview.entities %}{{featureview.entities | join(', ')}},{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
)
,{{featureview.name}}__subquery AS (
    SELECT
        strftime('%Y-%m-%d %H:%M:%f', {{featureview.timestamp_field}}) as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            {{feature}} as {% if full_feature_names %}{{featureview.name}}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{featureview.field_mapping.get(feature, feature)}}{% endif %}{% if loop.last %}{% else %},{% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
    WHERE datetime({{featureview.timestamp_field}}) <= datetime((SELECT MAX(entity_timestamp) FROM entity_dataframe))
    {% if featureview.ttl == 0 %}{% else %}
    AND datetime({{featureview.timestamp_field}}) >= datetime((SELECT MIN(entity_timestamp) FROM entity_dataframe), '-{{featureview.ttl}} seconds')
    {% endif %}
)
,{{featureview.name}}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{featureview.name}}__subquery AS subquery
    INNER JOIN {{featureview.name}}__entity_dataframe AS entity_dataframe
    ON TRUE
        AND datetime(subquery.event_timestamp) <= datetime(entity_dataframe.entity_timestamp)
        {% if featureview.ttl == 0 %}{% else %}
        AND datetime(subquery.event_timestamp) >= datetime(entity_dataframe.entity_timestamp, '-{{featureview.ttl}} seconds')
        {% endif %}
        {% for entity in featureview.entities %}
        AND CAST(subquery.{{entity}} AS TEXT) = CAST(entity_dataframe.{{entity}} AS TEXT)
        {% endfor %}
)
{% endfor %}
SELECT
    entity_dataframe.*
    {% for featureview in featureviews %}
        {% for feature in featureview.features %}
            ,COALESCE({{featureview.name}}__cleaned.{% if full_feature_names %}{{featureview.name}}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{featureview.field_mapping.get(feature, feature)}}{% endif %}, NULL) AS {% if full_feature_names %}{{featureview.name}}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{featureview.field_mapping.get(feature, feature)}}{% endif %}
        {% endfor %}
    {% endfor %}
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        {% for feature in featureview.features %}
            MAX({% if full_feature_names %}{{featureview.name}}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{featureview.field_mapping.get(feature, feature)}}{% endif %}) AS {% if full_feature_names %}{{featureview.name}}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{featureview.field_mapping.get(feature, feature)}}{% endif %}{% if loop.last %}{% else %},{% endif %}
        {% endfor %}
    FROM {{featureview.name}}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id
) {{featureview.name}}__cleaned
ON entity_dataframe.{{featureview.name}}__entity_row_unique_id = {{featureview.name}}__cleaned.{{featureview.name}}__entity_row_unique_id
{% endfor %}
"""