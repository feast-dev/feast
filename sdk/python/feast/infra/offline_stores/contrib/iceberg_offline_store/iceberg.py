import math
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import duckdb
import pandas as pd
import pyarrow as pa
from pydantic import Field
from pyiceberg.catalog import load_catalog

from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
    IcebergSource,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.utils import to_naive_utc

# SQL reserved words for DuckDB
# Reference: https://duckdb.org/docs/sql/keywords_and_identifiers
_SQL_RESERVED_WORDS = {
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
    "TRUE", "FALSE", "AS", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
    "CROSS", "FULL", "USING", "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC",
    "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "ALL", "DISTINCT",
    "CASE", "WHEN", "THEN", "ELSE", "END", "IF", "EXISTS", "BETWEEN", "LIKE",
    "ILIKE", "SIMILAR", "REGEXP", "GLOB", "CAST", "EXTRACT", "INTERVAL",
    "DATE", "TIME", "TIMESTAMP", "VARCHAR", "CHAR", "TEXT", "INTEGER", "INT",
    "BIGINT", "SMALLINT", "TINYINT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
    "BOOLEAN", "BLOB", "BINARY", "VARBINARY", "ARRAY", "STRUCT", "MAP",
    "CREATE", "DROP", "ALTER", "TABLE", "VIEW", "INDEX", "SCHEMA", "DATABASE",
    "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REPLACE", "VALUES", "SET",
    "PRAGMA", "EXPLAIN", "DESCRIBE", "SHOW", "WITH", "RECURSIVE", "OVER",
    "PARTITION", "WINDOW", "ROW", "ROWS", "RANGE", "PRECEDING", "FOLLOWING",
    "UNBOUNDED", "CURRENT", "TIES", "FIRST", "LAST", "NULLS", "PRIMARY",
    "FOREIGN", "KEY", "REFERENCES", "UNIQUE", "CHECK", "CONSTRAINT", "DEFAULT",
    "COLLATE", "FOR", "GRANT", "REVOKE", "TO", "ANALYZE", "VACUUM", "COPY",
    "EXPORT", "IMPORT", "LOAD", "INSTALL", "RETURNING", "ASOF", "LATERAL",
}


def validate_sql_identifier(identifier: str, context: str = "identifier") -> str:
    """Validate SQL identifier is safe for interpolation into queries.

    This function prevents SQL injection by ensuring identifiers contain only
    safe characters and are not SQL reserved words.

    Args:
        identifier: The identifier to validate (table name, column name, etc.)
        context: Description for error messages (e.g., "feature view name")

    Returns:
        The validated identifier (unchanged if valid)

    Raises:
        ValueError: If identifier contains unsafe characters or is a reserved word

    Examples:
        >>> validate_sql_identifier("my_table", "table name")
        'my_table'
        >>> validate_sql_identifier("user_id123", "column name")
        'user_id123'
        >>> validate_sql_identifier("SELECT", "table name")
        ValueError: SQL table name cannot be a reserved word: 'SELECT'
        >>> validate_sql_identifier("table; DROP TABLE users--", "table name")
        ValueError: Invalid SQL table name: 'table; DROP TABLE users--'...
    """
    if not identifier:
        raise ValueError(f"SQL {context} cannot be empty")

    # Validate identifier matches safe pattern: start with letter/underscore,
    # followed by letters, digits, or underscores
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(
            f"Invalid SQL {context}: '{identifier}'. "
            f"Only alphanumeric characters and underscores allowed, "
            f"must start with a letter or underscore."
        )

    # Check for SQL reserved words (case-insensitive)
    if identifier.upper() in _SQL_RESERVED_WORDS:
        raise ValueError(
            f"SQL {context} cannot be a reserved word: '{identifier}'"
        )

    return identifier


def _configure_duckdb_httpfs(con: duckdb.DuckDBPyConnection, storage_options: Dict[str, str]) -> None:
    """Configure DuckDB httpfs/S3 settings from Iceberg storage_options.

    This is required for S3-compatible warehouses (MinIO/R2/custom endpoints) when using
    DuckDB's `read_parquet([...])` fast path.

    SECURITY: Uses DuckDB's parameterized queries to avoid credential exposure in SQL strings.
    Credentials are never interpolated into SQL SET commands, preventing exposure in logs,
    error messages, and query history. Falls back to AWS environment variables if not provided.
    """
    import os

    # Extract S3 configuration from storage_options or environment variables
    s3_endpoint = storage_options.get("s3.endpoint") if storage_options else None
    s3_endpoint = s3_endpoint or os.getenv("S3_ENDPOINT")

    s3_region = storage_options.get("s3.region") if storage_options else None
    s3_region = s3_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")

    s3_access_key_id = storage_options.get("s3.access-key-id") if storage_options else None
    s3_access_key_id = s3_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")

    s3_secret_access_key = storage_options.get("s3.secret-access-key") if storage_options else None
    s3_secret_access_key = s3_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")

    s3_session_token = storage_options.get("s3.session-token") if storage_options else None
    s3_session_token = s3_session_token or os.getenv("AWS_SESSION_TOKEN")

    # Iceberg/PyIceberg supports `s3.path-style-access`.
    # Some docs use `s3.force-virtual-addressing` (the inverse of path-style).
    path_style_raw = storage_options.get("s3.path-style-access") if storage_options else None
    force_virtual_raw = storage_options.get("s3.force-virtual-addressing") if storage_options else None

    if any(
        v is not None
        for v in [
            s3_endpoint,
            s3_region,
            s3_access_key_id,
            s3_secret_access_key,
            s3_session_token,
            path_style_raw,
            force_virtual_raw,
        ]
    ):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

    # SECURITY FIX: Use DuckDB's parameterized queries to avoid credential exposure
    # This prevents credentials from appearing in SQL logs, error messages, or query history

    if s3_region:
        con.execute("SET s3_region = $1", [s3_region])

    if s3_endpoint:
        endpoint = str(s3_endpoint).rstrip('/')

        if endpoint.startswith('http://'):
            con.execute("SET s3_use_ssl = false")
            endpoint = endpoint.removeprefix('http://')
        elif endpoint.startswith('https://'):
            con.execute("SET s3_use_ssl = true")
            endpoint = endpoint.removeprefix('https://')

        con.execute("SET s3_endpoint = $1", [endpoint])

    # CRITICAL: Never use f-strings or string interpolation for credentials
    if s3_access_key_id:
        con.execute("SET s3_access_key_id = $1", [s3_access_key_id])

    if s3_secret_access_key:
        con.execute("SET s3_secret_access_key = $1", [s3_secret_access_key])

    if s3_session_token:
        con.execute("SET s3_session_token = $1", [s3_session_token])

    # DuckDB setting: s3_url_style = 'path' | 'vhost'
    if path_style_raw is not None:
        if str(path_style_raw).lower() == 'true':
            con.execute("SET s3_url_style = 'path'")

    if force_virtual_raw is not None:
        if str(force_virtual_raw).lower() == 'true':
            con.execute("SET s3_url_style = 'vhost'")


class IcebergOfflineStoreConfig(FeastConfigBaseModel):
    type: Literal["iceberg"] = "iceberg"
    """ Offline store type selector"""

    catalog_type: Optional[str] = "sql"
    """ Type of catalog (rest, sql, glue, hive, or None) """

    catalog_name: str = "default"
    """ Name of the catalog """

    uri: Optional[str] = "sqlite:///iceberg_catalog.db"
    """ URI for the catalog """

    warehouse: str = "warehouse"
    """ Warehouse path """

    namespace: str = "feast"
    """ Iceberg namespace """

    storage_options: Dict[str, str] = Field(default_factory=dict)
    """ Additional storage options (e.g., s3 credentials) """


class IcebergOfflineStore(OfflineStore):
    # Class-level catalog cache with thread-safe access
    _catalog_cache: Dict[Tuple, Any] = {}
    _cache_lock = threading.Lock()

    @classmethod
    def _get_cached_catalog(cls, config: IcebergOfflineStoreConfig) -> Any:
        """Get or create cached Iceberg catalog.

        Uses frozen config tuple as cache key to ensure catalog is reused
        across operations when config hasn't changed.

        Args:
            config: IcebergOfflineStoreConfig with catalog settings

        Returns:
            Cached or newly created Iceberg catalog instance
        """
        # Create immutable cache key from config
        cache_key = (
            config.catalog_type,
            config.catalog_name,
            config.uri,
            config.warehouse,
            frozenset(config.storage_options.items()) if config.storage_options else frozenset(),
        )

        with cls._cache_lock:
            if cache_key not in cls._catalog_cache:
                catalog_props = {
                    "type": config.catalog_type,
                    "uri": config.uri,
                    "warehouse": config.warehouse,
                    **config.storage_options,
                }
                catalog_props = {k: v for k, v in catalog_props.items() if v is not None}
                cls._catalog_cache[cache_key] = load_catalog(
                    config.catalog_name, **catalog_props
                )

        return cls._catalog_cache[cache_key]

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
        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            IcebergOfflineStoreConfig,
        )

        assert isinstance(config.offline_store, IcebergOfflineStoreConfig)

        # 1. Load Iceberg catalog (cached)
        catalog = IcebergOfflineStore._get_cached_catalog(config.offline_store)

        # 2. Setup DuckDB
        con = duckdb.connect(database=":memory:")
        _configure_duckdb_httpfs(con, config.offline_store.storage_options)

        # Register entity_df (must be a DataFrame for security)
        if not isinstance(entity_df, pd.DataFrame):
            raise ValueError(
                "entity_df must be a pandas DataFrame. "
                "SQL strings are not supported for security reasons."
            )
        con.register("entity_df", entity_df)

        # 3. For each feature view, load from Iceberg and register in DuckDB
        for fv in feature_views:
            assert isinstance(fv.batch_source, IcebergSource)

            # Validate feature view name for SQL safety
            fv_name = validate_sql_identifier(fv.name, "feature view name")

            # Validate timestamp field
            timestamp_field = validate_sql_identifier(
                fv.batch_source.timestamp_field, "timestamp field"
            )

            table_id = fv.batch_source.table_identifier
            if not table_id:
                raise ValueError(f"Table identifier missing for feature view {fv.name}")
            table = catalog.load_table(table_id)

            # Implement Hybrid Strategy: Fast-path for COW, Safe-path for MOR
            # Use streaming approach to avoid materializing all file metadata
            scan = table.scan()
            has_deletes = False
            file_paths = []

            for task in scan.plan_files():
                if task.delete_files:
                    has_deletes = True
                    break
                file_paths.append(task.file.file_path)

            if not has_deletes:
                # Fast Path: Read Parquet files directly in DuckDB
                if file_paths:
                    con.execute(
                        f"CREATE VIEW {fv_name} AS SELECT * FROM read_parquet({file_paths})"
                    )
                else:
                    # Empty table
                    empty_arrow = table.schema().as_arrow()
                    con.register(fv_name, pa.Table.from_batches([], schema=empty_arrow))
            else:
                # Safe Path: Use PyIceberg to resolve deletes into Arrow
                arrow_table = scan.to_arrow()
                con.register(fv_name, arrow_table)

        # 4. Construct ASOF join query with feature name handling
        query = "SELECT entity_df.*"
        for fv in feature_views:
            # Validate identifiers
            fv_name = validate_sql_identifier(fv.name, "feature view name")

            # Add all features from the feature view to SELECT clause
            for feature in fv.features:
                feature_col = validate_sql_identifier(feature.name, "feature column name")
                feature_name = feature.name
                if full_feature_names:
                    # Feature name for alias - validate the individual parts
                    feature_name = f"{fv.name}__{feature.name}"
                    feature_name = validate_sql_identifier(feature_name, "full feature name")
                else:
                    feature_name = validate_sql_identifier(feature_name, "feature name")
                query += f", {fv_name}.{feature_col} AS {feature_name}"

        query += " FROM entity_df"
        for fv in feature_views:
            assert isinstance(fv.batch_source, IcebergSource)

            # Validate identifiers
            fv_name = validate_sql_identifier(fv.name, "feature view name")
            timestamp_field = validate_sql_identifier(
                fv.batch_source.timestamp_field, "timestamp field"
            )

            # DuckDB ASOF JOIN:
            # 1. Join keys match exactly.
            # 2. Timestamp condition (entity_timestamp >= feature_timestamp).
            # 3. TTL filtering ensures features are fresh (within time-to-live window).
            # 4. Picks the latest feature record for each entity record.
            query += f" ASOF LEFT JOIN {fv_name} ON "
            # Use 'entity_df.event_timestamp' which is standard in Feast universal tests
            join_conds = []
            for k in fv.join_keys:
                join_key = validate_sql_identifier(k, "join key")
                join_conds.append(f"entity_df.{join_key} = {fv_name}.{join_key}")
            query += " AND ".join(join_conds)
            query += f" AND entity_df.event_timestamp >= {fv_name}.{timestamp_field}"

            # Add TTL filtering: feature must be within TTL window
            if fv.ttl and fv.ttl.total_seconds() > 0:
                ttl_seconds = fv.ttl.total_seconds()
                query += (
                    f" AND {fv_name}.{timestamp_field} >= "
                    f"entity_df.event_timestamp - INTERVAL '{ttl_seconds}' SECOND"
                )

        # Build metadata for the retrieval job
        metadata = RetrievalMetadata(
            features=feature_refs,
            keys=[key for fv in feature_views for key in fv.join_keys],
        )

        return IcebergRetrievalJob(
            con=con,
            query=query,
            full_feature_names=full_feature_names,
            metadata=metadata,
            config=config,
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: Any,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            IcebergOfflineStore,
        )

        # Reuse common setup logic
        con, source_table = IcebergOfflineStore._setup_duckdb_source(
            config, data_source, timestamp_field, start_date, end_date
        )

        # Validate all column names
        validated_join_keys = [
            validate_sql_identifier(col, "join key column") for col in join_key_columns
        ]
        validated_features = [
            validate_sql_identifier(col, "feature column") for col in feature_name_columns
        ]
        validated_timestamp = validate_sql_identifier(timestamp_field, "timestamp field")

        columns = validated_join_keys + validated_features + [validated_timestamp]
        if created_timestamp_column:
            validated_created = validate_sql_identifier(
                created_timestamp_column, "created timestamp column"
            )
            columns.append(validated_created)

        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {source_table}"

        return IcebergRetrievalJob(con, query)

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: Any,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> RetrievalJob:
        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            IcebergOfflineStore,
        )

        # Reuse common setup logic
        con, source_table = IcebergOfflineStore._setup_duckdb_source(
            config, data_source, timestamp_field, start_date, end_date
        )

        # 3. Construct "Latest" Query
        # Group by join keys and select the record with the maximum timestamp

        # Validate all column names
        validated_join_keys = [
            validate_sql_identifier(col, "join key column") for col in join_key_columns
        ]
        validated_features = [
            validate_sql_identifier(col, "feature column") for col in feature_name_columns
        ]
        validated_timestamp = validate_sql_identifier(timestamp_field, "timestamp field")

        join_keys_str = ", ".join(validated_join_keys)
        columns = validated_join_keys + validated_features + [validated_timestamp]
        if created_timestamp_column:
            validated_created = validate_sql_identifier(
                created_timestamp_column, "created timestamp column"
            )
            columns.append(validated_created)

        columns_str = ", ".join(columns)

        # Rank records by timestamp descending (with created_timestamp as tiebreaker) and pick rank 1
        order_by = f"{validated_timestamp} DESC"
        if created_timestamp_column:
            order_by += f", {validated_created} DESC"

        query = f"""
        SELECT {columns_str} FROM (
            SELECT *, row_number() OVER (PARTITION BY {join_keys_str} ORDER BY {order_by}) as rn
            FROM {source_table}
        ) WHERE rn = 1
        """

        return IcebergRetrievalJob(con, query)

    @staticmethod
    def _setup_duckdb_source(
        config: RepoConfig,
        data_source: Any,
        timestamp_field: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> Tuple[duckdb.DuckDBPyConnection, str]:
        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
            IcebergSource,
        )

        assert isinstance(data_source, IcebergSource)
        assert isinstance(config.offline_store, IcebergOfflineStoreConfig)

        # 1. Load Iceberg catalog (cached)
        catalog = IcebergOfflineStore._get_cached_catalog(config.offline_store)

        # 2. Setup DuckDB and Load Table
        con = duckdb.connect(database=":memory:")
        _configure_duckdb_httpfs(con, config.offline_store.storage_options)
        table_id = data_source.table_identifier
        if not table_id:
            raise ValueError(f"Table identifier missing for source {data_source.name}")
        table = catalog.load_table(table_id)

        # Validate timestamp field for SQL safety
        validated_timestamp = validate_sql_identifier(timestamp_field, "timestamp field")

        # Build row filter
        row_filters = []
        if start_date:
            start_date_naive = to_naive_utc(start_date)
            row_filters.append(f"{validated_timestamp} >= '{start_date_naive.isoformat()}'")
        if end_date:
            end_date_naive = to_naive_utc(end_date)
            row_filters.append(f"{validated_timestamp} <= '{end_date_naive.isoformat()}'")

        row_filter = " AND ".join(row_filters) if row_filters else None

        # Load filtered scan
        scan = table.scan(row_filter=row_filter) if row_filter else table.scan()

        # Use streaming approach to avoid materializing all file metadata
        # This prevents OOM for tables with 10,000+ data files
        has_deletes = False
        file_paths = []

        for task in scan.plan_files():
            if task.delete_files:
                has_deletes = True
                break
            file_paths.append(task.file.file_path)

        source_table = "source_table"
        if not has_deletes:
            # COW path: collect file paths and read Parquet directly in DuckDB
            if file_paths:
                con.execute(
                    f"CREATE VIEW {source_table} AS SELECT * FROM read_parquet({file_paths})"
                )
            else:
                con.register(source_table, scan.to_arrow())
        else:
            con.register(source_table, scan.to_arrow())

        return con, source_table

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pa.Table,
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Writes the specified arrow table to the Iceberg table underlying the specified feature view.

        Args:
            config: The config for the current feature store.
            feature_view: The feature view whose batch source should be written.
            table: The arrow table to write.
            progress: Function to be called once a portion of the data has been written.
        """
        assert isinstance(config.offline_store, IcebergOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, IcebergSource)

        # Load catalog (cached)
        catalog = IcebergOfflineStore._get_cached_catalog(config.offline_store)

        # Get table identifier from the feature view's batch source
        table_identifier = feature_view.batch_source.table_identifier
        if not table_identifier:
            raise ValueError(
                f"Table identifier missing for feature view {feature_view.name}"
            )

        try:
            iceberg_table = catalog.load_table(table_identifier)
            iceberg_table.append(table)
        except Exception:
            # Table doesn't exist, create it
            iceberg_table = IcebergOfflineStore._create_iceberg_table_from_arrow(
                catalog, table_identifier, table
            )
            iceberg_table.append(table)

        if progress:
            progress(len(table))

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pa.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ) -> None:
        """
        Writes logged features to an Iceberg table.

        Args:
            config: The config for the current feature store.
            data: An arrow table or a path to parquet directory containing the logs.
            source: The logging source that provides schema and metadata.
            logging_config: A LoggingConfig object that determines where logs will be written.
            registry: The registry for the current feature store.
        """
        assert isinstance(config.offline_store, IcebergOfflineStoreConfig)

        # Load the data if it's a path
        if isinstance(data, Path):
            import pyarrow.parquet as pq
            arrow_table = pq.read_table(str(data))
        else:
            arrow_table = data

        # Get the logging destination
        destination = logging_config.destination
        if destination is None:
            raise ValueError("LoggingConfig must have a destination configured")

        # Check if destination has Iceberg table identifier
        if hasattr(destination, "table_identifier") and destination.table_identifier:
            table_identifier = destination.table_identifier
        elif hasattr(destination, "path"):
            # Fall back to file-based logging
            import pyarrow.parquet as pq
            output_path = Path(destination.path)
            output_path.mkdir(parents=True, exist_ok=True)

            # Write as partitioned parquet
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = output_path / f"logged_features_{timestamp}.parquet"
            pq.write_table(arrow_table, str(file_path))
            return
        else:
            raise ValueError(
                f"Unsupported logging destination type: {type(destination)}. "
                "Use IcebergLoggingDestination or FileLoggingDestination."
            )

        # Load catalog (cached)
        catalog = IcebergOfflineStore._get_cached_catalog(config.offline_store)

        try:
            iceberg_table = catalog.load_table(table_identifier)
            iceberg_table.append(arrow_table)
        except Exception:
            # Table doesn't exist, create it
            iceberg_table = IcebergOfflineStore._create_iceberg_table_from_arrow(
                catalog, table_identifier, arrow_table
            )
            iceberg_table.append(arrow_table)

    @staticmethod
    def _create_iceberg_table_from_arrow(
        catalog,
        table_identifier: str,
        arrow_table: pa.Table,
    ):
        """Helper to create an Iceberg table from an Arrow schema."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            BinaryType,
            BooleanType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            NestedField,
            StringType,
            TimestampType,
        )

        def _arrow_to_iceberg_type(pa_type: pa.DataType):
            if pa_type == pa.bool_():
                return BooleanType()
            if pa_type == pa.int32():
                return IntegerType()
            if pa_type == pa.int64():
                return LongType()
            if pa_type == pa.float32():
                return FloatType()
            if pa_type == pa.float64():
                return DoubleType()
            if pa_type == pa.string() or pa_type == pa.utf8():
                return StringType()
            if pa_type == pa.binary():
                return BinaryType()
            if isinstance(pa_type, pa.TimestampType):
                return TimestampType()
            # Default to string for unsupported types
            return StringType()

        fields = []
        for i, field in enumerate(arrow_table.schema):
            fields.append(
                NestedField(
                    field_id=i + 1,
                    name=field.name,
                    type=_arrow_to_iceberg_type(field.type),
                    required=False,
                )
            )

        iceberg_schema = Schema(*fields)

        # Extract namespace from table identifier and create if needed
        parts = table_identifier.split(".")
        if len(parts) >= 2:
            namespace = ".".join(parts[:-1])
            try:
                catalog.create_namespace(namespace)
            except Exception:
                pass  # Namespace already exists

        return catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
        )


class IcebergRetrievalJob(RetrievalJob):
    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        full_feature_names: bool = False,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
        config: Optional[RepoConfig] = None,
    ):
        self.con = con
        self.query = query
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._config = config

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self.con.execute(self.query).df()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        return self.con.execute(self.query).arrow()

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """Returns metadata about the retrieval job."""
        return self._metadata

    def to_sql(self) -> str:
        """
        Returns the SQL query that will be executed in DuckDB to build the historical feature table.
        """
        return self.query

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> None:
        """
        Persists the retrieval job results to the specified storage.

        For Iceberg, this writes the results to an Iceberg table if SavedDatasetIcebergStorage
        is provided, otherwise falls back to writing a Parquet file.

        Args:
            storage: The saved dataset storage object specifying where the result should be persisted.
            allow_overwrite: If True, a pre-existing location can be overwritten.
            timeout: Optional query timeout.
        """
        # Get the Arrow table from the query
        arrow_table = self._to_arrow_internal(timeout=timeout)

        # Check if this is Iceberg-native storage
        if hasattr(storage, "table_identifier") and storage.table_identifier:
            # Iceberg-native persist
            self._persist_to_iceberg(arrow_table, storage, allow_overwrite)
        elif hasattr(storage, "file_options") and hasattr(storage.file_options, "uri"):
            # File-based persist (Parquet)
            self._persist_to_parquet(arrow_table, storage, allow_overwrite)
        else:
            raise ValueError(
                f"Unsupported storage type for IcebergRetrievalJob: {type(storage)}. "
                "Use SavedDatasetIcebergStorage or a file-based storage."
            )

    def _persist_to_iceberg(
        self,
        arrow_table: pa.Table,
        storage: SavedDatasetStorage,
        allow_overwrite: bool,
    ) -> None:
        """Persist results to an Iceberg table."""
        if self._config is None:
            raise ValueError(
                "RepoConfig is required for Iceberg persist. "
                "Ensure the retrieval job was created with config parameter."
            )

        assert isinstance(self._config.offline_store, IcebergOfflineStoreConfig)

        # Load catalog (cached)
        catalog = IcebergOfflineStore._get_cached_catalog(self._config.offline_store)

        table_identifier = storage.table_identifier  # type: ignore

        try:
            iceberg_table = catalog.load_table(table_identifier)
            if allow_overwrite:
                iceberg_table.overwrite(arrow_table)
            else:
                iceberg_table.append(arrow_table)
        except Exception:
            # Table doesn't exist, create it
            from pyiceberg.schema import Schema
            from pyiceberg.types import (
                BooleanType,
                DoubleType,
                FloatType,
                IntegerType,
                LongType,
                NestedField,
                StringType,
                TimestampType,
            )

            def _arrow_to_iceberg_type(pa_type: pa.DataType):
                if pa_type == pa.bool_():
                    return BooleanType()
                if pa_type == pa.int32():
                    return IntegerType()
                if pa_type == pa.int64():
                    return LongType()
                if pa_type == pa.float32():
                    return FloatType()
                if pa_type == pa.float64():
                    return DoubleType()
                if pa_type == pa.string() or pa_type == pa.utf8():
                    return StringType()
                if isinstance(pa_type, pa.TimestampType):
                    return TimestampType()
                # Default to string for unsupported types
                return StringType()

            fields = []
            for i, field in enumerate(arrow_table.schema):
                fields.append(
                    NestedField(
                        field_id=i + 1,
                        name=field.name,
                        type=_arrow_to_iceberg_type(field.type),
                        required=False,
                    )
                )

            iceberg_schema = Schema(*fields)

            # Extract namespace from table identifier
            parts = table_identifier.split(".")
            if len(parts) >= 2:
                namespace = ".".join(parts[:-1])
                try:
                    catalog.create_namespace(namespace)
                except Exception:
                    pass  # Namespace already exists

            iceberg_table = catalog.create_table(
                identifier=table_identifier,
                schema=iceberg_schema,
            )
            iceberg_table.append(arrow_table)

    def _persist_to_parquet(
        self,
        arrow_table: pa.Table,
        storage: SavedDatasetStorage,
        allow_overwrite: bool,
    ) -> None:
        """Persist results to a Parquet file."""
        import pyarrow.parquet as pq

        file_path = storage.file_options.uri  # type: ignore
        path = Path(file_path)

        if path.exists() and not allow_overwrite:
            raise ValueError(
                f"File {file_path} already exists. Set allow_overwrite=True to overwrite."
            )

        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        pq.write_table(arrow_table, file_path)
