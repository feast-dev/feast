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


def _configure_duckdb_httpfs(con: duckdb.DuckDBPyConnection, storage_options: Dict[str, str]) -> None:
    """Configure DuckDB httpfs/S3 settings from Iceberg storage_options.

    This is required for S3-compatible warehouses (MinIO/R2/custom endpoints) when using
    DuckDB's `read_parquet([...])` fast path.
    """

    if not storage_options:
        return

    def _sql_str(value: str) -> str:
        return value.replace("'", "''")

    s3_endpoint = storage_options.get("s3.endpoint")
    s3_region = storage_options.get("s3.region")
    s3_access_key_id = storage_options.get("s3.access-key-id")
    s3_secret_access_key = storage_options.get("s3.secret-access-key")
    s3_session_token = storage_options.get("s3.session-token")

    # Iceberg/PyIceberg supports `s3.path-style-access`.
    # Some docs use `s3.force-virtual-addressing` (the inverse of path-style).
    path_style_raw = storage_options.get("s3.path-style-access")
    force_virtual_raw = storage_options.get("s3.force-virtual-addressing")

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

    if s3_region:
        con.execute(f"SET s3_region='{_sql_str(s3_region)}'")

    if s3_endpoint:
        endpoint = str(s3_endpoint).rstrip('/')

        if endpoint.startswith('http://'):
            con.execute("SET s3_use_ssl=false")
            endpoint = endpoint.removeprefix('http://')
        elif endpoint.startswith('https://'):
            con.execute("SET s3_use_ssl=true")
            endpoint = endpoint.removeprefix('https://')

        con.execute(f"SET s3_endpoint='{_sql_str(endpoint)}'")

    if s3_access_key_id:
        con.execute(f"SET s3_access_key_id='{_sql_str(s3_access_key_id)}'")

    if s3_secret_access_key:
        con.execute(f"SET s3_secret_access_key='{_sql_str(s3_secret_access_key)}'")

    if s3_session_token:
        con.execute(f"SET s3_session_token='{_sql_str(s3_session_token)}'")

    # DuckDB setting: s3_url_style = 'path' | 'vhost'
    if path_style_raw is not None:
        if str(path_style_raw).lower() == 'true':
            con.execute("SET s3_url_style='path'")

    if force_virtual_raw is not None:
        if str(force_virtual_raw).lower() == 'true':
            con.execute("SET s3_url_style='vhost'")


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

        # 1. Load Iceberg catalog
        catalog_props = {
            "type": config.offline_store.catalog_type,
            "uri": config.offline_store.uri,
            "warehouse": config.offline_store.warehouse,
            **config.offline_store.storage_options,
        }
        # Filter out None values
        catalog_props = {k: v for k, v in catalog_props.items() if v is not None}

        catalog = load_catalog(
            config.offline_store.catalog_name,
            **catalog_props,
        )

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
            table_id = fv.batch_source.table_identifier
            if not table_id:
                raise ValueError(f"Table identifier missing for feature view {fv.name}")
            table = catalog.load_table(table_id)

            # Implement Hybrid Strategy: Fast-path for COW, Safe-path for MOR
            scan = table.scan()
            tasks = list(scan.plan_files())
            has_deletes = any(task.delete_files for task in tasks)

            if not has_deletes:
                # Fast Path: Read Parquet files directly in DuckDB
                file_paths = [task.file.file_path for task in tasks]
                if file_paths:
                    con.execute(
                        f"CREATE VIEW {fv.name} AS SELECT * FROM read_parquet({file_paths})"
                    )
                else:
                    # Empty table
                    empty_arrow = table.schema().as_arrow()
                    con.register(fv.name, pa.Table.from_batches([], schema=empty_arrow))
            else:
                # Safe Path: Use PyIceberg to resolve deletes into Arrow
                arrow_table = scan.to_arrow()
                con.register(fv.name, arrow_table)

        # 4. Construct ASOF join query with feature name handling
        query = "SELECT entity_df.*"
        for fv in feature_views:
            # Add all features from the feature view to SELECT clause
            for feature in fv.features:
                feature_name = feature.name
                if full_feature_names:
                    feature_name = f"{fv.name}__{feature.name}"
                query += f", {fv.name}.{feature.name} AS {feature_name}"

        query += " FROM entity_df"
        for fv in feature_views:
            assert isinstance(fv.batch_source, IcebergSource)
            # DuckDB ASOF JOIN:
            # 1. Join keys match exactly.
            # 2. Timestamp condition (entity_timestamp >= feature_timestamp).
            # 3. TTL filtering ensures features are fresh (within time-to-live window).
            # 4. Picks the latest feature record for each entity record.
            query += f" ASOF LEFT JOIN {fv.name} ON "
            # Use 'entity_df.event_timestamp' which is standard in Feast universal tests
            join_conds = [f"entity_df.{k} = {fv.name}.{k}" for k in fv.join_keys]
            query += " AND ".join(join_conds)
            query += f" AND entity_df.event_timestamp >= {fv.name}.{fv.batch_source.timestamp_field}"

            # Add TTL filtering: feature must be within TTL window
            if fv.ttl and fv.ttl.total_seconds() > 0:
                ttl_seconds = fv.ttl.total_seconds()
                query += (
                    f" AND {fv.name}.{fv.batch_source.timestamp_field} >= "
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

        columns = join_key_columns + feature_name_columns + [timestamp_field]
        if created_timestamp_column:
            columns.append(created_timestamp_column)

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
        join_keys_str = ", ".join(join_key_columns)
        columns = join_key_columns + feature_name_columns + [timestamp_field]
        if created_timestamp_column:
            columns.append(created_timestamp_column)

        columns_str = ", ".join(columns)

        # Rank records by timestamp descending (with created_timestamp as tiebreaker) and pick rank 1
        order_by = f"{timestamp_field} DESC"
        if created_timestamp_column:
            order_by += f", {created_timestamp_column} DESC"

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

        # 1. Load Iceberg catalog
        catalog_props = {
            "type": config.offline_store.catalog_type,
            "uri": config.offline_store.uri,
            "warehouse": config.offline_store.warehouse,
            **config.offline_store.storage_options,
        }
        catalog_props = {k: v for k, v in catalog_props.items() if v is not None}
        catalog = load_catalog(config.offline_store.catalog_name, **catalog_props)

        # 2. Setup DuckDB and Load Table
        con = duckdb.connect(database=":memory:")
        _configure_duckdb_httpfs(con, config.offline_store.storage_options)
        table_id = data_source.table_identifier
        if not table_id:
            raise ValueError(f"Table identifier missing for source {data_source.name}")
        table = catalog.load_table(table_id)

        # Build row filter
        row_filters = []
        if start_date:
            start_date_naive = to_naive_utc(start_date)
            row_filters.append(f"{timestamp_field} >= '{start_date_naive.isoformat()}'")
        if end_date:
            end_date_naive = to_naive_utc(end_date)
            row_filters.append(f"{timestamp_field} <= '{end_date_naive.isoformat()}'")

        row_filter = " AND ".join(row_filters) if row_filters else None

        # Load filtered scan
        scan = table.scan(row_filter=row_filter) if row_filter else table.scan()

        # Use any() for memory-efficient MOR detection (avoids materializing all file metadata)
        has_deletes = any(task.delete_files for task in scan.plan_files())

        source_table = "source_table"
        if not has_deletes:
            # COW path: collect file paths and read Parquet directly in DuckDB
            file_paths = [task.file.file_path for task in scan.plan_files()]
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

        # Load catalog
        catalog_props = {
            "type": config.offline_store.catalog_type,
            "uri": config.offline_store.uri,
            "warehouse": config.offline_store.warehouse,
            **config.offline_store.storage_options,
        }
        catalog_props = {k: v for k, v in catalog_props.items() if v is not None}
        catalog = load_catalog(config.offline_store.catalog_name, **catalog_props)

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

        # Load catalog
        catalog_props = {
            "type": config.offline_store.catalog_type,
            "uri": config.offline_store.uri,
            "warehouse": config.offline_store.warehouse,
            **config.offline_store.storage_options,
        }
        catalog_props = {k: v for k, v in catalog_props.items() if v is not None}
        catalog = load_catalog(config.offline_store.catalog_name, **catalog_props)

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

        catalog_props = {
            "type": self._config.offline_store.catalog_type,
            "uri": self._config.offline_store.uri,
            "warehouse": self._config.offline_store.warehouse,
            **self._config.offline_store.storage_options,
        }
        catalog_props = {k: v for k, v in catalog_props.items() if v is not None}
        catalog = load_catalog(self._config.offline_store.catalog_name, **catalog_props)

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
