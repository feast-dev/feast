from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import duckdb

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pydantic import Field

from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
    IcebergSource,
)
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig


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

        # Register entity_df
        if isinstance(entity_df, pd.DataFrame):
            con.register("entity_df", entity_df)
        else:
            # Handle SQL string if provided
            con.execute(f"CREATE VIEW entity_df AS {entity_df}")

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

        # 4. Construct ASOF join query
        # We'll use a simplified version for now and expand as needed for Feast complexities
        feature_names_joined = ", ".join([f"{fv.name}.*" for fv in feature_views])

        # Simplified ASOF Join for one feature view to start.
        # Multi-FV join requires chaining ASOF joins or subqueries.
        query = "SELECT entity_df.*"
        for fv in feature_views:
            query += f", {fv.name}.*"

        query += " FROM entity_df"
        for fv in feature_views:
            # Note: entity_df must have the timestamp_field and entity keys
            # fv.batch_source has the timestamp_field and join_keys (entities)
            join_keys = fv.entities
            # This is a placeholder for a robust PIT join generation logic
            query += f" ASOF LEFT JOIN {fv.name} ON "
            join_conds = [f"entity_df.{k} = {fv.name}.{k}" for k in join_keys]
            query += " AND ".join(join_conds)
            query += f" AND entity_df.event_timestamp >= {fv.name}.{fv.batch_source.timestamp_field}"

        return IcebergRetrievalJob(con, query)

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: Any,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
            IcebergSource,
        )

        assert isinstance(data_source, IcebergSource)
        # Implementation for materialization
        # ...
        return IcebergRetrievalJob(duckdb.connect(), "")


class IcebergRetrievalJob(RetrievalJob):
    def __init__(self, con: duckdb.DuckDBPyConnection, query: str):
        self.con = con
        self.query = query

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self.con.execute(self.query).df()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        return self.con.execute(self.query).arrow()

    @property
    def full_feature_names(self) -> bool:
        return False

    @property
    def on_demand_feature_views(self) -> List["OnDemandFeatureView"]:
        return []
