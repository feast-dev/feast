import os
import uuid
from typing import Dict, List, Optional

import pandas as pd

from feast import SnowflakeSource
from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.snowflake import SnowflakeOfflineStoreConfig
from feast.infra.offline_stores.snowflake_source import (
    SavedDatasetSnowflakeStorage,
    SnowflakeLoggingDestination,
)
from feast.infra.utils.snowflake.snowflake_utils import (
    GetSnowflakeConnection,
    execute_snowflake_statement,
    write_pandas,
)
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class SnowflakeDataSourceCreator(DataSourceCreator):

    tables: List[str] = []

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.offline_store_config = SnowflakeOfflineStoreConfig(
            type="snowflake.offline",
            account=os.environ["SNOWFLAKE_CI_DEPLOYMENT"],
            user=os.environ["SNOWFLAKE_CI_USER"],
            password=os.environ["SNOWFLAKE_CI_PASSWORD"],
            role=os.environ["SNOWFLAKE_CI_ROLE"],
            warehouse=os.environ["SNOWFLAKE_CI_WAREHOUSE"],
            database="FEAST",
            schema="OFFLINE",
            storage_integration_name=os.getenv("BLOB_EXPORT_STORAGE_NAME", "FEAST_S3"),
            blob_export_location=os.getenv(
                "BLOB_EXPORT_URI", "s3://feast-snowflake-offload/export"
            ),
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        suffix: Optional[str] = None,
        timestamp_field="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:

        destination_name = self.get_prefixed_table_name(destination_name)

        with GetSnowflakeConnection(self.offline_store_config) as conn:
            write_pandas(conn, df, destination_name, auto_create_table=True)

        self.tables.append(destination_name)

        return SnowflakeSource(
            table=destination_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetSnowflakeStorage:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return SavedDatasetSnowflakeStorage(table_ref=table)

    def create_logged_features_destination(self) -> LoggingDestination:
        table = self.get_prefixed_table_name(
            f"logged_features_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return SnowflakeLoggingDestination(table_name=table)

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def teardown(self):
        with GetSnowflakeConnection(self.offline_store_config) as conn:
            for table in self.tables:
                query = f'DROP TABLE IF EXISTS "{table}"'
                execute_snowflake_statement(conn, query)
