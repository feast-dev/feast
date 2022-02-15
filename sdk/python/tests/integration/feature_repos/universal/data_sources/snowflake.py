import os
import uuid
from typing import Dict, List, Optional

import pandas as pd

from feast import SnowflakeSource
from feast.data_source import DataSource
from feast.infra.offline_stores.snowflake import SnowflakeOfflineStoreConfig
from feast.infra.offline_stores.snowflake_source import SavedDatasetSnowflakeStorage
from feast.infra.utils.snowflake_utils import get_snowflake_conn, write_pandas
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class SnowflakeDataSourceCreator(DataSourceCreator):

    tables: List[str] = []

    def __init__(self, project_name: str):
        super().__init__()
        self.project_name = project_name
        self.offline_store_config = SnowflakeOfflineStoreConfig(
            type="snowflake.offline",
            account=os.environ["SNOWFLAKE_CI_DEPLOYMENT"],
            user=os.environ["SNOWFLAKE_CI_USER"],
            password=os.environ["SNOWFLAKE_CI_PASSWORD"],
            role=os.environ["SNOWFLAKE_CI_ROLE"],
            warehouse=os.environ["SNOWFLAKE_CI_WAREHOUSE"],
            database="FEAST",
            schema="OFFLINE",
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        suffix: Optional[str] = None,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:

        snowflake_conn = get_snowflake_conn(self.offline_store_config)

        destination_name = self.get_prefixed_table_name(destination_name)

        write_pandas(snowflake_conn, df, destination_name, auto_create_table=True)

        self.tables.append(destination_name)

        return SnowflakeSource(
            table=destination_name,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetSnowflakeStorage:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return SavedDatasetSnowflakeStorage(table_ref=table)

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def teardown(self):
        snowflake_conn = get_snowflake_conn(self.offline_store_config)

        with snowflake_conn as conn:
            cur = conn.cursor()
            for table in self.tables:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
