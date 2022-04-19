from typing import Dict, List, Optional

import pandas as pd

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.postgres import (
    PostgreSQLOfflineStoreConfig,
    PostgreSQLSource,
)
from feast.infra.utils.postgres.utils import _get_conn, df_to_postgres_table
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class PostgreSQLDataSourceCreator(DataSourceCreator):
    tables: List[str] = []

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.project_name = project_name

        self.offline_store_config = PostgreSQLOfflineStoreConfig(
            type="postgres",
            host="localhost",
            port=5432,
            database="postgres",
            db_schema="public",
            user="postgres",
            password="docker",
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

        df_to_postgres_table(self.offline_store_config, df, destination_name)

        self.tables.append(destination_name)

        return PostgreSQLSource(
            name=destination_name,
            query=f"SELECT * FROM {destination_name}",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def create_saved_dataset_destination(self):
        # FIXME: ...
        return None

    def teardown(self):
        with _get_conn(self.offline_store_config) as conn, conn.cursor() as cur:
            for table in self.tables:
                cur.execute("DROP TABLE IF EXISTS " + table)
