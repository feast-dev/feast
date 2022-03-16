# TODO: Do we want to keep these tests? Have not been re-validated with v0.19

from typing import Dict, List

import pandas as pd

from feast.data_source import DataSource
from feast.repo_config import FeastConfigBaseModel
from feast_trino.connectors.upload import upload_pandas_dataframe_to_trino
from feast_trino.trino import TrinoOfflineStoreConfig
from feast_trino.trino_source import TrinoSource
from feast_trino.trino_utils import Trino
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class TrinoSourceCreator(DataSourceCreator):
    def __init__(self, project_name: str):
        self.project_name = project_name
        self.client = Trino(user="user", catalog="memory", host="localhost", port=8080,)
        self.tables_created: List[str] = []

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:
        table_ref = self.get_prefixed_table_name(destination_name)
        self.client.execute_query(
            f"CREATE SCHEMA IF NOT EXISTS memory.{self.project_name}"
        )
        self.client.execute_query(f"DROP TABLE IF EXISTS {table_ref}")

        upload_pandas_dataframe_to_trino(
            client=self.client,
            df=df,
            table_ref=table_ref,
            connector_args={"type": "memory"},
        )
        self.tables_created.append(table_ref)

        return TrinoSource(
            table_ref=table_ref,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            query=f"SELECT * FROM {table_ref}",
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"memory.{self.project_name}.{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return TrinoOfflineStoreConfig(
            host="localhost",
            port=8080,
            catalog="memory",
            dataset=self.project_name,
            connector={"type": "memory"},
        )

    def teardown(self):
        for table in self.tables_created:
            self.client.execute_query(f"DROP TABLE IF EXISTS {table}")


FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(),
    IntegrationTestRepoConfig(
        provider="local", offline_store_creator=TrinoSourceCreator,
    ),
]
