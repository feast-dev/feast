import uuid
from typing import Dict, List, Optional

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import Dataset

from feast import BigQuerySource
from feast.data_source import DataSource
from feast.infra.offline_stores.bigquery import BigQueryOfflineStoreConfig
from feast.infra.offline_stores.bigquery_source import SavedDatasetBigQueryStorage
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class BigQueryDataSourceCreator(DataSourceCreator):
    dataset: Optional[Dataset] = None

    def __init__(self, project_name: str):
        self.client = bigquery.Client()
        self.project_name = project_name
        self.gcp_project = self.client.project
        self.dataset_id = f"{self.gcp_project}.{project_name}"

        self.tables: List[str] = []

    def create_dataset(self):
        if not self.dataset:
            self.dataset = bigquery.Dataset(self.dataset_id)
            print(f"Creating dataset: {self.dataset_id}")
            self.client.create_dataset(self.dataset, exists_ok=True)
            self.dataset.default_table_expiration_ms = (
                1000 * 60 * 60 * 24 * 14
            )  # 2 weeks in milliseconds
            self.client.update_dataset(self.dataset, ["default_table_expiration_ms"])

    def teardown(self):

        for table in self.tables:
            self.client.delete_table(table, not_found_ok=True)

        self.client.delete_dataset(
            self.dataset_id, delete_contents=True, not_found_ok=True
        )
        print(f"Deleted dataset '{self.dataset_id}'")
        self.dataset = None

    def create_offline_store_config(self):
        return BigQueryOfflineStoreConfig()

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
        **kwargs,
    ) -> DataSource:

        destination_name = self.get_prefixed_table_name(destination_name)

        self.create_dataset()

        if self.gcp_project not in destination_name:
            destination_name = (
                f"{self.gcp_project}.{self.project_name}.{destination_name}"
            )

        job = self.client.load_table_from_dataframe(df, destination_name)
        job.result()

        self.tables.append(destination_name)

        return BigQuerySource(
            table_ref=destination_name,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetBigQueryStorage:
        table = self.get_prefixed_table_name(
            f"persisted_{str(uuid.uuid4()).replace('-', '_')}"
        )
        return SavedDatasetBigQueryStorage(table_ref=table)

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.client.project}.{self.project_name}.{suffix}"
