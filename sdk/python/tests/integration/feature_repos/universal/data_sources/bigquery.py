import os
import uuid
from typing import Dict, List, Optional

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import Dataset

from feast import BigQuerySource
from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.bigquery import BigQueryOfflineStoreConfig
from feast.infra.offline_stores.bigquery_source import (
    BigQueryLoggingDestination,
    SavedDatasetBigQueryStorage,
)
from feast.utils import make_df_tzaware
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class BigQueryDataSourceCreator(DataSourceCreator):
    dataset: Optional[Dataset] = None

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.client = bigquery.Client()
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
        return BigQueryOfflineStoreConfig(
            location=os.getenv("GCS_REGION", "US"),
            gcs_staging_location=os.getenv(
                "GCS_STAGING_LOCATION", "gs://feast-export/"
            ),
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        timestamp_field="ts",
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

        # Make all datetime columns timezone aware. This should be the behaviour of
        # `BigQueryOfflineStore.offline_write_batch`, but since we're bypassing that API here, we should follow the same
        # rule. The schema of this initial dataframe determines the schema in the newly created BigQuery table.
        df = make_df_tzaware(df)
        job = self.client.load_table_from_dataframe(df, destination_name)
        job.result()

        self.tables.append(destination_name)

        return BigQuerySource(
            table=destination_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetBigQueryStorage:
        table = self.get_prefixed_table_name(
            f"persisted_{str(uuid.uuid4()).replace('-', '_')}"
        )
        return SavedDatasetBigQueryStorage(table=table)

    def create_logged_features_destination(self) -> LoggingDestination:
        table = self.get_prefixed_table_name(
            f"logged_features_{str(uuid.uuid4()).replace('-', '_')}"
        )
        return BigQueryLoggingDestination(table_ref=table)

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.client.project}.{self.project_name}.{suffix}"
