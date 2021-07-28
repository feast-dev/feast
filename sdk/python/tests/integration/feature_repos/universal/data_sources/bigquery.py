import time

import pandas as pd
from google.cloud import bigquery

from feast import BigQuerySource
from feast.data_source import DataSource
from feast.infra.offline_stores.bigquery import BigQueryOfflineStoreConfig
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class BigQueryDataSourceCreator(DataSourceCreator):
    def teardown(self, name: str):
        pass

    def __init__(self):
        self.client = bigquery.Client()

    def create_offline_store_config(self):
        return BigQueryOfflineStoreConfig()

    def create_data_source(
        self,
        name: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        **kwargs,
    ) -> DataSource:
        gcp_project = self.client.project
        bigquery_dataset = "test_ingestion"
        dataset = bigquery.Dataset(f"{gcp_project}.{bigquery_dataset}")
        self.client.create_dataset(dataset, exists_ok=True)
        dataset.default_table_expiration_ms = (
            1000 * 60 * 60 * 24 * 14
        )  # 2 weeks in milliseconds
        self.client.update_dataset(dataset, ["default_table_expiration_ms"])

        job_config = bigquery.LoadJobConfig()
        table_ref = f"{gcp_project}.{bigquery_dataset}.{name}_{int(time.time_ns())}"
        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()

        return BigQuerySource(
            table_ref=table_ref,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping={"ts_1": "ts", "id": "driver_id"},
        )
