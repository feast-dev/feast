import time
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from pandas import DataFrame

from feast import BigQuerySource, FeatureView, driver_test_data
from feast.data_source import DataSource
from feast.infra.offline_stores.bigquery import BigQueryOfflineStoreConfig
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.feature_views import (
    create_customer_daily_profile_feature_view,
    create_driver_hourly_stats_feature_view,
)


class BigQueryDataSourceCreator(DataSourceCreator):
    def teardown(self, name: str):
        dataset_id = f"{self.client.project}.{name}"

        self.client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        print(f"Deleted dataset '{dataset_id}'")

    def __init__(self, project_name: str):
        self.client = bigquery.Client()
        self.project_name = project_name
        self.gcp_project = self.client.project
        dataset = bigquery.Dataset(f"{self.gcp_project}.{project_name}")
        self.client.create_dataset(dataset, exists_ok=True)
        dataset.default_table_expiration_ms = (
            1000 * 60 * 60 * 24 * 14
        )  # 2 weeks in milliseconds
        self.client.update_dataset(dataset, ["default_table_expiration_ms"])

    def create_offline_store_config(self):
        return BigQueryOfflineStoreConfig()

    def create_data_sources(
        self,
        destination: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        **kwargs,
    ) -> DataSource:

        job_config = bigquery.LoadJobConfig()
        if self.gcp_project not in destination:
            destination = f"{self.gcp_project}.{self.project_name}.{destination}"

        job = self.client.load_table_from_dataframe(
            df, destination, job_config=job_config
        )
        job.result()

        return BigQuerySource(
            table_ref=destination,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping={"ts_1": "ts", "id": "driver_id"},
        )

    def upload_df(self, df: DataFrame, destination: str):
        job_config = bigquery.LoadJobConfig()
        job = self.client.load_table_from_dataframe(
            df, destination, job_config=job_config
        )
        job.result()

    def create_entity_data(self, name: str) -> [FeatureView]:
        bigquery_dataset = name
        gcp_project = self.client.project

        # Create a DataSet in BQ to organize tables
        dataset = bigquery.Dataset(f"{self.client.project}.{name}")
        dataset.location = "US"
        self.dataset = self.client.create_dataset(dataset, exists_ok=True)

        start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        (
            customer_entities,
            driver_entities,
            end_date,
            orders_df,
            start_date,
        ) = self.generate_entities(start_date)

        # Upload entites data into table
        table_id = f"{bigquery_dataset}.orders"
        self.upload_df_to_table(orders_df, table_id)

        driver_df = driver_test_data.create_driver_hourly_stats_df(
            driver_entities, start_date, end_date
        )
        driver_table_id = f"{gcp_project}.{bigquery_dataset}.driver_hourly"
        self.upload_df_to_table(driver_df, driver_table_id)

        driver_source = BigQuerySource(
            table_ref=driver_table_id,
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created",
        )
        driver_fv = create_driver_hourly_stats_feature_view(driver_source)

        # Customer Feature View
        customer_df = driver_test_data.create_customer_daily_profile_df(
            customer_entities, start_date, end_date
        )
        customer_table_id = f"{gcp_project}.{bigquery_dataset}.customer_profile"

        self.upload_df_to_table(customer_df, customer_table_id)
        customer_source = BigQuerySource(
            table_ref=customer_table_id,
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created",
        )
        customer_fv = create_customer_daily_profile_feature_view(customer_source)

        return [driver_fv, customer_fv]

    def get_prefixed_table_name(self, name: str, suffix: str) -> str:
        return f"{self.client.project}.{name}.{suffix}"

    def upload_df_to_table(self, df, table_id):
        df.reset_index(drop=True, inplace=True)
        job_config = bigquery.LoadJobConfig()
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
