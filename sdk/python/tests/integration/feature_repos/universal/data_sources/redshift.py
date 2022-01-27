import uuid
from typing import Dict, List, Optional

import pandas as pd

from feast import RedshiftSource
from feast.data_source import DataSource
from feast.infra.offline_stores.redshift import RedshiftOfflineStoreConfig
from feast.infra.offline_stores.redshift_source import SavedDatasetRedshiftStorage
from feast.infra.utils import aws_utils
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class RedshiftDataSourceCreator(DataSourceCreator):

    tables: List[str] = []

    def __init__(self, project_name: str):
        super().__init__()
        self.project_name = project_name
        self.client = aws_utils.get_redshift_data_client("us-west-2")
        self.s3 = aws_utils.get_s3_resource("us-west-2")

        self.offline_store_config = RedshiftOfflineStoreConfig(
            cluster_id="feast-integration-tests",
            region="us-west-2",
            user="admin",
            database="feast",
            s3_staging_location="s3://feast-integration-tests/redshift/tests/ingestion",
            iam_role="arn:aws:iam::402087665549:role/redshift_s3_access_role",
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

        destination_name = self.get_prefixed_table_name(destination_name)

        aws_utils.upload_df_to_redshift(
            self.client,
            self.offline_store_config.cluster_id,
            self.offline_store_config.database,
            self.offline_store_config.user,
            self.s3,
            f"{self.offline_store_config.s3_staging_location}/copy/{destination_name}.parquet",
            self.offline_store_config.iam_role,
            destination_name,
            df,
        )

        self.tables.append(destination_name)

        return RedshiftSource(
            table=destination_name,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetRedshiftStorage:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return SavedDatasetRedshiftStorage(table_ref=table)

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def teardown(self):
        for table in self.tables:
            aws_utils.execute_redshift_statement(
                self.client,
                self.offline_store_config.cluster_id,
                self.offline_store_config.database,
                self.offline_store_config.user,
                f"DROP TABLE IF EXISTS {table}",
            )
