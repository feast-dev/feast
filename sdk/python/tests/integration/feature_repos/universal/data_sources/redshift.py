import os
import uuid
from typing import Dict, List, Optional

import pandas as pd

from feast import RedshiftSource
from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.redshift import RedshiftOfflineStoreConfig
from feast.infra.offline_stores.redshift_source import (
    RedshiftLoggingDestination,
    SavedDatasetRedshiftStorage,
)
from feast.infra.utils import aws_utils
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class RedshiftDataSourceCreator(DataSourceCreator):

    tables: List[str] = []

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.client = aws_utils.get_redshift_data_client(
            os.getenv("AWS_REGION", "us-west-2")
        )
        self.s3 = aws_utils.get_s3_resource(os.getenv("AWS_REGION", "us-west-2"))

        self.offline_store_config = RedshiftOfflineStoreConfig(
            cluster_id=os.getenv("AWS_CLUSTER_ID", "feast-integration-tests"),
            region=os.getenv("AWS_REGION", "us-west-2"),
            user=os.getenv("AWS_USER", "admin"),
            database=os.getenv("AWS_DB", "feast"),
            s3_staging_location=os.getenv(
                "AWS_STAGING_LOCATION",
                "s3://feast-integration-tests/redshift/tests/ingestion",
            ),
            iam_role=os.getenv(
                "AWS_IAM_ROLE", "arn:aws:iam::402087665549:role/redshift_s3_access_role"
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

        aws_utils.upload_df_to_redshift(
            self.client,
            self.offline_store_config.cluster_id,
            self.offline_store_config.workgroup,
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
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
            database=self.offline_store_config.database,
        )

    def create_saved_dataset_destination(self) -> SavedDatasetRedshiftStorage:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return SavedDatasetRedshiftStorage(table_ref=table)

    def create_logged_features_destination(self) -> LoggingDestination:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return RedshiftLoggingDestination(table_name=table)

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def teardown(self):
        for table in self.tables:
            aws_utils.execute_redshift_statement(
                self.client,
                self.offline_store_config.cluster_id,
                self.offline_store_config.workgroup,
                self.offline_store_config.database,
                self.offline_store_config.user,
                f"DROP TABLE IF EXISTS {table}",
            )
