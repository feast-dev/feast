import random
import time
from typing import Optional

import pandas as pd

from feast import RedshiftSource
from feast.data_source import DataSource
from feast.infra.offline_stores.redshift import RedshiftOfflineStoreConfig
from feast.infra.utils import aws_utils
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class RedshiftDataSourceCreator(DataSourceCreator):

    table_name: Optional[str] = None
    redshift_source: Optional[RedshiftSource] = None

    def __init__(self) -> None:
        super().__init__()
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
        name: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
    ) -> DataSource:
        self.table_name = f"{name}_{time.time_ns()}_{random.randint(1000, 9999)}"
        aws_utils.upload_df_to_redshift(
            self.client,
            self.offline_store_config.cluster_id,
            self.offline_store_config.database,
            self.offline_store_config.user,
            self.s3,
            f"{self.offline_store_config.s3_staging_location}/copy/{self.table_name}.parquet",
            self.offline_store_config.iam_role,
            self.table_name,
            df,
        )

        self.redshift_source = RedshiftSource(
            table=self.table_name,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping={"ts_1": "ts", "id": "driver_id"},
        )
        return self.redshift_source

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def teardown(self, name: str):
        if self.table_name:
            aws_utils.execute_redshift_statement(
                self.client,
                self.offline_store_config.cluster_id,
                self.offline_store_config.database,
                self.offline_store_config.user,
                f"DROP TABLE {self.table_name}",
            )
