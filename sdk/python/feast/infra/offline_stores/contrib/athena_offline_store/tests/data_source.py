import os
import uuid
from typing import Dict, List, Optional

import pandas as pd

from feast import AthenaSource
from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.contrib.athena_offline_store.athena import (
    AthenaOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
    AthenaLoggingDestination,
    SavedDatasetAthenaStorage,
)
from feast.infra.utils import aws_utils
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class AthenaDataSourceCreator(DataSourceCreator):
    tables: List[str] = []

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)

        region = os.getenv("ATHENA_REGION", "ap-northeast-2")
        data_source = os.getenv("ATHENA_DATA_SOURCE", "AwsDataCatalog")
        database = os.getenv("ATHENA_DATABASE", "default")
        workgroup = os.getenv("ATHENA_WORKGROUP", "primary")
        bucket_name = os.getenv("ATHENA_S3_BUCKET_NAME", "feast-int-bucket")

        self.client = aws_utils.get_athena_data_client(region)
        self.s3 = aws_utils.get_s3_resource(region)
        self.offline_store_config = AthenaOfflineStoreConfig(
            data_source=data_source,
            region=region,
            database=database,
            workgroup=workgroup,
            s3_staging_location=f"s3://{bucket_name}/test_dir",
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        table_name = destination_name
        s3_target = (
            self.offline_store_config.s3_staging_location
            + "/"
            + self.project_name
            + "/"
            + table_name
            + "/"
            + table_name
            + ".parquet"
        )

        aws_utils.upload_df_to_athena(
            self.client,
            self.offline_store_config.data_source,
            self.offline_store_config.database,
            self.offline_store_config.workgroup,
            self.s3,
            s3_target,
            table_name,
            df,
        )

        self.tables.append(table_name)

        return AthenaSource(
            table=table_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
            database=self.offline_store_config.database,
            data_source=self.offline_store_config.data_source,
        )

    def create_saved_dataset_destination(self) -> SavedDatasetAthenaStorage:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return SavedDatasetAthenaStorage(
            table_ref=table,
            database=self.offline_store_config.database,
            data_source=self.offline_store_config.data_source,
        )

    def create_logged_features_destination(self) -> LoggingDestination:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return AthenaLoggingDestination(table_name=table)

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def teardown(self):
        for table in self.tables:
            aws_utils.execute_athena_query(
                self.client,
                self.offline_store_config.data_source,
                self.offline_store_config.database,
                self.offline_store_config.workgroup,
                f"DROP TABLE IF EXISTS {table}",
            )
