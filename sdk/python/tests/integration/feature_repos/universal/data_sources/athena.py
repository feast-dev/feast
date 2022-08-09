import os
import uuid
from typing import Any, Dict, List, Optional

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
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class AthenaDataSourceCreator(DataSourceCreator):

    tables: List[str] = []

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.client = aws_utils.get_athena_data_client("ap-northeast-2")
        self.s3 = aws_utils.get_s3_resource("ap-northeast-2")
        data_source = (
            os.environ.get("S3_DATA_SOURCE")
            if os.environ.get("S3_DATA_SOURCE")
            else "AwsDataCatalog"
        )
        database = (
            os.environ.get("S3_DATABASE")
            if os.environ.get("S3_DATABASE")
            else "sampledb"
        )
        bucket_name = (
            os.environ.get("S3_BUCKET_NAME")
            if os.environ.get("S3_BUCKET_NAME")
            else "feast-integration-tests"
        )
        self.offline_store_config = AthenaOfflineStoreConfig(
            data_source=f"{data_source}",
            region="ap-northeast-2",
            database=f"{database}",
            s3_staging_location=f"s3://{bucket_name}/test_dir",
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
                f"DROP TABLE IF EXISTS {table}",
            )


FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(),
    IntegrationTestRepoConfig(
        provider="aws",
        offline_store_creator=AthenaDataSourceCreator,
    ),
]

AVAILABLE_OFFLINE_STORES = [("aws", AthenaDataSourceCreator)]
