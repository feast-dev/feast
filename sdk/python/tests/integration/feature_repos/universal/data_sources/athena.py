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

    def __init__(self, project_name: str):
        super().__init__(project_name)
        self.client = aws_utils.get_athena_data_client("ap-northeast-2")
        self.s3 = aws_utils.get_s3_resource("ap-northeast-2")

        self.offline_store_config = AthenaOfflineStoreConfig(
            data_source="AwsDataCatalog",
            region="ap-northeast-2",
            database="sampledb",
            s3_staging_location="s3://sagemaker-yelo-test",
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

        aws_utils.upload_df_to_athena(
            self.client,
            self.offline_store_config.data_source,
            self.offline_store_config.database,
            self.s3,
            self.offline_store_config.s3_staging_location,
            destination_name,
            df,
        )

        self.tables.append(destination_name)

        return AthenaSource(
            table=destination_name,
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

        return SavedDatasetAthenaStorage(table_ref=table)

    def create_logged_features_destination(self) -> LoggingDestination:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return AthenaLoggingDestination(table_name=table)

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}.{suffix}"

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
        provider="aws", offline_store_creator=AthenaDataSourceCreator,
    ),
]