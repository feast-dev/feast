import os
import uuid
from typing import Dict, List
from venv import create

import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.mssql_offline_store.mssql import (
    MsSqlServerOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source import (
    MsSqlServerSource,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class MsqlDataSourceCreator(DataSourceCreator):
    mssql_offline_store_config: MsSqlServerOfflineStoreConfig

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        if not self.mssql_offline_store_config:
            self.create_offline_store_config()

    def create_offline_store_config(self) -> MsSqlServerOfflineStoreConfig:
        # TODO: Fill in connection string
        connection_string = os.getenv("AZURE_CONNECTION_STRING", "")
        self.mssql_offline_store_config = MsSqlServerOfflineStoreConfig()
        self.mssql_offline_store_config.connection_string = connection_string
        return self.mssql_offline_store_config

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        timestamp_field="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
        **kwargs,
    ) -> DataSource:
        if timestamp_field in df:
            df[timestamp_field] = pd.to_datetime(df[timestamp_field], utc=True)
        # Make sure the field mapping is correct and convert the datetime datasources.

        if field_mapping:
            timestamp_mapping = {value: key for key, value in field_mapping.items()}
            if (
                timestamp_field in timestamp_mapping
                and timestamp_mapping[timestamp_field] in df
            ):
                col = timestamp_mapping[timestamp_field]
                df[col] = pd.to_datetime(df[col], utc=True)
        # Upload dataframe to azure table
        destination_name = self.get_prefixed_table_name(destination_name)
        return MsSqlServerSource(
            connection_str=self.mssql_offline_store_config.connection_string,
            table_ref=destination_name,
            event_timestamp_column=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
        )

    def get_prefixed_table_name(self, destination_name: str) -> str:
        # TODO fix this
        return f"{self.project_name}_{destination_name}"

    def teardown(self):
        pass
