import uuid
from typing import Dict, List

import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SavedDatasetSparkStorage,
    SparkSource,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class SparkDataSourceCreator(DataSourceCreator):
    tables: List[str] = []
    spark_offline_store_config = None
    spark_session = None

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.spark_conf = {
            "master": "local[*]",
            "spark.ui.enabled": "false",
            "spark.eventLog.enabled": "false",
            "spark.sql.parser.quotedRegexColumnNames": "true",
            "spark.sql.session.timeZone": "UTC",
        }
        if not self.spark_offline_store_config:
            self.create_offline_store_config()
        if not self.spark_session:
            self.spark_session = (
                SparkSession.builder.config(
                    conf=SparkConf().setAll(
                        [(k, v) for k, v in self.spark_conf.items()]
                    )
                )
                .appName("pytest-pyspark-local-testing")
                .getOrCreate()
            )
        self.tables: List[str] = []

    def teardown(self):
        self.spark_session.stop()

    def create_offline_store_config(self):
        self.spark_offline_store_config = SparkOfflineStoreConfig()
        self.spark_offline_store_config.type = "spark"
        self.spark_offline_store_config.spark_conf = self.spark_conf
        return self.spark_offline_store_config

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
        destination_name = self.get_prefixed_table_name(destination_name)
        if not self.spark_session:
            self.spark_session = (
                SparkSession.builder.config(
                    conf=SparkConf().setAll(
                        [(k, v) for k, v in self.spark_conf.items()]
                    )
                )
                .appName("pytest-pyspark-local-testing")
                .getOrCreate()
            )
        self.spark_session.createDataFrame(df).createOrReplaceTempView(destination_name)
        self.tables.append(destination_name)

        return SparkSource(
            table=destination_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetSparkStorage:
        table = f"persisted_{str(uuid.uuid4()).replace('-', '_')}"
        return SavedDatasetSparkStorage(
            table=table, query=None, path=None, file_format=None
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"
