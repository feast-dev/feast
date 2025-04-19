from datetime import datetime, timedelta
from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pyspark.sql import DataFrame
from tqdm import tqdm

from feast import BatchFeatureView, Entity, Field
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.spark.compute import SparkComputeEngine
from feast.infra.compute_engines.spark.job import SparkDAGRetrievalJob
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
)
from feast.infra.offline_stores.contrib.spark_offline_store.tests.data_source import (
    SparkDataSourceCreator,
)
from feast.types import Float32, Int32, Int64
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)

now = datetime.now()
today = datetime.today()

driver = Entity(
    name="driver_id",
    description="driver id",
)


def create_feature_dataset(spark_environment) -> DataSource:
    yesterday = today - timedelta(days=1)
    last_week = today - timedelta(days=7)
    df = pd.DataFrame(
        [
            {
                "driver_id": 1001,
                "event_timestamp": yesterday,
                "created": now - timedelta(hours=2),
                "conv_rate": 0.8,
                "acc_rate": 0.5,
                "avg_daily_trips": 15,
            },
            {
                "driver_id": 1001,
                "event_timestamp": last_week,
                "created": now - timedelta(hours=3),
                "conv_rate": 0.75,
                "acc_rate": 0.9,
                "avg_daily_trips": 14,
            },
            {
                "driver_id": 1002,
                "event_timestamp": yesterday,
                "created": now - timedelta(hours=2),
                "conv_rate": 0.7,
                "acc_rate": 0.4,
                "avg_daily_trips": 12,
            },
            {
                "driver_id": 1002,
                "event_timestamp": yesterday - timedelta(days=1),
                "created": now - timedelta(hours=2),
                "conv_rate": 0.3,
                "acc_rate": 0.6,
                "avg_daily_trips": 12,
            },
        ]
    )
    ds = spark_environment.data_source_creator.create_data_source(
        df,
        spark_environment.feature_store.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    return ds


def create_entity_df() -> pd.DataFrame:
    entity_df = pd.DataFrame(
        [
            {"driver_id": 1001, "event_timestamp": today},
            {"driver_id": 1002, "event_timestamp": today},
        ]
    )
    return entity_df


def create_spark_environment():
    spark_config = IntegrationTestRepoConfig(
        provider="local",
        online_store_creator=RedisOnlineStoreCreator,
        offline_store_creator=SparkDataSourceCreator,
        batch_engine={"type": "spark.engine", "partitions": 10},
    )
    spark_environment = construct_test_environment(
        spark_config, None, entity_key_serialization_version=2
    )
    spark_environment.setup()
    return spark_environment


@pytest.mark.integration
def test_spark_compute_engine_get_historical_features():
    spark_environment = create_spark_environment()
    fs = spark_environment.feature_store
    registry = fs.registry
    data_source = create_feature_dataset(spark_environment)

    def transform_feature(df: DataFrame) -> DataFrame:
        df = df.withColumn("sum_conv_rate", df["sum_conv_rate"] * 2)
        df = df.withColumn("avg_acc_rate", df["avg_acc_rate"] * 2)
        return df

    driver_stats_fv = BatchFeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        mode="python",
        aggregations=[
            Aggregation(column="conv_rate", function="sum"),
            Aggregation(column="acc_rate", function="avg"),
        ],
        udf=transform_feature,
        udf_string="transform_feature",
        ttl=timedelta(days=3),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
            Field(name="driver_id", dtype=Int32),
        ],
        online=False,
        offline=False,
        source=data_source,
    )

    entity_df = create_entity_df()

    try:
        fs.apply([driver, driver_stats_fv])

        # 🛠 Build retrieval task
        task = HistoricalRetrievalTask(
            project=spark_environment.project,
            entity_df=entity_df,
            feature_view=driver_stats_fv,
            full_feature_name=False,
            registry=registry,
            start_time=now - timedelta(days=1),
            end_time=now,
        )

        # 🧪 Run SparkComputeEngine
        engine = SparkComputeEngine(
            repo_config=spark_environment.config,
            offline_store=SparkOfflineStore(),
            online_store=MagicMock(),
            registry=registry,
        )

        spark_dag_retrieval_job = engine.get_historical_features(task)
        spark_df = cast(SparkDAGRetrievalJob, spark_dag_retrieval_job).to_spark_df()
        df_out = spark_df.orderBy("driver_id").to_pandas_on_spark()

        # ✅ Assert output
        assert df_out.driver_id.to_list() == [1001, 1002]
        assert abs(df_out["sum_conv_rate"].to_list()[0] - 1.6) < 1e-6
        assert abs(df_out["sum_conv_rate"].to_list()[1] - 2.0) < 1e-6
        assert abs(df_out["avg_acc_rate"].to_list()[0] - 1.0) < 1e-6
        assert abs(df_out["avg_acc_rate"].to_list()[1] - 1.0) < 1e-6

    finally:
        spark_environment.teardown()


@pytest.mark.integration
def test_spark_compute_engine_materialize():
    spark_environment = create_spark_environment()
    fs = spark_environment.feature_store
    registry = fs.registry

    data_source = create_feature_dataset(spark_environment)

    def transform_feature(df: DataFrame) -> DataFrame:
        df = df.withColumn("conv_rate", df["conv_rate"] * 2)
        df = df.withColumn("acc_rate", df["acc_rate"] * 2)
        return df

    driver_stats_fv = BatchFeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        mode="python",
        udf=transform_feature,
        udf_string="transform_feature",
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=False,
        source=data_source,
    )

    def tqdm_builder(length):
        return tqdm(total=length, ncols=100)

    try:
        fs.apply([driver, driver_stats_fv])

        # 🛠 Build retrieval task
        task = MaterializationTask(
            project=spark_environment.project,
            feature_view=driver_stats_fv,
            start_time=now - timedelta(days=1),
            end_time=now,
            tqdm_builder=tqdm_builder,
        )

        # 🧪 Run SparkComputeEngine
        engine = SparkComputeEngine(
            repo_config=spark_environment.config,
            offline_store=SparkOfflineStore(),
            online_store=MagicMock(),
            registry=registry,
        )

        spark_materialize_job = engine.materialize(task)

        assert spark_materialize_job.status() == MaterializationJobStatus.SUCCEEDED
    finally:
        spark_environment.teardown()


if __name__ == "__main__":
    test_spark_compute_engine_get_historical_features()
