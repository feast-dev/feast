from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame
from tqdm import tqdm

from feast import BatchFeatureView, Field
from feast.aggregation import Aggregation
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
from feast.types import Float32, Int32, Int64
from tests.integration.compute_engines.spark.utils import (
    _check_offline_features,
    _check_online_features,
    create_entity_df,
    create_feature_dataset,
    create_spark_environment,
    driver,
    now,
)


@pytest.mark.integration
def test_spark_compute_engine_get_historical_features():
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
        )

        # 🧪 Run SparkComputeEngine
        engine = SparkComputeEngine(
            repo_config=spark_environment.config,
            offline_store=SparkOfflineStore(),
            online_store=MagicMock(),
        )

        spark_dag_retrieval_job = engine.get_historical_features(registry, task)
        spark_df = cast(SparkDAGRetrievalJob, spark_dag_retrieval_job).to_spark_df()
        df_out = spark_df.orderBy("driver_id").toPandas()

        # ✅ Assert output
        assert df_out.driver_id.to_list() == [1001, 1002]
        assert abs(df_out["sum_conv_rate"].to_list()[0] - 3.1) < 1e-6
        assert abs(df_out["sum_conv_rate"].to_list()[1] - 2.0) < 1e-6
        assert abs(df_out["avg_acc_rate"].to_list()[0] - 1.4) < 1e-6
        assert abs(df_out["avg_acc_rate"].to_list()[1] - 1.0) < 1e-6

    finally:
        spark_environment.teardown()


@pytest.mark.integration
def test_spark_compute_engine_materialize():
    """
    Test the SparkComputeEngine materialize method.
    For the current feature view driver_hourly_stats, The below execution plan:
    1. feature data from create_feature_dataset
    2. filter by start_time and end_time, that is, the last 2 days
        for the driver_id 1001, the data left is row 0
        for the driver_id 1002, the data left is row 2
    3. apply the transform_feature function to the data
        for all features, the value is multiplied by 2
    4. write the data to the online store and offline store

    Returns:

    """
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
        offline=True,
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
            start_time=now - timedelta(days=2),
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

        spark_materialize_jobs = engine.materialize(registry, task)

        assert len(spark_materialize_jobs) == 1

        assert spark_materialize_jobs[0].status() == MaterializationJobStatus.SUCCEEDED

        _check_online_features(
            fs=fs,
            driver_id=1001,
            feature="driver_hourly_stats:conv_rate",
            expected_value=1.6,
            full_feature_names=True,
        )

        entity_df = create_entity_df()

        _check_offline_features(
            fs=fs,
            feature="driver_hourly_stats:conv_rate",
            entity_df=entity_df,
        )
    finally:
        spark_environment.teardown()


if __name__ == "__main__":
    test_spark_compute_engine_get_historical_features()
