from datetime import timedelta
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
from feast.infra.compute_engines.spark.compute import SparkComputeEngine
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
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


def create_base_feature_view(source):
    return BatchFeatureView(
        name="hourly_driver_stats",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=True,
        source=source,
    )


def create_agg_feature_view(source):
    return BatchFeatureView(
        name="agg_hourly_driver_stats",
        entities=[driver],
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=True,
        source=source,
        aggregations=[
            Aggregation(column="conv_rate", function="sum"),
            Aggregation(column="acc_rate", function="avg"),
        ],
    )


def create_chained_feature_view(base_fv: BatchFeatureView):
    def transform_feature(df: DataFrame) -> DataFrame:
        df = df.withColumn("conv_rate", df["conv_rate"] * 2)
        df = df.withColumn("acc_rate", df["acc_rate"] * 2)
        return df

    return BatchFeatureView(
        name="daily_driver_stats",
        entities=[driver],
        udf=transform_feature,
        udf_string="transform",
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=True,
        source=base_fv,
        sink_source=SparkSource(
            name="daily_driver_stats_sink",
            path="/tmp/daily_driver_stats_sink",
            file_format="parquet",
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        ),
    )


@pytest.mark.integration
def test_spark_dag_materialize_recursive_view():
    spark_env = create_spark_environment()
    fs = spark_env.feature_store
    registry = fs.registry
    source = create_feature_dataset(spark_env)

    base_fv = create_base_feature_view(source)
    chained_fv = create_chained_feature_view(base_fv)

    def tqdm_builder(length):
        return tqdm(total=length, ncols=100)

    try:
        fs.apply([driver, base_fv, chained_fv])

        # 🧪 Materialize top-level view; DAG will include base_fv implicitly
        task = MaterializationTask(
            project=fs.project,
            feature_view=chained_fv,
            start_time=now - timedelta(days=2),
            end_time=now,
            tqdm_builder=tqdm_builder,
        )

        engine = SparkComputeEngine(
            repo_config=spark_env.config,
            offline_store=SparkOfflineStore(),
            online_store=MagicMock(),
            registry=registry,
        )

        jobs = engine.materialize(registry, task)

        # ✅ Validate jobs ran
        assert len(jobs) == 1
        assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED

        _check_online_features(
            fs=fs,
            driver_id=1001,
            feature="daily_driver_stats:conv_rate",
            expected_value=1.6,
            full_feature_names=True,
        )

        entity_df = create_entity_df()

        _check_offline_features(
            fs=fs, feature="hourly_driver_stats:conv_rate", entity_df=entity_df, size=2
        )
    finally:
        spark_env.teardown()


@pytest.mark.integration
def test_spark_dag_materialize_multi_views():
    spark_env = create_spark_environment()
    fs = spark_env.feature_store
    registry = fs.registry
    source = create_feature_dataset(spark_env)

    base_fv = create_base_feature_view(source)
    chained_fv = create_chained_feature_view(base_fv)

    multi_view = BatchFeatureView(
        name="multi_view",
        entities=[driver],
        schema=[
            Field(name="driver_id", dtype=Int32),
            Field(name="daily_driver_stats__conv_rate", dtype=Float32),
            Field(name="daily_driver_stats__acc_rate", dtype=Float32),
        ],
        online=True,
        offline=True,
        source=[base_fv, chained_fv],
        sink_source=SparkSource(
            name="multi_view_sink",
            path="/tmp/multi_view_sink",
            file_format="parquet",
            timestamp_field="daily_driver_stats__event_timestamp",
            created_timestamp_column="daily_driver_stats__created",
        ),
    )

    def tqdm_builder(length):
        return tqdm(total=length, ncols=100)

    try:
        fs.apply([driver, base_fv, chained_fv, multi_view])

        # 🧪 Materialize multi-view
        task = MaterializationTask(
            project=fs.project,
            feature_view=multi_view,
            start_time=now - timedelta(days=2),
            end_time=now,
            tqdm_builder=tqdm_builder,
        )

        engine = SparkComputeEngine(
            repo_config=spark_env.config,
            offline_store=SparkOfflineStore(),
            online_store=MagicMock(),
            registry=registry,
        )

        jobs = engine.materialize(registry, task)

        # ✅ Validate jobs ran
        assert len(jobs) == 1
        assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED

        _check_online_features(
            fs=fs,
            driver_id=1001,
            feature="multi_view:daily_driver_stats__conv_rate",
            expected_value=1.6,
            full_feature_names=True,
        )

        entity_df = create_entity_df()

        _check_offline_features(
            fs=fs, feature="hourly_driver_stats:conv_rate", entity_df=entity_df, size=2
        )
    finally:
        spark_env.teardown()
