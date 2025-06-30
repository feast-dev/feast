import pandas as pd
import pytest
from datetime import datetime, timedelta
from typing import cast
from unittest.mock import MagicMock
from pyspark.sql import DataFrame

from feast import BatchFeatureView, Entity, Field
from feast.types import Float32, Int64, Int32
from feast.aggregation import Aggregation
from feast.infra.common.materialization_job import MaterializationJobStatus, MaterializationTask
from feast.infra.compute_engines.spark.compute import SparkComputeEngine
from feast.infra.offline_stores.contrib.spark_offline_store.spark import SparkOfflineStore
from feast.infra.offline_stores.contrib.spark_offline_store.tests.data_source import SparkDataSourceCreator
from tests.integration.feature_repos.repo_configuration import construct_test_environment
from tests.integration.feature_repos.integration_test_repo_config import IntegrationTestRepoConfig
from tests.integration.feature_repos.universal.online_store.redis import RedisOnlineStoreCreator
from tqdm import tqdm


now = datetime.now()
today = datetime.today()

driver = Entity(
    name="driver_id",
    description="driver id",
)


@pytest.fixture(scope="module")
def spark_env():
    config = IntegrationTestRepoConfig(
        provider="local",
        online_store_creator=RedisOnlineStoreCreator,
        offline_store_creator=SparkDataSourceCreator,
        batch_engine={"type": "spark.engine", "partitions": 10},
    )
    env = construct_test_environment(config, None, entity_key_serialization_version=2)
    env.setup()
    yield env
    env.teardown()


def create_sample_datasource(spark_environment):
    df = pd.DataFrame([
        {
            "driver_id": 1001,
            "event_timestamp": today - timedelta(days=1),
            "created": now - timedelta(hours=2),
            "conv_rate": 0.8,
            "acc_rate": 0.5,
            "avg_daily_trips": 15,
        },
        {
            "driver_id": 1002,
            "event_timestamp": today - timedelta(days=1),
            "created": now - timedelta(hours=2),
            "conv_rate": 0.7,
            "acc_rate": 0.4,
            "avg_daily_trips": 12,
        },
    ])
    ds = spark_environment.data_source_creator.create_data_source(
        df,
        spark_environment.feature_store.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    return ds


def create_base_feature_view(source):
    return BatchFeatureView(
        name="hourly_driver_stats",
        entities=[driver],
        aggregations=[
            Aggregation(column="conv_rate", function="sum"),
            Aggregation(column="acc_rate", function="avg"),
        ],
        ttl=timedelta(days=3),
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


def create_chained_feature_view(base_fv: BatchFeatureView):
    def transform(df: DataFrame) -> DataFrame:
        return df.withColumn("sum_conv_rate", df["sum_conv_rate"] * 10)

    return BatchFeatureView(
        name="daily_driver_stats",
        entities=[driver],
        udf=transform,
        udf_string="transform",
        schema=[
            Field(name="sum_conv_rate", dtype=Float32),
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=True,
        source_view=base_fv,
        tags={
            "join_keys": "driver_id",
            "feature_cols": "sum_conv_rate",
            "ts_col": "event_timestamp",
            "created_ts_col": "created",
        }
    )


def _tqdm_builder(length):
    return tqdm(total=length, ncols=100)


@pytest.mark.integration
def test_spark_dag_materialize_recursive_view(spark_env):
    fs = spark_env.feature_store
    registry = fs.registry
    source = create_sample_datasource(spark_env)

    base_fv = create_base_feature_view(source)
    chained_fv = create_chained_feature_view(base_fv)

    fs.apply([driver, base_fv, chained_fv])

    # 🧪 Materialize top-level view; DAG will include base_fv implicitly
    task = MaterializationTask(
        project=fs.project,
        feature_view=chained_fv,
        start_time=now - timedelta(days=2),
        end_time=now,
        tqdm_builder=_tqdm_builder,
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

    # ✅ Verify output exists in offline store
    df = jobs[0].to_df()
    assert "sum_conv_rate" in df.columns
    assert sorted(df["driver_id"].tolist()) == [1001, 1002]
    assert abs(df["sum_conv_rate"].iloc[0] - 16.0) < 1e-6  # (0.8 + 0.8) * 10
