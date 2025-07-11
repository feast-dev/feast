from datetime import datetime, timedelta
from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest
import ray
from tqdm import tqdm

from feast import BatchFeatureView, Entity, Field
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.ray.compute import RayComputeEngine
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.job import RayDAGRetrievalJob
from feast.infra.offline_stores.contrib.ray_offline_store.ray import (
    RayOfflineStore,
)
from feast.infra.offline_stores.contrib.ray_repo_configuration import (
    RayDataSourceCreator,
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


def create_feature_dataset(ray_environment) -> DataSource:
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
    ds = ray_environment.data_source_creator.create_data_source(
        df,
        ray_environment.feature_store.project,
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


def create_ray_environment():
    ray_config = IntegrationTestRepoConfig(
        provider="local",
        online_store_creator=RedisOnlineStoreCreator,
        offline_store_creator=RayDataSourceCreator,
        batch_engine={
            "type": "ray.engine",
            "use_ray_cluster": False,
            "max_workers": 2,
            "enable_optimization": True,
        },
    )
    ray_environment = construct_test_environment(
        ray_config, None, entity_key_serialization_version=3
    )
    ray_environment.setup()
    return ray_environment


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_get_historical_features():
    """Test Ray compute engine historical feature retrieval."""
    ray_environment = create_ray_environment()
    fs = ray_environment.feature_store
    registry = fs.registry
    data_source = create_feature_dataset(ray_environment)

    def transform_feature(df: pd.DataFrame) -> pd.DataFrame:
        df["sum_conv_rate"] = df["sum_conv_rate"] * 2
        df["avg_acc_rate"] = df["avg_acc_rate"] * 2
        return df

    driver_stats_fv = BatchFeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        mode="pandas",
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

        # Build retrieval task
        task = HistoricalRetrievalTask(
            project=ray_environment.project,
            entity_df=entity_df,
            feature_view=driver_stats_fv,
            full_feature_name=False,
            registry=registry,
        )

        # Run RayComputeEngine
        engine = RayComputeEngine(
            repo_config=ray_environment.config,
            offline_store=RayOfflineStore(),
            online_store=MagicMock(),
        )

        ray_dag_retrieval_job = engine.get_historical_features(registry, task)
        ray_dataset = cast(RayDAGRetrievalJob, ray_dag_retrieval_job).to_ray_dataset()
        df_out = ray_dataset.to_pandas().sort_values("driver_id")

        # Assert output
        assert df_out.driver_id.to_list() == [1001, 1002]
        assert abs(df_out["sum_conv_rate"].to_list()[0] - 1.6) < 1e-6
        assert abs(df_out["sum_conv_rate"].to_list()[1] - 2.0) < 1e-6
        assert abs(df_out["avg_acc_rate"].to_list()[0] - 1.0) < 1e-6
        assert abs(df_out["avg_acc_rate"].to_list()[1] - 1.0) < 1e-6

    finally:
        ray_environment.teardown()
        if ray.is_initialized():
            ray.shutdown()


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_materialize():
    """Test Ray compute engine materialization."""
    ray_environment = create_ray_environment()
    fs = ray_environment.feature_store
    registry = fs.registry
    data_source = create_feature_dataset(ray_environment)

    def transform_feature(df: pd.DataFrame) -> pd.DataFrame:
        df["sum_conv_rate"] = df["sum_conv_rate"] * 2
        df["avg_acc_rate"] = df["avg_acc_rate"] * 2
        return df

    driver_stats_fv = BatchFeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        mode="pandas",
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
        online=True,
        offline=False,
        source=data_source,
    )

    def tqdm_builder(length):
        return tqdm(length, ncols=100)

    try:
        fs.apply([driver, driver_stats_fv])

        # Build materialization task
        task = MaterializationTask(
            project=ray_environment.project,
            feature_view=driver_stats_fv,
            start_time=now - timedelta(days=2),
            end_time=now,
            tqdm_builder=tqdm_builder,
        )

        # Run RayComputeEngine
        engine = RayComputeEngine(
            repo_config=ray_environment.config,
            offline_store=RayOfflineStore(),
            online_store=MagicMock(),
        )

        ray_materialize_jobs = engine.materialize(registry, task)

        assert len(ray_materialize_jobs) == 1
        assert ray_materialize_jobs[0].status() == MaterializationJobStatus.SUCCEEDED

        # Additional assertions can be added here for online store checks

    finally:
        ray_environment.teardown()
        if ray.is_initialized():
            ray.shutdown()


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_config():
    """Test Ray compute engine configuration."""
    config = RayComputeEngineConfig(
        type="ray.engine",
        use_ray_cluster=True,
        ray_address="ray://localhost:10001",
        broadcast_join_threshold_mb=200,
        enable_distributed_joins=True,
        max_parallelism_multiplier=4,
        target_partition_size_mb=128,
        window_size_for_joins="2H",
        max_workers=4,
        enable_optimization=True,
        execution_timeout_seconds=3600,
    )

    assert config.type == "ray.engine"
    assert config.use_ray_cluster is True
    assert config.ray_address == "ray://localhost:10001"
    assert config.broadcast_join_threshold_mb == 200
    assert config.window_size_timedelta == timedelta(hours=2)
