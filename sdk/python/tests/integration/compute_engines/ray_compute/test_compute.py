from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest
from tqdm import tqdm

from feast import BatchFeatureView, Field
from feast.aggregation import Aggregation
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
from feast.transformation.ray_transformation import RayTransformation
from feast.types import Float32, Int32, Int64
from tests.integration.compute_engines.ray_compute.ray_shared_utils import (
    driver,
    now,
)


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_get_historical_features(
    ray_environment, feature_dataset, entity_df
):
    """Test Ray compute engine historical feature retrieval."""
    fs = ray_environment.feature_store
    registry = fs.registry

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
        source=feature_dataset,
    )

    fs.apply([driver, driver_stats_fv])

    # Build retrieval task
    task = HistoricalRetrievalTask(
        project=ray_environment.project,
        entity_df=entity_df,
        feature_view=driver_stats_fv,
        full_feature_name=False,
        registry=registry,
    )
    engine = RayComputeEngine(
        repo_config=ray_environment.config,
        offline_store=RayOfflineStore(),
        online_store=MagicMock(),
    )

    ray_dag_retrieval_job = engine.get_historical_features(registry, task)
    ray_dataset = cast(RayDAGRetrievalJob, ray_dag_retrieval_job).to_ray_dataset()
    df_out = ray_dataset.to_pandas().sort_values("driver_id")

    assert df_out.driver_id.to_list() == [1001, 1002]
    assert abs(df_out["sum_conv_rate"].to_list()[0] - 1.6) < 1e-6
    assert abs(df_out["sum_conv_rate"].to_list()[1] - 2.0) < 1e-6
    assert abs(df_out["avg_acc_rate"].to_list()[0] - 1.0) < 1e-6
    assert abs(df_out["avg_acc_rate"].to_list()[1] - 1.0) < 1e-6


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_materialize(ray_environment, feature_dataset):
    """Test Ray compute engine materialization."""
    fs = ray_environment.feature_store
    registry = fs.registry

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
        source=feature_dataset,
    )

    def tqdm_builder(length):
        return tqdm(length, ncols=100)

    fs.apply([driver, driver_stats_fv])

    task = MaterializationTask(
        project=ray_environment.project,
        feature_view=driver_stats_fv,
        start_time=now - timedelta(days=2),
        end_time=now,
        tqdm_builder=tqdm_builder,
    )

    engine = RayComputeEngine(
        repo_config=ray_environment.config,
        offline_store=RayOfflineStore(),
        online_store=MagicMock(),
    )

    ray_materialize_jobs = engine.materialize(registry, task)

    assert len(ray_materialize_jobs) == 1
    assert ray_materialize_jobs[0].status() == MaterializationJobStatus.SUCCEEDED


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_compute_engine_config():
    """Test Ray compute engine configuration."""
    config = RayComputeEngineConfig(
        type="ray.engine",
        ray_address="ray://localhost:10001",
        broadcast_join_threshold_mb=200,
        enable_distributed_joins=True,
        max_parallelism_multiplier=4,
        target_partition_size_mb=128,
        window_size_for_joins="2H",
        max_workers=4,
        enable_optimization=True,
    )

    assert config.type == "ray.engine"
    assert config.ray_address == "ray://localhost:10001"
    assert config.broadcast_join_threshold_mb == 200
    assert config.window_size_timedelta == timedelta(hours=2)


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_transformation_compute_engine(ray_environment, feature_dataset, entity_df):
    """Test Ray compute engine with Ray transformation mode."""
    import ray.data

    fs = ray_environment.feature_store
    registry = fs.registry

    def ray_transformation_udf(ds: ray.data.Dataset) -> ray.data.Dataset:
        """Ray native transformation that processes data in parallel."""

        def process_batch(batch: pd.DataFrame) -> pd.DataFrame:
            # Simulate some computation (e.g., feature engineering)
            if "conv_rate" in batch.columns:
                batch["processed_conv_rate"] = batch["conv_rate"] * 2.0
            if "acc_rate" in batch.columns:
                batch["processed_acc_rate"] = batch["acc_rate"] * 1.5
            return batch

        return ds.map_batches(
            process_batch,
            batch_format="pandas",
            concurrency=2,  # Use 2 parallel workers
        )

    # Create Ray transformation
    ray_transform = RayTransformation(
        udf=ray_transformation_udf,
        udf_string="def ray_transformation_udf(ds): return ds.map_batches(...)",
    )

    driver_stats_fv = BatchFeatureView(
        name="driver_hourly_stats_ray",
        entities=[driver],
        mode="ray",  # Use Ray transformation mode
        feature_transformation=ray_transform,
        ttl=timedelta(days=3),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="processed_conv_rate", dtype=Float32),
            Field(name="processed_acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
            Field(name="driver_id", dtype=Int32),
        ],
        online=False,
        offline=False,
        source=feature_dataset,
    )

    fs.apply([driver, driver_stats_fv])

    # Build retrieval task
    task = HistoricalRetrievalTask(
        project=ray_environment.project,
        entity_df=entity_df,
        feature_view=driver_stats_fv,
        full_feature_name=False,
        registry=registry,
    )
    engine = RayComputeEngine(
        repo_config=ray_environment.config,
        offline_store=RayOfflineStore(),
        online_store=MagicMock(),
    )

    ray_dag_retrieval_job = engine.get_historical_features(registry, task)
    ray_dataset = cast(RayDAGRetrievalJob, ray_dag_retrieval_job).to_ray_dataset()
    df_out = ray_dataset.to_pandas().sort_values("driver_id")

    # Verify the transformation was applied
    assert df_out.driver_id.to_list() == [1001, 1002]

    # Check that original columns are present
    assert "conv_rate" in df_out.columns
    assert "acc_rate" in df_out.columns

    # Check that transformed columns are present
    assert "processed_conv_rate" in df_out.columns
    assert "processed_acc_rate" in df_out.columns

    # Verify the transformation logic was applied
    for idx, row in df_out.iterrows():
        assert abs(row["processed_conv_rate"] - row["conv_rate"] * 2.0) < 1e-6
        assert abs(row["processed_acc_rate"] - row["acc_rate"] * 1.5) < 1e-6


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_ray_transformation_materialization(ray_environment, feature_dataset):
    """Test Ray transformation during materialization."""
    import ray.data

    fs = ray_environment.feature_store
    registry = fs.registry

    def ray_embedding_udf(ds: ray.data.Dataset) -> ray.data.Dataset:
        """Simulate embedding generation with Ray native processing."""

        def generate_embeddings(batch: pd.DataFrame) -> pd.DataFrame:
            # Simulate embedding generation
            if "conv_rate" in batch.columns:
                # Create a simple embedding based on conv_rate
                batch["embedding"] = batch["conv_rate"].apply(
                    lambda x: [x * 0.1, x * 0.2, x * 0.3]
                )
            return batch

        return ds.map_batches(generate_embeddings, batch_format="pandas", concurrency=2)

    # Create Ray transformation for embeddings
    ray_embedding_transform = RayTransformation(
        udf=ray_embedding_udf,
        udf_string="def ray_embedding_udf(ds): return ds.map_batches(...)",
    )

    driver_embeddings_fv = BatchFeatureView(
        name="driver_embeddings",
        entities=[driver],
        mode="ray",
        feature_transformation=ray_embedding_transform,
        ttl=timedelta(days=3),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(
                name="embedding", dtype=Float32
            ),  # This would be Array(Float32) in real usage
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=False,
        source=feature_dataset,
    )

    def tqdm_builder(length):
        return tqdm(length, ncols=100)

    fs.apply([driver, driver_embeddings_fv])

    task = MaterializationTask(
        project=ray_environment.project,
        feature_view=driver_embeddings_fv,
        start_time=now - timedelta(days=2),
        end_time=now,
        tqdm_builder=tqdm_builder,
    )

    engine = RayComputeEngine(
        repo_config=ray_environment.config,
        offline_store=RayOfflineStore(),
        online_store=MagicMock(),
    )

    ray_materialize_jobs = engine.materialize(registry, task)

    assert len(ray_materialize_jobs) == 1
    assert ray_materialize_jobs[0].status() == MaterializationJobStatus.SUCCEEDED
