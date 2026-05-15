import tempfile
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
from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import (
    RaySource,
)
from feast.transformation.ray_transformation import RayTransformation
from feast.types import Float32, Int32, Int64
from tests.component.ray.ray_shared_utils import (
    driver,
    now,
    today,
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


# ---------------------------------------------------------------------------
# RaySource integration tests — pull_latest and pull_all time-range filtering
# ---------------------------------------------------------------------------


def _write_ray_source_parquet(tmp_dir: str) -> str:
    """Write a small driver-stats parquet with 4 rows (2 per entity) and return path."""
    yesterday = today - timedelta(days=1)
    last_week = today - timedelta(days=7)
    two_weeks_ago = today - timedelta(days=14)

    df = pd.DataFrame(
        [
            # driver 1001 — two rows within the last 10 days
            {
                "driver_id": 1001,
                "event_timestamp": yesterday,
                "created": now,
                "conv_rate": 0.8,
                "acc_rate": 0.5,
                "avg_daily_trips": 15,
            },
            {
                "driver_id": 1001,
                "event_timestamp": last_week,
                "created": now,
                "conv_rate": 0.75,
                "acc_rate": 0.9,
                "avg_daily_trips": 14,
            },
            # driver 1002 — recent row inside window, old row outside
            {
                "driver_id": 1002,
                "event_timestamp": yesterday,
                "created": now,
                "conv_rate": 0.7,
                "acc_rate": 0.4,
                "avg_daily_trips": 12,
            },
            {
                "driver_id": 1002,
                "event_timestamp": two_weeks_ago,
                "created": now,
                "conv_rate": 0.3,
                "acc_rate": 0.6,
                "avg_daily_trips": 11,
            },
        ]
    )
    path = f"{tmp_dir}/ray_source_features.parquet"
    df.to_parquet(path, index=False)
    return path


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_pull_latest_from_ray_source_filters_and_deduplicates(ray_environment):
    """pull_latest_from_table_or_query with RaySource must:

    1. Respect the start_date/end_date window — the row for driver 1002 at
       two_weeks_ago falls outside the 10-day window and must be excluded.
    2. Deduplicate to the single latest row per entity — driver 1001 has two
       rows inside the window; only the most recent (yesterday) must survive.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        parquet_path = _write_ray_source_parquet(tmp_dir)

        source = RaySource(
            name="driver_stats_ray",
            reader_type="parquet",
            path=parquet_path,
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        )

        start = now - timedelta(days=10)
        end = now

        job = RayOfflineStore.pull_latest_from_table_or_query(
            config=ray_environment.config,
            data_source=source,
            join_key_columns=["driver_id"],
            feature_name_columns=["conv_rate", "acc_rate", "avg_daily_trips"],
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
            start_date=start,
            end_date=end,
        )

        df = job.to_df().sort_values("driver_id").reset_index(drop=True)

        # Exactly one row per entity
        assert df["driver_id"].to_list() == [1001, 1002], (
            f"Expected [1001, 1002], got {df['driver_id'].to_list()}"
        )

        # driver 1001: latest row is yesterday (conv_rate=0.8)
        assert abs(df.loc[df["driver_id"] == 1001, "conv_rate"].iloc[0] - 0.8) < 1e-5

        # driver 1002: the two_weeks_ago row is outside the window; only yesterday survives
        assert abs(df.loc[df["driver_id"] == 1002, "conv_rate"].iloc[0] - 0.7) < 1e-5


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_pull_all_from_ray_source_filters_by_time_range(ray_environment):
    """pull_all_from_table_or_query with RaySource must return all rows within
    the time window without deduplication.

    - driver 1001: both rows (yesterday, last_week) are inside the 10-day window → 2 rows
    - driver 1002: yesterday is inside, two_weeks_ago is outside → 1 row
    - Total: 3 rows
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        parquet_path = _write_ray_source_parquet(tmp_dir)

        source = RaySource(
            name="driver_stats_ray_all",
            reader_type="parquet",
            path=parquet_path,
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        )

        start = now - timedelta(days=10)
        end = now

        job = RayOfflineStore.pull_all_from_table_or_query(
            config=ray_environment.config,
            data_source=source,
            join_key_columns=["driver_id"],
            feature_name_columns=["conv_rate", "acc_rate", "avg_daily_trips"],
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
            start_date=start,
            end_date=end,
        )

        df = (
            job.to_df()
            .sort_values(["driver_id", "event_timestamp"])
            .reset_index(drop=True)
        )

        # 3 rows: both for 1001, one for 1002 (the 14-day-old row is outside window)
        assert len(df) == 3, (
            f"Expected 3 rows, got {len(df)}: "
            f"{df[['driver_id', 'event_timestamp']].to_dict('records')}"
        )
        assert sorted(df["driver_id"].to_list()) == [1001, 1001, 1002]


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_get_historical_features_with_ray_source(ray_environment):
    """engine.get_historical_features() with a BatchFeatureView backed by RaySource.

    This exercises the DAG execution path (RayFeatureBuilder → RayReadNode →
    _resolve_source_dataset) rather than pull_latest_from_table_or_query, which
    is a distinct code path from the two pull_* tests above.  Asserting on
    returned feature values confirms the full read pipeline is correct, not just
    that execution succeeded.

    Expected results with entity_df timestamp = today and TTL = 10 days:
      - driver 1001: latest row is yesterday  → conv_rate ≈ 0.8
      - driver 1002: latest row is yesterday  → conv_rate ≈ 0.7
        (the two_weeks_ago row is outside the TTL window and must be excluded)
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        parquet_path = _write_ray_source_parquet(tmp_dir)

        source = RaySource(
            name="driver_stats_ray_hist",
            reader_type="parquet",
            path=parquet_path,
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        )

        fs = ray_environment.feature_store
        registry = fs.registry

        driver_stats_fv = BatchFeatureView(
            name="driver_hourly_stats_ray_source",
            entities=[driver],
            mode="pandas",
            ttl=timedelta(days=10),
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="acc_rate", dtype=Float32),
                Field(name="avg_daily_trips", dtype=Int64),
                Field(name="driver_id", dtype=Int32),
            ],
            online=False,
            offline=False,
            source=source,
        )

        fs.apply([driver, driver_stats_fv])

        entity_df = pd.DataFrame(
            [
                {"driver_id": 1001, "event_timestamp": today},
                {"driver_id": 1002, "event_timestamp": today},
            ]
        )

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

        job = engine.get_historical_features(registry, task)
        df_out = (
            cast(RayDAGRetrievalJob, job)
            .to_ray_dataset()
            .to_pandas()
            .sort_values("driver_id")
            .reset_index(drop=True)
        )

        assert df_out["driver_id"].to_list() == [1001, 1002]
        assert (
            abs(df_out.loc[df_out["driver_id"] == 1001, "conv_rate"].iloc[0] - 0.8)
            < 1e-5
        )
        assert (
            abs(df_out.loc[df_out["driver_id"] == 1002, "conv_rate"].iloc[0] - 0.7)
            < 1e-5
        )


@pytest.mark.integration
@pytest.mark.xdist_group(name="ray")
def test_pull_latest_from_ray_sql_source_filters_and_deduplicates(ray_environment):
    """pull_latest_from_table_or_query with RaySource(reader_type="sql") using
    a local SQLite database.

    SQLite requires no external server, making it CI-safe.  This exercises the
    non-file-backed RaySource path where data is loaded by read_sql and then
    filtered/deduplicated by _load_and_filter_dataset_ray(pre_loaded_ds=...).
    """
    pytest.importorskip("sqlalchemy", reason="sqlalchemy required for SQL reader")

    import sqlite3

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = f"{tmp_dir}/driver_stats.db"
        yesterday = today - timedelta(days=1)
        last_week = today - timedelta(days=7)
        two_weeks_ago = today - timedelta(days=14)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE driver_stats "
            "(driver_id INTEGER, event_timestamp TEXT, created TEXT, "
            "conv_rate REAL, acc_rate REAL, avg_daily_trips INTEGER)"
        )
        conn.executemany(
            "INSERT INTO driver_stats VALUES (?,?,?,?,?,?)",
            [
                (1001, yesterday.isoformat(), now.isoformat(), 0.8, 0.5, 15),
                (1001, last_week.isoformat(), now.isoformat(), 0.75, 0.9, 14),
                (1002, yesterday.isoformat(), now.isoformat(), 0.7, 0.4, 12),
                (1002, two_weeks_ago.isoformat(), now.isoformat(), 0.3, 0.6, 11),
            ],
        )
        conn.commit()
        conn.close()

        source = RaySource(
            name="driver_stats_sql",
            reader_type="sql",
            timestamp_field="event_timestamp",
            reader_options={
                "sql": "SELECT * FROM driver_stats",
                "connection_url": f"sqlite:///{db_path}",
            },
        )

        start = now - timedelta(days=10)
        end = now

        job = RayOfflineStore.pull_latest_from_table_or_query(
            config=ray_environment.config,
            data_source=source,
            join_key_columns=["driver_id"],
            feature_name_columns=["conv_rate", "acc_rate", "avg_daily_trips"],
            timestamp_field="event_timestamp",
            created_timestamp_column=None,
            start_date=start,
            end_date=end,
        )

        df = job.to_df().sort_values("driver_id").reset_index(drop=True)

        # One row per entity, within the window
        assert df["driver_id"].to_list() == [1001, 1002], (
            f"Expected [1001, 1002], got {df['driver_id'].to_list()}"
        )

        # Latest for each: 1001→yesterday (0.8), 1002→yesterday (0.7)
        assert abs(df.loc[df["driver_id"] == 1001, "conv_rate"].iloc[0] - 0.8) < 1e-5
        assert abs(df.loc[df["driver_id"] == 1002, "conv_rate"].iloc[0] - 0.7) < 1e-5
