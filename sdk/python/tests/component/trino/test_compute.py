from datetime import datetime, timedelta, timezone
from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

from feast import BatchFeatureView, Entity, Field
from feast.aggregation import Aggregation
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.trino.compute import (
    TrinoComputeEngine,
    TrinoComputeEngineConfig,
)
from feast.infra.compute_engines.trino.job import TrinoDAGRetrievalJob
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import (
    Results,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
    TrinoSource,
)
from feast.transformation.mode import TransformationMode
from feast.transformation.trino_transformation import TrinoTransformation
from feast.types import Float32, Int32
from feast.value_type import ValueType


@pytest.fixture
def driver_entity():
    return Entity(
        name="driver_id",
        value_type=ValueType.INT32,
        join_keys=["driver_id"],
    )


@pytest.fixture
def trino_source():
    return TrinoSource(
        name="driver_hourly_stats_source",
        table="iceberg.feast.driver_hourly_stats",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )


def test_trino_compute_engine_get_historical_features(driver_entity, trino_source):
    """Verifies historical feature retrieval via Trino SQL compilation and execution."""
    tf = TrinoTransformation(
        mode=TransformationMode.TRINO_SQL,
        udf="SELECT driver_id, event_timestamp, conv_rate * 2.0 AS conv_rate, acc_rate * 2.0 AS acc_rate FROM {}",
        udf_string="SELECT driver_id, event_timestamp, conv_rate * 2.0 AS conv_rate, acc_rate * 2.0 AS acc_rate FROM {}",
    )

    fv = BatchFeatureView(
        name="driver_hourly_stats",
        entities=[driver_entity],
        feature_transformation=tf,
        aggregations=[
            Aggregation(column="conv_rate", function="sum"),
            Aggregation(column="acc_rate", function="avg"),
        ],
        ttl=timedelta(days=3),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="driver_id", dtype=Int32),
        ],
        online=False,
        offline=False,
        source=trino_source,
    )

    entity_df = pd.DataFrame(
        {
            "driver_id": [1001, 1002],
            "event_timestamp": [
                datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            ],
        }
    )

    mock_client = MagicMock()
    # Mock query result
    mock_client.execute_query.return_value = Results(
        data=[
            [1001, 3.1, 1.4],
            [1002, 2.0, 1.0],
        ],
        columns=[
            {"name": "driver_id", "type": "integer"},
            {"name": "sum_conv_rate", "type": "real"},
            {"name": "avg_acc_rate", "type": "real"},
        ],
    )

    repo_config = MagicMock()
    repo_config.batch_engine = TrinoComputeEngineConfig(
        host="localhost",
        port=8080,
        catalog="iceberg",
        user="test_user",
    )
    repo_config.offline_store = MagicMock()

    engine = TrinoComputeEngine(
        repo_config=repo_config,
        offline_store=MagicMock(),
        online_store=MagicMock(),
    )
    engine.client = mock_client
    mock_registry = MagicMock()
    mock_registry.get_entity.return_value = driver_entity

    task = HistoricalRetrievalTask(
        project="test_project",
        entity_df=entity_df,
        feature_view=fv,
        full_feature_name=False,
        registry=mock_registry,
    )

    job = cast(
        TrinoDAGRetrievalJob, engine.get_historical_features(mock_registry, task)
    )
    compiled_sql = job.to_sql()

    # Assert SQL structure
    assert "WITH" in compiled_sql
    assert "iceberg.feast.driver_hourly_stats" in compiled_sql
    assert "GROUP BY" in compiled_sql
    assert "conv_rate * 2.0" in compiled_sql

    # Assert lazy evaluation works
    df_out = job.to_df()
    assert df_out["driver_id"].tolist() == [1001, 1002]
    assert df_out["sum_conv_rate"].tolist() == [3.1, 2.0]


def test_trino_compute_engine_materialize(driver_entity, trino_source):
    """Verifies materialization via streaming online writes and offline staging swap."""
    fv = BatchFeatureView(
        name="driver_hourly_stats",
        entities=[driver_entity],
        aggregations=[
            Aggregation(column="conv_rate", function="sum"),
        ],
        schema=[
            Field(name="sum_conv_rate", dtype=Float32),
            Field(name="driver_id", dtype=Int32),
        ],
        online=True,
        offline=True,
        source=trino_source,
    )

    mock_client = MagicMock()
    # Mock cursor for streaming online writes
    mock_cursor = MagicMock()
    mock_query = MagicMock()
    mock_query.columns = [
        {"name": "driver_id", "type": "integer"},
        {"name": "event_timestamp", "type": "timestamp"},
        {"name": "sum_conv_rate", "type": "real"},
    ]
    mock_cursor._query = mock_query
    # Return 1 batch of rows then empty
    mock_cursor.fetchmany.side_effect = [
        [
            (1001, datetime(2025, 1, 15, 12, 0, 0), 3.1),
            (1002, datetime(2025, 1, 15, 12, 0, 0), 2.0),
        ],
        [],
    ]
    mock_client._get_cursor.return_value = mock_cursor

    repo_config = MagicMock()
    repo_config.batch_engine = TrinoComputeEngineConfig(
        host="localhost",
        port=8080,
        catalog="iceberg",
        user="test_user",
        batch_size=1000,
        write_concurrency=2,
    )
    repo_config.materialization_config.online_write_batch_size = 100
    repo_config.offline_store = MagicMock()

    mock_online_store = MagicMock()

    engine = TrinoComputeEngine(
        repo_config=repo_config,
        offline_store=MagicMock(),
        online_store=mock_online_store,
    )
    engine.client = mock_client
    mock_registry = MagicMock()
    mock_registry.get_entity.return_value = driver_entity

    task = MaterializationTask(
        project="test_project",
        feature_view=fv,
        start_time=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc),
        tqdm_builder=MagicMock(),
    )

    jobs = engine.materialize(mock_registry, task)
    assert len(jobs) == 1
    if jobs[0].error() is not None:
        raise jobs[0].error()
    assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED

    # Verify online store was called with batch writes
    assert mock_online_store.online_write_batch.called

    # Verify offline staging + swap queries were executed
    executed_queries = [call[0][0] for call in mock_client.execute_query.call_args_list]
    assert any(
        "CREATE TABLE iceberg.feast.driver_hourly_stats__staging" in q
        for q in executed_queries
    )
    assert any(
        "DROP TABLE IF EXISTS iceberg.feast.driver_hourly_stats" in q
        for q in executed_queries
    )
    assert any(
        "ALTER TABLE iceberg.feast.driver_hourly_stats__staging RENAME TO driver_hourly_stats"
        in q
        for q in executed_queries
    )
