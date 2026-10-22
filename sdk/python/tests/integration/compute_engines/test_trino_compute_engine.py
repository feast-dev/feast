from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict
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
from feast.infra.offline_stores.contrib.trino_offline_store.connectors.upload import (
    upload_pandas_dataframe_to_trino,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    TrinoOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
    TrinoSource,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStore, SqliteOnlineStoreConfig
from feast.transformation.mode import TransformationMode
from feast.transformation.trino_transformation import TrinoTransformation
from feast.types import Float32, Int32, Int64, String
from feast.value_type import ValueType

docker_available = False
try:
    import docker
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    try:
        _docker = docker.from_env()
        _docker.ping()
        docker_available = True
    except Exception:
        pass
except ImportError:
    pass

_requires_docker = pytest.mark.skipif(
    not docker_available,
    reason="Docker is not available or not running. Start Docker daemon to run Trino integration tests.",
)


@pytest.fixture(scope="module")
def trino_container():
    """Start a real Trino container using the memory catalog for integration tests."""
    catalog_dir = (
        Path(__file__).parents[3]
        / "feast"
        / "infra"
        / "offline_stores"
        / "contrib"
        / "trino_offline_store"
        / "tests"
        / "catalog"
    )
    container = (
        DockerContainer("trinodb/trino:483")
        .with_volume_mapping(str(catalog_dir.resolve()), "/etc/catalog/")
        .with_volume_mapping(str(catalog_dir.resolve()), "/etc/trino/catalog/")
        .with_exposed_ports("8080")
    )
    container.start()
    try:
        wait_for_logs(container=container, predicate="SERVER STARTED", timeout=60)
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def trino_server_info(trino_container) -> Dict[str, Any]:
    host = trino_container.get_container_host_ip()
    port = int(trino_container.get_exposed_port("8080"))
    return {
        "host": host,
        "port": port,
        "catalog": "memory",
        "dataset": "feast_integration",
        "user": "test_user",
    }


@pytest.fixture(scope="module")
def live_trino_client(trino_server_info) -> Trino:
    client = Trino(
        host=trino_server_info["host"],
        port=trino_server_info["port"],
        user=trino_server_info["user"],
        catalog=trino_server_info["catalog"],
        source="feast-integration-tests",
        http_scheme="http",
        verify=False,
        extra_credential=None,
        auth=None,
    )
    client.execute_query(
        f"CREATE SCHEMA IF NOT EXISTS {trino_server_info['catalog']}.{trino_server_info['dataset']}"
    )
    return client


@pytest.fixture
def driver_entity():
    return Entity(
        name="driver_id",
        value_type=ValueType.INT32,
        join_keys=["driver_id"],
    )


@_requires_docker
@pytest.mark.integration
class TestTrinoComputeEngineIntegration:
    """Integration test suite executing real queries against a live Trino cluster."""

    def test_trino_historical_retrieval_pit_e2e(
        self, trino_server_info, live_trino_client, driver_entity
    ):
        """Validates real Trino CTE compilation, entity_df upload, and PIT join execution."""
        catalog = trino_server_info["catalog"]
        dataset = trino_server_info["dataset"]
        source_table = f"{catalog}.{dataset}.source_driver_stats"

        # 1. Upload source features to memory catalog
        live_trino_client.execute_query(f"DROP TABLE IF EXISTS {source_table}")
        source_df = pd.DataFrame(
            {
                "driver_id": [1001, 1001, 1002],
                "event_timestamp": [
                    datetime(2025, 1, 10, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 14, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 12, 10, 0, 0, tzinfo=timezone.utc),
                ],
                "created_timestamp": [
                    datetime(2025, 1, 10, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 14, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 12, 10, 0, 0, tzinfo=timezone.utc),
                ],
                "conv_rate": [0.2, 0.8, 0.5],
                "acc_rate": [0.3, 0.9, 0.6],
            }
        )
        upload_pandas_dataframe_to_trino(
            client=live_trino_client,
            df=source_df,
            table=source_table,
            connector_args={"type": "memory"},
        )

        trino_source = TrinoSource(
            name="source_driver_stats",
            table=source_table,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        tf = TrinoTransformation(
            mode=TransformationMode.TRINO_SQL,
            udf="SELECT driver_id, event_timestamp, created_timestamp, conv_rate * 2.0 AS conv_rate, acc_rate FROM {}",
            udf_string="SELECT driver_id, event_timestamp, created_timestamp, conv_rate * 2.0 AS conv_rate, acc_rate FROM {}",
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

        # 2. Entity dataframe requesting point-in-time features
        entity_df = pd.DataFrame(
            {
                "driver_id": [1001, 1002],
                "event_timestamp": [
                    datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 13, 12, 0, 0, tzinfo=timezone.utc),
                ],
            }
        )

        repo_config = MagicMock()
        repo_config.batch_engine = TrinoComputeEngineConfig(
            host=trino_server_info["host"],
            port=trino_server_info["port"],
            catalog=catalog,
            dataset=dataset,
            user=trino_server_info["user"],
            connector={"type": "memory"},
        )
        repo_config.offline_store = TrinoOfflineStoreConfig(
            host=trino_server_info["host"],
            port=trino_server_info["port"],
            catalog=catalog,
            dataset=dataset,
            user=trino_server_info["user"],
            connector={"type": "memory"},
        )

        engine = TrinoComputeEngine(
            repo_config=repo_config,
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )

        mock_registry = MagicMock()
        mock_registry.get_entity.return_value = driver_entity

        task = HistoricalRetrievalTask(
            project="test_project",
            entity_df=entity_df,
            feature_view=fv,
            full_feature_name=False,
            registry=mock_registry,
        )

        job = engine.get_historical_features(mock_registry, task)
        assert isinstance(job, TrinoDAGRetrievalJob)

        # 3. Execute live query on Trino and verify returned PIT data
        result_df = job.to_df()
        assert len(result_df) == 2
        assert "driver_id" in result_df.columns
        assert "sum_conv_rate" in result_df.columns
        assert "avg_acc_rate" in result_df.columns

        row_1001 = result_df[result_df["driver_id"] == 1001].iloc[0]
        # driver 1001 on 2025-01-15: row on 2025-01-14 (0.8 * 2 = 1.6, acc_rate = 0.9) within 3-day TTL,
        # row on 2025-01-10 (5 days earlier) excluded by TTL
        assert pytest.approx(row_1001["sum_conv_rate"], rel=1e-3) == 1.6
        assert pytest.approx(row_1001["avg_acc_rate"], rel=1e-3) == 0.9

        row_1002 = result_df[result_df["driver_id"] == 1002].iloc[0]
        # driver 1002 on 2025-01-13: row on 2025-01-12 (0.5 * 2 = 1.0, acc_rate = 0.6) within 3-day TTL
        assert pytest.approx(row_1002["sum_conv_rate"], rel=1e-3) == 1.0
        assert pytest.approx(row_1002["avg_acc_rate"], rel=1e-3) == 0.6

    def test_trino_materialize_online_streaming_e2e(
        self, trino_server_info, live_trino_client, driver_entity, tmp_path
    ):
        """Validates real cursor fetchmany streaming and batch writes to online store."""
        catalog = trino_server_info["catalog"]
        dataset = trino_server_info["dataset"]
        source_table = f"{catalog}.{dataset}.stream_driver_stats"

        # 1. Upload source features
        live_trino_client.execute_query(f"DROP TABLE IF EXISTS {source_table}")
        source_df = pd.DataFrame(
            {
                "driver_id": [101, 102, 103, 104, 105],
                "event_timestamp": [
                    datetime(2025, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
                ]
                * 5,
                "created_timestamp": [
                    datetime(2025, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
                ]
                * 5,
                "conv_rate": [0.1, 0.2, 0.3, 0.4, 0.5],
            }
        )
        upload_pandas_dataframe_to_trino(
            client=live_trino_client,
            df=source_df,
            table=source_table,
            connector_args={"type": "memory"},
        )

        trino_source = TrinoSource(
            name="stream_source",
            table=source_table,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        fv = BatchFeatureView(
            name="stream_fv",
            entities=[driver_entity],
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="driver_id", dtype=Int32),
            ],
            online=True,
            offline=False,
            source=trino_source,
        )

        # 2. Setup real SqliteOnlineStore
        sqlite_db_path = str(tmp_path / "online.db")
        online_store_cfg = SqliteOnlineStoreConfig(path=sqlite_db_path)
        online_store = SqliteOnlineStore()

        repo_config = MagicMock()
        repo_config.project = "test_project"
        repo_config.repo_path = str(tmp_path)
        repo_config.online_store = online_store_cfg
        repo_config.entity_key_serialization_version = 3
        repo_config.registry.enable_online_feature_view_versioning = False
        online_store.update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[fv],
            entities_to_delete=[],
            entities_to_keep=[driver_entity],
            partial=False,
        )
        repo_config.batch_engine = TrinoComputeEngineConfig(
            host=trino_server_info["host"],
            port=trino_server_info["port"],
            catalog=catalog,
            dataset=dataset,
            user=trino_server_info["user"],
            batch_size=2,  # Force multiple fetchmany chunk iterations
            write_concurrency=1,
            connector={"type": "memory"},
        )
        repo_config.materialization_config.online_write_batch_size = 2
        repo_config.offline_store = TrinoOfflineStoreConfig(
            host=trino_server_info["host"],
            port=trino_server_info["port"],
            catalog=catalog,
            dataset=dataset,
            user=trino_server_info["user"],
            connector={"type": "memory"},
        )

        engine = TrinoComputeEngine(
            repo_config=repo_config,
            offline_store=MagicMock(),
            online_store=online_store,
        )

        mock_registry = MagicMock()
        mock_registry.get_entity.return_value = driver_entity

        task = MaterializationTask(
            project="test_project",
            feature_view=fv,
            start_time=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc),
            tqdm_builder=MagicMock(),
        )

        # 3. Execute materialization and verify status
        jobs = engine.materialize(mock_registry, task)
        assert len(jobs) == 1
        if jobs[0].error() is not None:
            raise jobs[0].error()
        assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED

        from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        k101 = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int32_val=101)]
        )
        k105 = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int32_val=105)]
        )

        online_features = online_store.online_read(
            config=repo_config,
            table=fv,
            entity_keys=[k101, k105],
            requested_features=["conv_rate"],
        )
        assert len(online_features) == 2
        ts_101, feats_101 = online_features[0]
        ts_105, feats_105 = online_features[1]
        assert feats_101 is not None and feats_105 is not None
        assert pytest.approx(feats_101["conv_rate"].float_val, rel=1e-3) == 0.1
        assert pytest.approx(feats_105["conv_rate"].float_val, rel=1e-3) == 0.5

    def test_trino_materialize_offline_append_e2e(
        self, trino_server_info, live_trino_client, driver_entity, tmp_path
    ):
        """Validates real Trino offline write in 'append' mode."""
        catalog = trino_server_info["catalog"]
        dataset = trino_server_info["dataset"]
        source_table = f"{catalog}.{dataset}.offline_src_stats"
        target_table = f"{catalog}.{dataset}.offline_target_stats"

        # 1. Clean existing tables
        live_trino_client.execute_query(f"DROP TABLE IF EXISTS {source_table}")
        live_trino_client.execute_query(f"DROP TABLE IF EXISTS {target_table}")

        # 2. Upload source rows
        source_df = pd.DataFrame(
            {
                "driver_id": [201, 202],
                "event_timestamp": [
                    datetime(2025, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
                ]
                * 2,
                "created_timestamp": [
                    datetime(2025, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
                ]
                * 2,
                "conv_rate": [0.4, 0.7],
            }
        )
        upload_pandas_dataframe_to_trino(
            client=live_trino_client,
            df=source_df,
            table=source_table,
            connector_args={"type": "memory"},
        )

        trino_source = TrinoSource(
            name="offline_src",
            table=source_table,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        target_source = TrinoSource(
            name="offline_target",
            table=target_table,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        source_fv = BatchFeatureView(
            name="source_fv",
            entities=[driver_entity],
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="driver_id", dtype=Int32),
            ],
            source=trino_source,
        )

        fv = BatchFeatureView(
            name="offline_fv",
            entities=[driver_entity],
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="driver_id", dtype=Int32),
            ],
            online=False,
            offline=True,
            source=[source_fv],
            sink_source=target_source,
        )

        repo_config = MagicMock()
        repo_config.repo_path = str(tmp_path)
        repo_config.batch_engine = TrinoComputeEngineConfig(
            host=trino_server_info["host"],
            port=trino_server_info["port"],
            catalog=catalog,
            dataset=dataset,
            user=trino_server_info["user"],
            offline_write_mode="append",
            connector={"type": "memory"},
        )
        repo_config.offline_store = TrinoOfflineStoreConfig(
            host=trino_server_info["host"],
            port=trino_server_info["port"],
            catalog=catalog,
            dataset=dataset,
            user=trino_server_info["user"],
            connector={"type": "memory"},
        )

        engine = TrinoComputeEngine(
            repo_config=repo_config,
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )

        mock_registry = MagicMock()
        mock_registry.get_entity.return_value = driver_entity

        task = MaterializationTask(
            project="test_project",
            feature_view=fv,
            start_time=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc),
            tqdm_builder=MagicMock(),
        )

        # First materialization creates the target table
        jobs = engine.materialize(mock_registry, task)
        assert len(jobs) == 1
        if jobs[0].error() is not None:
            raise jobs[0].error()
        assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED

        res = live_trino_client.execute_query(f"SELECT COUNT(*) FROM {target_table}")
        assert res.data[0][0] == 2

        # Second materialization appends rows
        jobs2 = engine.materialize(mock_registry, task)
        assert len(jobs2) == 1
        assert jobs2[0].status() == MaterializationJobStatus.SUCCEEDED

        res2 = live_trino_client.execute_query(f"SELECT COUNT(*) FROM {target_table}")
        assert res2.data[0][0] == 4

    def test_trino_data_types_roundtrip_e2e(
        self, trino_server_info, live_trino_client, driver_entity
    ):
        """Validates end-to-end type mapping from Trino types to Python/Arrow results."""
        catalog = trino_server_info["catalog"]
        dataset = trino_server_info["dataset"]
        types_table = f"{catalog}.{dataset}.types_test_stats"

        live_trino_client.execute_query(f"DROP TABLE IF EXISTS {types_table}")
        types_df = pd.DataFrame(
            {
                "driver_id": [3001],
                "event_timestamp": [
                    datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
                ],
                "created_timestamp": [
                    datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
                ],
                "int_val": [9223372036854775807],  # max int64
                "float_val": [3.1415926535],
                "str_val": ["trino_roundtrip_test"],
            }
        )
        upload_pandas_dataframe_to_trino(
            client=live_trino_client,
            df=types_df,
            table=types_table,
            connector_args={"type": "memory"},
        )

        trino_source = TrinoSource(
            name="types_source",
            table=types_table,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        fv = BatchFeatureView(
            name="types_fv",
            entities=[driver_entity],
            schema=[
                Field(name="int_val", dtype=Int64),
                Field(name="float_val", dtype=Float32),
                Field(name="str_val", dtype=String),
                Field(name="driver_id", dtype=Int32),
            ],
            online=False,
            offline=False,
            source=trino_source,
        )

        entity_df = pd.DataFrame(
            {
                "driver_id": [3001],
                "event_timestamp": [
                    datetime(2025, 1, 16, 0, 0, 0, tzinfo=timezone.utc)
                ],
            }
        )

        repo_config = MagicMock()
        repo_config.batch_engine = TrinoComputeEngineConfig(
            host=trino_server_info["host"],
            port=trino_server_info["port"],
            catalog=catalog,
            dataset=dataset,
            user=trino_server_info["user"],
            connector={"type": "memory"},
        )
        repo_config.offline_store = TrinoOfflineStoreConfig(
            host=trino_server_info["host"],
            port=trino_server_info["port"],
            catalog=catalog,
            dataset=dataset,
            user=trino_server_info["user"],
            connector={"type": "memory"},
        )

        engine = TrinoComputeEngine(
            repo_config=repo_config,
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )

        mock_registry = MagicMock()
        mock_registry.get_entity.return_value = driver_entity

        task = HistoricalRetrievalTask(
            project="test_project",
            entity_df=entity_df,
            feature_view=fv,
            full_feature_name=False,
            registry=mock_registry,
        )

        job = engine.get_historical_features(mock_registry, task)
        df = job.to_df()
        assert len(df) == 1
        assert df["int_val"].iloc[0] == 9223372036854775807
        assert pytest.approx(df["float_val"].iloc[0], rel=1e-3) == 3.1415926535
        assert df["str_val"].iloc[0] == "trino_roundtrip_test"
