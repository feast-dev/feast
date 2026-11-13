from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)

pytest.importorskip("kubernetes")

from feast.infra.compute_engines.kubernetes.k8s_engine import (  # noqa: E402
    KubernetesComputeEngine,
    KubernetesComputeEngineConfig,
)

_MODULE = "feast.infra.compute_engines.kubernetes.k8s_engine"


class _FakeJob:
    """Reports WAITING, then RUNNING, then SUCCEEDED on later calls."""

    def __init__(self):
        self._statuses = [
            MaterializationJobStatus.WAITING,
            MaterializationJobStatus.RUNNING,
        ]

    def status(self):
        if self._statuses:
            return self._statuses.pop(0)
        return MaterializationJobStatus.SUCCEEDED

    def job_id(self):
        return "feast-materialization-test"


@patch(f"{_MODULE}.sleep")
@patch(f"{_MODULE}._get_column_names", return_value=([], [], "ts", None))
@patch(f"{_MODULE}.client")
@patch(f"{_MODULE}.k8s_config")
def test_async_mode_waits_for_job_to_finish(
    _k8s_config, _client, _get_column_names, mock_sleep
):
    repo_config = MagicMock()
    repo_config.batch_engine = KubernetesComputeEngineConfig(synchronous=False)
    offline_store = MagicMock()
    offline_store.pull_latest_from_table_or_query.return_value.to_remote_storage.return_value = [
        "s3://bucket/part-0.parquet",
        "s3://bucket/part-1.parquet",
    ]
    engine = KubernetesComputeEngine(
        repo_config=repo_config, offline_store=offline_store, online_store=None
    )
    fake_job = _FakeJob()
    engine._create_kubernetes_job = MagicMock(return_value=fake_job)

    feature_view = MagicMock()
    feature_view.name = "driver_stats"
    feature_view.entities = []
    task = MaterializationTask(
        project="test",
        feature_view=feature_view,
        start_time=datetime(2025, 1, 1),
        end_time=datetime(2025, 1, 2),
    )

    jobs = engine.materialize(MagicMock(), task)

    # One job for all pods, and the engine only returns once it has finished.
    engine._create_kubernetes_job.assert_called_once()
    assert engine._create_kubernetes_job.call_args.args[1] == [
        "s3://bucket/part-0.parquet",
        "s3://bucket/part-1.parquet",
    ]
    assert mock_sleep.call_count == 2
    assert jobs == [fake_job]
    assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED
