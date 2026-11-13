from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)

pytest.importorskip("kubernetes")

from kubernetes.client import V1Job, V1JobCondition, V1JobStatus  # noqa: E402

from feast.infra.compute_engines.kubernetes.k8s_engine import (  # noqa: E402
    KubernetesComputeEngine,
    KubernetesComputeEngineConfig,
)
from feast.infra.compute_engines.kubernetes.k8s_materialization_job import (  # noqa: E402
    KubernetesMaterializationJob,
)

_MODULE = "feast.infra.compute_engines.kubernetes.k8s_engine"
_JOB_MODULE = "feast.infra.compute_engines.kubernetes.k8s_materialization_job"


def _condition(type_: str, message: str = "") -> V1JobCondition:
    return V1JobCondition(type=type_, status="True", message=message)


# Kubernetes 1.31+ puts SuccessCriteriaMet / FailureTarget before the terminal
# Complete / Failed condition.
_SUCCEEDED = V1JobStatus(
    completion_time=datetime(2025, 1, 2),
    conditions=[_condition("SuccessCriteriaMet"), _condition("Complete")],
)
_FAILED = V1JobStatus(
    conditions=[
        _condition("FailureTarget", "BackoffLimitExceeded"),
        _condition("Failed", "BackoffLimitExceeded"),
    ],
)


@pytest.mark.parametrize(
    "final_status, expected",
    [
        (_SUCCEEDED, MaterializationJobStatus.SUCCEEDED),
        (_FAILED, MaterializationJobStatus.ERROR),
    ],
)
@patch(f"{_JOB_MODULE}.client")
@patch(f"{_MODULE}.sleep")
@patch(f"{_MODULE}._get_column_names", return_value=([], [], "ts", None))
@patch(f"{_MODULE}.client")
@patch(f"{_MODULE}.k8s_config")
def test_async_mode_waits_for_job_to_finish(
    _k8s_config,
    _client,
    _get_column_names,
    mock_sleep,
    _job_client,
    final_status,
    expected,
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

    # Real status parsing: pending, then running, then the final condition list.
    job = KubernetesMaterializationJob("test", "default")
    responses = [V1JobStatus(), V1JobStatus(active=1), final_status]
    job.batch_v1.read_namespaced_job_status.side_effect = lambda *_: V1Job(
        status=responses.pop(0) if len(responses) > 1 else responses[0]
    )
    engine._create_kubernetes_job = MagicMock(return_value=job)
    # Fail instead of looping forever if the final status is not recognized.
    mock_sleep.side_effect = [None, None, AssertionError("job never finished")]

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
    assert jobs == [job]
    assert job.status() == expected
