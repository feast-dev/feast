from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.compute_engines.spark_application.config import (
    SparkApplicationComputeEngineConfig,
)
from feast.infra.compute_engines.spark_application.job import (
    SparkApplicationMaterializationJob,
    _STATE_MAP,
)


def _make_repo_config(
    online_store_type="redis",
    registry_path="s3://bucket/registry.db",
    registry_address=None,
    offline_store_type="dask",
    spark_conf=None,
):
    """Build a mock RepoConfig for testing."""
    config = MagicMock()
    config.online_store = MagicMock()
    config.online_store.type = online_store_type
    config.registry = MagicMock()
    config.registry.path = registry_path
    config.registry.registry_type = "file"
    config.batch_engine = SparkApplicationComputeEngineConfig(
        image="quay.io/test/feast-spark:latest",
        registry_address=registry_address,
        spark_conf=spark_conf,
    )
    config.model_dump = MagicMock(return_value={
        "project": "test",
        "provider": "local",
        "batch_engine": {"type": "spark_application"},
        "offline_store": {"type": offline_store_type, "spark_conf": {"spark.existing": "value"}},
        "online_store": {"type": online_store_type},
        "registry": {"registry_type": "file", "path": registry_path},
    })
    return config


@patch("feast.infra.compute_engines.spark_application.compute.k8s_config")
@patch("feast.infra.compute_engines.spark_application.compute.client")
def _make_engine(mock_client, mock_k8s_config, **kwargs):
    """Create engine with mocked K8s client."""
    from feast.infra.compute_engines.spark_application.compute import (
        SparkApplicationComputeEngine,
    )
    repo_config = _make_repo_config(**kwargs)
    return SparkApplicationComputeEngine(
        repo_config=repo_config, offline_store=None, online_store=None
    )


# ── Test 1: Config defaults + required field ──

def test_config_defaults_and_required_image():
    c = SparkApplicationComputeEngineConfig(image="quay.io/test:v1")
    assert c.type == "spark_application"
    assert c.namespace == "default"
    assert c.executor_instances == 1
    assert c.restart_policy == "Never"
    assert c.max_retries == 3

    with pytest.raises(Exception):
        SparkApplicationComputeEngineConfig()  # image is required


# ── Test 2: EC-3 rejects SQLite ──

def test_rejects_sqlite_online_store():
    with pytest.raises(ValueError, match="SQLite"):
        _make_engine(online_store_type="sqlite")


# ── Test 3: EC-2 rejects local registry without registry_address ──

def test_rejects_local_registry_without_address():
    with pytest.raises(ValueError, match="local file"):
        _make_engine(registry_path="/local/registry.db")


# ── Test 4: EC-2 accepts local registry WITH registry_address ──

def test_accepts_local_registry_with_address():
    engine = _make_engine(registry_path="/local/registry.db", registry_address="feast:6570")
    assert engine is not None


# ── Test 5: _build_driver_repo_config — two rewrites (batch_engine + registry) ──

def test_build_driver_repo_config_rewrites():
    engine = _make_engine(
        offline_store_type="dask",
        registry_address="feast-server:6566",
    )
    d = engine._build_driver_repo_config()
    assert d["batch_engine"]["type"] == "spark.engine"
    assert d["offline_store"]["type"] == "dask"  # NOT rewritten — respects user intent
    assert d["registry"] == {"registry_type": "remote", "path": "feast-server:6566"}


# ── Test 6: _build_driver_repo_config — spark_conf placed in batch_engine ──

def test_build_driver_repo_config_spark_conf():
    engine = _make_engine(spark_conf={"spark.new": "from_engine", "spark.existing": "from_engine"})
    d = engine._build_driver_repo_config()
    assert d["batch_engine"]["spark_conf"]["spark.new"] == "from_engine"
    assert d["batch_engine"]["spark_conf"]["spark.existing"] == "from_engine"
    # offline_store spark_conf left untouched
    assert d["offline_store"]["spark_conf"]["spark.existing"] == "value"


# ── Test 7: _build_driver_repo_config — no registry rewrite without address ──

def test_build_driver_repo_config_no_registry_rewrite():
    engine = _make_engine(registry_address=None)
    d = engine._build_driver_repo_config()
    assert d["registry"] == {"registry_type": "file", "path": "s3://bucket/registry.db"}


# ── Test 8: CR structure ──

def test_cr_structure():
    engine = _make_engine()
    cr = engine._build_spark_application_cr("abcd1234")
    assert cr["apiVersion"] == "sparkoperator.k8s.io/v1beta2"
    assert cr["kind"] == "SparkApplication"
    assert cr["spec"]["type"] == "Python"
    assert cr["spec"]["mode"] == "cluster"
    assert cr["spec"]["mainApplicationFile"] == "local:///opt/feast/main.py"
    assert "driver" in cr["spec"]
    assert "executor" in cr["spec"]
    assert cr["metadata"]["name"] == "feast-sa-abcd1234"


# ── Test 9: Status mapping covers all 14 states ──

def test_state_map_coverage():
    assert len(_STATE_MAP) == 14
    assert _STATE_MAP["COMPLETED"] == MaterializationJobStatus.SUCCEEDED
    assert _STATE_MAP["FAILED"] == MaterializationJobStatus.ERROR
    assert _STATE_MAP["SUBMISSION_FAILED"] == MaterializationJobStatus.ERROR
    assert _STATE_MAP["RUNNING"] == MaterializationJobStatus.RUNNING
    assert _STATE_MAP[""] == MaterializationJobStatus.WAITING


# ── Test 10: Cleanup swallows 404 ──

@patch("feast.infra.compute_engines.spark_application.compute.k8s_config")
@patch("feast.infra.compute_engines.spark_application.compute.client")
def test_cleanup_swallows_404(mock_client, mock_k8s_config):
    from kubernetes.client.exceptions import ApiException
    from feast.infra.compute_engines.spark_application.compute import (
        SparkApplicationComputeEngine,
    )

    repo_config = _make_repo_config()
    engine = SparkApplicationComputeEngine(
        repo_config=repo_config, offline_store=None, online_store=None
    )

    # Mock both delete calls to raise 404
    engine.custom_api.delete_namespaced_custom_object.side_effect = ApiException(status=404)
    engine.core_v1.delete_namespaced_secret.side_effect = ApiException(status=404)

    # Should not raise
    engine._cleanup("test-id")


# ── Test 11: Timeout sets error on job (does not raise) ──

@patch("feast.infra.compute_engines.spark_application.compute.k8s_config")
@patch("feast.infra.compute_engines.spark_application.compute.client")
@patch("feast.infra.compute_engines.spark_application.compute.time")
def test_timeout_sets_error(mock_time, mock_client, mock_k8s_config):
    from feast.infra.compute_engines.spark_application.compute import (
        SparkApplicationComputeEngine,
    )

    repo_config = _make_repo_config()
    repo_config.batch_engine = SparkApplicationComputeEngineConfig(
        image="test", job_timeout_seconds=1, poll_interval_seconds=1,
    )
    engine = SparkApplicationComputeEngine(
        repo_config=repo_config, offline_store=None, online_store=None
    )

    # Calls: start(0), while-check(0), elapsed(0), sleep, while-check(2 > deadline=1) → exit
    mock_time.monotonic.side_effect = [0, 0, 0, 2]
    mock_time.sleep = MagicMock()

    mock_job = MagicMock()
    mock_job.status.return_value = MaterializationJobStatus.RUNNING
    mock_job._job_id = "test123"
    mock_job._error = None
    mock_job.job_id.return_value = "feast-sa-test123"

    engine._wait_for_completion(mock_job)
    assert mock_job._error is not None
    assert "did not complete" in str(mock_job._error)


# ── Test 12: Job naming < 63 chars ──

def test_job_naming_under_63_chars():
    mock_api = MagicMock()
    job = SparkApplicationMaterializationJob("abcdef12", "default", mock_api)
    assert len(job.job_id()) <= 63
    assert job.job_id() == "feast-sa-abcdef12"
