from unittest.mock import MagicMock, patch

import pytest

from feast.feature_view import FeatureViewState
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
)
from feast.infra.compute_engines.spark_application.config import (
    SparkApplicationComputeEngineConfig,
)
from feast.infra.compute_engines.spark_application.job import (
    _STATE_MAP,
    CompletedMaterializationJob,
    SparkApplicationMaterializationJob,
)


def _make_repo_config(
    online_store_type="redis",
    registry_type="sql",
    registry_path="postgresql://user:pass@host:5432/feast",  # pragma: allowlist secret
    offline_store_type="spark",
    spark_conf=None,
):
    """Build a mock RepoConfig for testing."""
    config = MagicMock()
    config.online_store = MagicMock()
    config.online_store.type = online_store_type
    config.offline_store = MagicMock()
    config.offline_store.type = offline_store_type
    config.registry = MagicMock()
    config.registry.path = registry_path
    config.registry.registry_type = registry_type
    config.batch_engine = SparkApplicationComputeEngineConfig(
        image="quay.io/test/feast-spark:latest",
        spark_conf=spark_conf,
    )
    config.model_dump = MagicMock(
        return_value={
            "project": "test",
            "provider": "local",
            "batch_engine": {"type": "spark_application"},
            "offline_store": {
                "type": offline_store_type,
                "spark_conf": {"spark.existing": "value"},
            },
            "online_store": {"type": online_store_type},
            "registry": {"registry_type": registry_type, "path": registry_path},
        }
    )
    return config


@patch("feast.infra.compute_engines.spark_application.compute.k8s_config")
@patch("feast.infra.compute_engines.spark_application.compute.client")
def _make_engine(mock_client, mock_k8s_config, **kwargs):
    """Create engine with mocked K8s client.

    Pre-seed API clients so later property access does not call real
    load_config() after this helper's patches have exited.
    """
    from feast.infra.compute_engines.spark_application.compute import (
        SparkApplicationComputeEngine,
    )

    repo_config = _make_repo_config(**kwargs)
    engine = SparkApplicationComputeEngine(
        repo_config=repo_config, offline_store=None, online_store=None
    )
    engine._core_v1 = MagicMock()
    engine._custom_api = MagicMock()
    return engine


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


# ── Test 2: EC-3 rejects file-based online stores ──


def test_rejects_sqlite_online_store():
    with pytest.raises(ValueError, match="sqlite"):
        _make_engine(online_store_type="sqlite")


def test_rejects_faiss_online_store():
    with pytest.raises(ValueError, match="faiss"):
        _make_engine(online_store_type="faiss")


# ── Test 2b: rejects file-based offline stores ──


@pytest.mark.parametrize("store_type", ["dask", "file", "duckdb"])
def test_rejects_file_based_offline_store(store_type):
    with pytest.raises(ValueError, match=store_type):
        _make_engine(offline_store_type=store_type)


# ── Test 3: EC-2 rejects file-based registries ──


def test_rejects_file_registry():
    with pytest.raises(ValueError, match="file.*registry"):
        _make_engine(registry_type="file")


# ── Test 4: accepts network-accessible registries ──


def test_accepts_sql_registry():
    engine = _make_engine(registry_type="sql")
    assert engine is not None


def test_accepts_snowflake_registry():
    engine = _make_engine(registry_type="snowflake.registry")
    assert engine is not None


@patch("feast.infra.compute_engines.spark_application.compute.k8s_config")
@patch("feast.infra.compute_engines.spark_application.compute.client")
def test_init_does_not_load_kubeconfig(mock_client, mock_k8s_config):
    """feast apply constructs the engine but never materializes — no kubeconfig needed."""
    from feast.infra.compute_engines.spark_application.compute import (
        SparkApplicationComputeEngine,
    )

    engine = SparkApplicationComputeEngine(
        repo_config=_make_repo_config(), offline_store=None, online_store=None
    )
    mock_k8s_config.load_config.assert_not_called()

    _ = engine.core_v1
    mock_k8s_config.load_config.assert_called_once()
    assert engine.custom_api is not None
    # Second access must not reload
    _ = engine.custom_api
    mock_k8s_config.load_config.assert_called_once()


# ── Test 5: _build_driver_repo_config — one rewrite (batch_engine only) ──


def test_build_driver_repo_config_rewrites():
    engine = _make_engine(offline_store_type="spark")
    d = engine._build_driver_repo_config()
    assert d["batch_engine"]["type"] == "spark.engine"
    assert d["offline_store"]["type"] == "spark"  # NOT rewritten — respects user intent
    assert (
        d["registry"]["registry_type"] == "sql"
    )  # NOT rewritten — pod uses SQL directly


# ── Test 6: _build_driver_repo_config — batch_engine is just type (no spark_conf copy) ──


def test_build_driver_repo_config_batch_engine_minimal():
    engine = _make_engine(spark_conf={"spark.new": "from_engine"})
    d = engine._build_driver_repo_config()
    assert d["batch_engine"] == {"type": "spark.engine"}
    assert d["offline_store"]["spark_conf"]["spark.existing"] == "value"


# ── Test 7: CR structure ──


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


# ── Test 8: CR sparkConf includes driver env passthrough ──


def test_cr_driver_env_passthrough():
    engine = _make_engine()
    cr = engine._build_spark_application_cr("abcd1234")
    spark_conf = cr["spec"]["sparkConf"]
    assert (
        spark_conf["spark.kubernetes.driverEnv.FEAST_CONFIGMAP_NAME"]
        == "feast-sa-abcd1234"
    )
    assert (
        spark_conf["spark.kubernetes.driverEnv.FEAST_CONFIGMAP_NAMESPACE"] == "default"
    )


# ── Test 9: Status mapping covers all 14 states ──


def test_state_map_coverage():
    assert len(_STATE_MAP) == 14
    assert _STATE_MAP["COMPLETED"] == MaterializationJobStatus.SUCCEEDED
    assert _STATE_MAP["FAILED"] == MaterializationJobStatus.ERROR
    assert _STATE_MAP["SUBMISSION_FAILED"] == MaterializationJobStatus.ERROR
    assert _STATE_MAP["RUNNING"] == MaterializationJobStatus.RUNNING
    assert _STATE_MAP[""] == MaterializationJobStatus.WAITING
    assert _STATE_MAP["UNKNOWN"] == MaterializationJobStatus.WAITING


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

    engine.custom_api.delete_namespaced_custom_object.side_effect = ApiException(
        status=404
    )
    engine.core_v1.delete_namespaced_config_map.side_effect = ApiException(status=404)

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
        image="test",
        job_timeout_seconds=1,
        poll_interval_seconds=1,
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


# ── Test 13: _build_per_fv_jobs — all succeeded ──


def test_build_per_fv_jobs_all_succeeded():
    engine = _make_engine()
    mock_registry = MagicMock()

    fv1 = MagicMock()
    fv1.name = "fv_1"
    fv1.state = FeatureViewState.AVAILABLE_ONLINE
    fv2 = MagicMock()
    fv2.name = "fv_2"
    fv2.state = FeatureViewState.AVAILABLE_ONLINE
    mock_registry.get_feature_view.side_effect = [fv1, fv2]

    task1 = MagicMock()
    task1.feature_view.name = "fv_1"
    task1.project = "test"
    task2 = MagicMock()
    task2.feature_view.name = "fv_2"
    task2.project = "test"

    parent_job = SparkApplicationMaterializationJob("job1", "default", MagicMock())
    jobs = engine._build_per_fv_jobs(mock_registry, [task1, task2], "job1", parent_job)

    assert len(jobs) == 2
    assert all(isinstance(j, CompletedMaterializationJob) for j in jobs)
    assert all(j.status() == MaterializationJobStatus.SUCCEEDED for j in jobs)


# ── Test 14: _build_per_fv_jobs — partial failure (independent jobs) ──


def test_build_per_fv_jobs_partial_failure():
    engine = _make_engine()
    mock_registry = MagicMock()

    fv_ok = MagicMock()
    fv_ok.name = "fv_ok"
    fv_ok.state = FeatureViewState.AVAILABLE_ONLINE
    fv_fail = MagicMock()
    fv_fail.name = "fv_fail"
    fv_fail.state = FeatureViewState.MATERIALIZING
    mock_registry.get_feature_view.side_effect = [fv_ok, fv_fail]

    task_ok = MagicMock()
    task_ok.feature_view.name = "fv_ok"
    task_ok.project = "test"
    task_fail = MagicMock()
    task_fail.feature_view.name = "fv_fail"
    task_fail.project = "test"

    parent_job = SparkApplicationMaterializationJob("job1", "default", MagicMock())
    jobs = engine._build_per_fv_jobs(
        mock_registry, [task_ok, task_fail], "job1", parent_job
    )

    assert len(jobs) == 2
    assert isinstance(jobs[0], CompletedMaterializationJob)
    assert jobs[0].status() == MaterializationJobStatus.SUCCEEDED
    assert jobs[1].status() == MaterializationJobStatus.ERROR
    assert "fv_fail" in str(jobs[1].error())


# ── Test 15: _build_per_fv_jobs — single task returns parent job directly ──


def test_build_per_fv_jobs_single_task():
    engine = _make_engine()
    mock_registry = MagicMock()
    task = MagicMock()
    task.feature_view.name = "fv_1"
    task.project = "test"

    parent_job = SparkApplicationMaterializationJob("job1", "default", MagicMock())
    jobs = engine._build_per_fv_jobs(mock_registry, [task], "job1", parent_job)

    assert len(jobs) == 1
    assert jobs[0] is parent_job
    mock_registry.get_feature_view.assert_not_called()


# ── Test 16: CompletedMaterializationJob is always SUCCEEDED ──


def test_completed_job_status():
    job = CompletedMaterializationJob("abc123")
    assert job.status() == MaterializationJobStatus.SUCCEEDED
    assert job.error() is None
    assert job.job_id() == "feast-sa-abc123"
    assert job.should_be_retried() is False


# ── Test 17: Env validation — requires value or valueFrom ──


def test_env_requires_value_or_valuefrom():
    with pytest.raises(ValueError, match="'value' or 'valueFrom'"):
        SparkApplicationComputeEngineConfig(
            image="test:v1",
            env=[{"name": "FOO"}],
        )


def test_env_accepts_value():
    c = SparkApplicationComputeEngineConfig(
        image="test:v1",
        env=[{"name": "FOO", "value": "bar"}],
    )
    assert len(c.env) == 1


def test_env_accepts_valuefrom():
    c = SparkApplicationComputeEngineConfig(
        image="test:v1",
        env=[
            {"name": "SECRET", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}}
        ],
    )
    assert len(c.env) == 1


def test_env_rejects_non_dict():
    with pytest.raises(Exception, match="dict"):
        SparkApplicationComputeEngineConfig(
            image="test:v1",
            env=["not_a_dict"],
        )


# ── Test 18: Retry — 403 fails fast with RBAC hint ──


def test_poll_403_fails_fast_with_hint():
    from kubernetes.client.exceptions import ApiException

    mock_api = MagicMock()
    mock_api.get_namespaced_custom_object.side_effect = ApiException(
        status=403, reason="Forbidden"
    )

    job = SparkApplicationMaterializationJob("test1", "default", mock_api)
    status = job.status()

    assert status == MaterializationJobStatus.ERROR
    assert "Role/RoleBinding" in str(job.error())
    mock_api.get_namespaced_custom_object.assert_called_once()
