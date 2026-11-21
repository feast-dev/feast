from unittest.mock import MagicMock, patch

import pytest

from feast.infra.common.materialization_job import MaterializationJobStatus
from feast.infra.compute_engines.flink_application.config import (
    FlinkApplicationComputeEngineConfig,
)
from feast.infra.compute_engines.flink_application.job import (
    _STATE_MAP,
    CompletedMaterializationJob,
    FlinkDeploymentMaterializationJob,
)


def _make_repo_config(
    online_store_type="redis",
    registry_type="sql",
    registry_path="postgresql://user:pass@host:5432/feast",  # pragma: allowlist secret
    offline_store_type="spark",
):
    """Build a mock RepoConfig for testing."""
    config = MagicMock()
    config.project = "test"
    config.online_store = MagicMock()
    config.online_store.type = online_store_type
    config.offline_store = MagicMock()
    config.offline_store.type = offline_store_type
    config.registry = MagicMock()
    config.registry.path = registry_path
    config.registry.registry_type = registry_type
    config.batch_engine = FlinkApplicationComputeEngineConfig(
        image="quay.io/test/feast-flink:latest",
    )
    config.model_dump = MagicMock(
        return_value={
            "project": "test",
            "provider": "local",
            "batch_engine": {"type": "flink_application"},
            "offline_store": {"type": offline_store_type},
            "online_store": {"type": online_store_type},
            "registry": {"registry_type": registry_type, "path": registry_path},
        }
    )
    return config


@patch("feast.infra.compute_engines.operator.compute.k8s_config")
@patch("feast.infra.compute_engines.operator.compute.client")
def _make_engine(mock_client, mock_k8s_config, **kwargs):
    """Create engine with mocked K8s client, pre-seeding API clients."""
    from feast.infra.compute_engines.flink_application.compute import (
        FlinkApplicationComputeEngine,
    )

    repo_config = _make_repo_config(**kwargs)
    engine = FlinkApplicationComputeEngine(
        repo_config=repo_config, offline_store=None, online_store=None
    )
    engine._core_v1 = MagicMock()
    engine._custom_api = MagicMock()
    return engine


# ── Config defaults + required image ──


def test_config_defaults_and_required_image():
    c = FlinkApplicationComputeEngineConfig(image="quay.io/test:v1")
    assert c.type == "flink_application"
    assert c.namespace == "default"
    assert c.flink_version == "v2_2"
    assert c.parallelism == 1
    assert c.upgrade_mode == "stateless"

    with pytest.raises(Exception):
        FlinkApplicationComputeEngineConfig()  # image is required


# ── Store validation is inherited from the operator base ──


def test_rejects_sqlite_online_store():
    with pytest.raises(ValueError, match="sqlite"):
        _make_engine(online_store_type="sqlite")


@pytest.mark.parametrize("store_type", ["dask", "file", "duckdb"])
def test_rejects_file_based_offline_store(store_type):
    with pytest.raises(ValueError, match=store_type):
        _make_engine(offline_store_type=store_type)


def test_rejects_file_registry():
    with pytest.raises(ValueError, match="file.*registry"):
        _make_engine(registry_type="file")


def test_accepts_sql_registry():
    assert _make_engine(registry_type="sql") is not None


@patch("feast.infra.compute_engines.operator.compute.k8s_config")
@patch("feast.infra.compute_engines.operator.compute.client")
def test_init_does_not_load_kubeconfig(mock_client, mock_k8s_config):
    """feast apply constructs the engine but never materializes — no kubeconfig."""
    from feast.infra.compute_engines.flink_application.compute import (
        FlinkApplicationComputeEngine,
    )

    engine = FlinkApplicationComputeEngine(
        repo_config=_make_repo_config(), offline_store=None, online_store=None
    )
    mock_k8s_config.load_config.assert_not_called()
    _ = engine.core_v1
    mock_k8s_config.load_config.assert_called_once()
    _ = engine.custom_api
    mock_k8s_config.load_config.assert_called_once()


# ── Driver repo config rewrites batch_engine to the in-pod engine ──


def test_build_driver_repo_config_rewrites_to_flink_engine():
    engine = _make_engine()
    d = engine._build_driver_repo_config()
    assert d["batch_engine"] == {"type": "flink.engine"}
    assert d["offline_store"]["type"] == "spark"  # NOT rewritten


# ── FlinkDeployment CR structure ──


def test_cr_structure():
    engine = _make_engine()
    cr = engine._build_flink_deployment_cr("abcd1234")
    assert cr["apiVersion"] == "flink.apache.org/v1beta1"
    assert cr["kind"] == "FlinkDeployment"
    assert cr["metadata"]["name"] == "feast-flink-abcd1234"
    assert cr["spec"]["flinkVersion"] == "v2_2"
    job = cr["spec"]["job"]
    assert job["entryClass"] == "org.apache.flink.client.python.PythonDriver"
    assert job["args"] == [
        "-pyclientexec",
        "/usr/bin/python3",
        "-py",
        "/opt/feast/main.py",
    ]
    assert job["upgradeMode"] == "stateless"
    assert cr["spec"]["flinkConfiguration"]["execution.runtime-mode"] == "BATCH"
    assert "jobManager" in cr["spec"]
    assert "taskManager" in cr["spec"]


def test_cr_configmap_env_passthrough():
    engine = _make_engine()
    cr = engine._build_flink_deployment_cr("abcd1234")
    container = cr["spec"]["podTemplate"]["spec"]["containers"][0]
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["FEAST_CONFIGMAP_NAME"] == "feast-flink-abcd1234"
    assert env["FEAST_CONFIGMAP_NAMESPACE"] == "default"
    vol = cr["spec"]["podTemplate"]["spec"]["volumes"][0]
    assert vol["configMap"]["name"] == "feast-flink-abcd1234"


# ── Status mapping ──


def test_state_map_coverage():
    assert len(_STATE_MAP) == 12
    assert _STATE_MAP["FINISHED"] == MaterializationJobStatus.SUCCEEDED
    assert _STATE_MAP["FAILED"] == MaterializationJobStatus.ERROR
    assert _STATE_MAP["CANCELED"] == MaterializationJobStatus.ERROR
    assert _STATE_MAP["RUNNING"] == MaterializationJobStatus.RUNNING
    assert _STATE_MAP[""] == MaterializationJobStatus.WAITING


def test_extract_state_reads_jobstatus():
    job = FlinkDeploymentMaterializationJob("abcd1234", "default", MagicMock())
    obj = {"status": {"jobStatus": {"state": "FINISHED"}}}
    assert job._extract_state(obj) == "FINISHED"


def test_extract_state_falls_back_to_lifecycle_failed():
    """A failure before the job starts has no jobStatus.state but lifecycleState
    FAILED — this must still be reported as terminal."""
    job = FlinkDeploymentMaterializationJob("abcd1234", "default", MagicMock())
    obj = {"status": {"jobStatus": {}, "lifecycleState": "FAILED"}}
    assert job._extract_state(obj) == "FAILED"


def test_extract_error_message_prefers_status_error():
    job = FlinkDeploymentMaterializationJob("abcd1234", "default", MagicMock())
    obj = {"status": {"error": "boom"}}
    assert job._extract_error_message(obj, "FAILED") == "boom"


# ── Job naming ──


def test_job_naming():
    job = FlinkDeploymentMaterializationJob("abcdef12", "default", MagicMock())
    assert job.job_id() == "feast-flink-abcdef12"
    assert job.resource_name == "feast-flink-abcdef12"


def test_completed_job_status():
    job = CompletedMaterializationJob("abc123")
    assert job.status() == MaterializationJobStatus.SUCCEEDED
    assert job.error() is None
    assert job.job_id() == "feast-flink-abc123"
    assert job.should_be_retried() is False


# ── Cleanup swallows 404 (inherited base behavior, Flink CR coordinates) ──


@patch("feast.infra.compute_engines.operator.compute.k8s_config")
@patch("feast.infra.compute_engines.operator.compute.client")
def test_cleanup_swallows_404(mock_client, mock_k8s_config):
    from kubernetes.client.exceptions import ApiException

    from feast.infra.compute_engines.flink_application.compute import (
        FlinkApplicationComputeEngine,
    )

    engine = FlinkApplicationComputeEngine(
        repo_config=_make_repo_config(), offline_store=None, online_store=None
    )
    engine.custom_api.delete_namespaced_custom_object.side_effect = ApiException(
        status=404
    )
    engine.core_v1.delete_namespaced_config_map.side_effect = ApiException(status=404)

    engine._cleanup("test-id")  # must not raise


def test_cleanup_uses_flink_cr_coordinates():
    engine = _make_engine()
    engine._cleanup("test-id")
    args, kwargs = engine.custom_api.delete_namespaced_custom_object.call_args
    assert args[0] == "flink.apache.org"
    assert args[1] == "v1beta1"
    assert args[3] == "flinkdeployments"
    assert args[4] == "feast-flink-test-id"
