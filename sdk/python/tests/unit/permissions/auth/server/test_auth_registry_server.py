import assertpy
import pytest
import yaml

from feast import FeatureStore
from feast.infra.registry.remote import RemoteRegistryConfig
from feast.registry_server import start_server
from feast.repo_config import RepoConfig
from feast.wait import wait_retry_backoff  # noqa: E402
from tests.unit.permissions.auth.server import mock_utils
from tests.utils.http_server import check_port_open  # noqa: E402


@pytest.fixture
def start_registry_server(
    request,
    auth_config,
    server_port,
    feature_store,
    monkeypatch,
):
    if "kubernetes" in auth_config:
        mock_utils._mock_kubernetes(request=request, monkeypatch=monkeypatch)
    elif "oidc" in auth_config:
        auth_config_yaml = yaml.safe_load(auth_config)
        mock_utils._mock_oidc(
            request=request,
            monkeypatch=monkeypatch,
            client_id=auth_config_yaml["auth"]["client_id"],
        )

    assertpy.assert_that(server_port).is_not_equal_to(0)

    print(f"Starting Registry at {server_port}")
    server = start_server(feature_store, server_port, wait_for_termination=False)
    print("Waiting server availability")
    wait_retry_backoff(
        lambda: (None, check_port_open("localhost", server_port)),
        timeout_secs=10,
    )
    print("Server started")

    yield server

    print("Stopping server")
    server.stop(grace=None)  # Teardown server


@pytest.fixture
def remote_feature_store(server_port, feature_store):
    registry_config = RemoteRegistryConfig(
        registry_type="remote", path=f"localhost:{server_port}"
    )

    store = FeatureStore(
        config=RepoConfig(
            project=mock_utils.PROJECT_NAME,
            auth=feature_store.config.auth,
            registry=registry_config,
            provider="local",
            entity_key_serialization_version=2,
        )
    )
    return store


def test_remote_offline_store_apis(
    auth_config, temp_dir, server_port, start_registry_server, remote_feature_store
):
    print(f"Runnning for\n:{auth_config}")
    _test_list_entities(remote_feature_store)
    _test_list_fvs(remote_feature_store)


def _test_list_entities(fs: FeatureStore):
    entities = fs.list_entities()

    assertpy.assert_that(entities).is_not_none()
    assertpy.assert_that(len(entities)).is_equal_to(1)

    assertpy.assert_that(entities[0].name).is_equal_to("driver")


def _test_list_fvs(fs: FeatureStore):
    fvs = fs.list_feature_views()
    for fv in fvs:
        print(f"{fv.name}, {type(fv).__name__}")

    assertpy.assert_that(fvs).is_not_none()
    assertpy.assert_that(len(fvs)).is_equal_to(2)

    names = _to_names(fvs)
    assertpy.assert_that(names).contains("driver_hourly_stats")
    assertpy.assert_that(names).contains("driver_hourly_stats_fresh")


def _to_names(items):
    return [i.name for i in items]
