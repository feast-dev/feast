import os
import tempfile
from textwrap import dedent

import pytest

from feast import Entity, FeatureView, OnDemandFeatureView, StreamFeatureView
from feast.feature_store import FeatureStore
from feast.permissions.action import AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from tests.utils.auth_permissions_util import (
    PROJECT_NAME,
    default_store,
    start_feature_server,
)
from tests.utils.cli_repo_creator import CliRunner
from tests.utils.http_server import free_port


@pytest.mark.integration
def test_remote_online_store_read(auth_config):
    with tempfile.TemporaryDirectory() as remote_server_tmp_dir, tempfile.TemporaryDirectory() as remote_client_tmp_dir:
        permissions_list = [
            Permission(
                name="online_list_entities_perm",
                types=Entity,
                with_subclasses=False,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
            ),
            Permission(
                name="online_list_permissions_perm",
                types=Permission,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
            ),
            Permission(
                name="online_list_fv_perm",
                types=FeatureView,
                with_subclasses=False,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
            ),
            Permission(
                name="online_list_odfv_perm",
                types=OnDemandFeatureView,
                with_subclasses=False,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
            ),
            Permission(
                name="online_list_sfv_perm",
                types=StreamFeatureView,
                with_subclasses=False,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
            ),
        ]
        server_store, server_url, registry_path = (
            _create_server_store_spin_feature_server(
                temp_dir=remote_server_tmp_dir,
                auth_config=auth_config,
                permissions_list=permissions_list,
            )
        )
        assert None not in (server_store, server_url, registry_path)
        client_store = _create_remote_client_feature_store(
            temp_dir=remote_client_tmp_dir,
            server_registry_path=str(registry_path),
            feature_server_url=server_url,
            auth_config=auth_config,
        )
        assert client_store is not None
        _assert_non_existing_entity_feature_views_entity(
            client_store=client_store, server_store=server_store
        )
        _assert_existing_feature_views_entity(
            client_store=client_store, server_store=server_store
        )
        _assert_non_existing_feature_views(
            client_store=client_store, server_store=server_store
        )


def _assert_non_existing_entity_feature_views_entity(
    client_store: FeatureStore, server_store: FeatureStore
):
    features = [
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ]

    entity_rows = [{"driver_id": 1234}]
    _assert_client_server_online_stores_are_matching(
        client_store=client_store,
        server_store=server_store,
        features=features,
        entity_rows=entity_rows,
    )


def _assert_non_existing_feature_views(
    client_store: FeatureStore, server_store: FeatureStore
):
    features = [
        "driver_hourly_stats1:conv_rate",
        "driver_hourly_stats1:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ]

    entity_rows = [{"driver_id": 1001}, {"driver_id": 1002}]

    with pytest.raises(
        Exception, match="Feature view driver_hourly_stats1 does not exist"
    ):
        client_store.get_online_features(
            features=features, entity_rows=entity_rows
        ).to_dict()

    with pytest.raises(
        Exception, match="Feature view driver_hourly_stats1 does not exist"
    ):
        server_store.get_online_features(
            features=features, entity_rows=entity_rows
        ).to_dict()


def _assert_existing_feature_views_entity(
    client_store: FeatureStore, server_store: FeatureStore
):
    features = [
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ]

    entity_rows = [{"driver_id": 1001}, {"driver_id": 1002}]
    _assert_client_server_online_stores_are_matching(
        client_store=client_store,
        server_store=server_store,
        features=features,
        entity_rows=entity_rows,
    )

    features = ["driver_hourly_stats:conv_rate"]
    _assert_client_server_online_stores_are_matching(
        client_store=client_store,
        server_store=server_store,
        features=features,
        entity_rows=entity_rows,
    )


def _assert_client_server_online_stores_are_matching(
    client_store: FeatureStore,
    server_store: FeatureStore,
    features: list[str],
    entity_rows: list,
):
    online_features_from_client = client_store.get_online_features(
        features=features, entity_rows=entity_rows
    ).to_dict()

    assert online_features_from_client is not None

    online_features_from_server = server_store.get_online_features(
        features=features, entity_rows=entity_rows
    ).to_dict()

    assert online_features_from_server is not None
    assert online_features_from_client is not None
    assert online_features_from_client == online_features_from_server


def _create_server_store_spin_feature_server(
    temp_dir, auth_config: str, permissions_list
):
    store = default_store(str(temp_dir), auth_config, permissions_list)
    feast_server_port = free_port()
    server_url = next(
        start_feature_server(
            repo_path=str(store.repo_path), server_port=feast_server_port
        )
    )
    print(f"Server started successfully, {server_url}")
    return store, server_url, os.path.join(store.repo_path, "data", "registry.db")


def _create_remote_client_feature_store(
    temp_dir, server_registry_path: str, feature_server_url: str, auth_config: str
) -> FeatureStore:
    project_name = "REMOTE_ONLINE_CLIENT_PROJECT"
    runner = CliRunner()
    result = runner.run(["init", project_name], cwd=temp_dir)
    assert result.returncode == 0
    repo_path = os.path.join(temp_dir, project_name, "feature_repo")
    _overwrite_remote_client_feature_store_yaml(
        repo_path=str(repo_path),
        registry_path=server_registry_path,
        feature_server_url=feature_server_url,
        auth_config=auth_config,
    )

    result = runner.run(["--chdir", repo_path, "apply"], cwd=temp_dir)
    assert result.returncode == 0

    return FeatureStore(repo_path=repo_path)


def _overwrite_remote_client_feature_store_yaml(
    repo_path: str, registry_path: str, feature_server_url: str, auth_config: str
):
    repo_config = os.path.join(repo_path, "feature_store.yaml")
    with open(repo_config, "w") as repo_config:
        repo_config.write(
            dedent(
                f"""
            project: {PROJECT_NAME}
            registry: {registry_path}
            provider: local
            online_store:
                path: {feature_server_url}
                type: remote
            entity_key_serialization_version: 2
            """
            )
            + auth_config
        )


def _start_feature_server(repo_path: str, server_port: int, metrics: bool = False):
    host = "0.0.0.0"
    cmd = [
        "feast",
        "-c" + repo_path,
        "serve",
        "--host",
        host,
        "--port",
        str(server_port),
    ]
    feast_server_process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    _time_out_sec: int = 60
    # Wait for server to start
    wait_retry_backoff(
        lambda: (None, check_port_open(host, server_port)),
        timeout_secs=_time_out_sec,
        timeout_msg=f"Unable to start the feast server in {_time_out_sec} seconds for remote online store type, port={server_port}",
    )

    if metrics:
        cmd.append("--metrics")

    # Check if metrics are enabled and Prometheus server is running
    if metrics:
        wait_retry_backoff(
            lambda: (None, check_port_open("localhost", 8000)),
            timeout_secs=_time_out_sec,
            timeout_msg="Unable to start the Prometheus server in 60 seconds.",
        )
    else:
        assert not check_port_open(
            "localhost", 8000
        ), "Prometheus server is running when it should be disabled."

    yield f"http://localhost:{server_port}"

    if feast_server_process is not None:
        feast_server_process.kill()

        # wait server to free the port
        wait_retry_backoff(
            lambda: (
                None,
                not check_port_open("localhost", server_port),
            ),
            timeout_msg=f"Unable to stop the feast server in {_time_out_sec} seconds for remote online store type, port={server_port}",
            timeout_secs=_time_out_sec,
        )
