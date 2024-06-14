import os
import subprocess
import tempfile
from datetime import datetime
from textwrap import dedent

import pytest

from feast.feature_store import FeatureStore
from feast.wait import wait_retry_backoff
from tests.utils.cli_repo_creator import CliRunner
from tests.utils.http_server import check_port_open, free_port


@pytest.mark.integration
def test_remote_online_store_read():
    with tempfile.TemporaryDirectory() as remote_server_tmp_dir, tempfile.TemporaryDirectory() as remote_client_tmp_dir:
        server_store, server_url, registry_path = (
            _create_server_store_spin_feature_server(temp_dir=remote_server_tmp_dir)
        )
        assert None not in (server_store, server_url, registry_path)
        client_store = _create_remote_client_feature_store(
            temp_dir=remote_client_tmp_dir,
            server_registry_path=str(registry_path),
            feature_server_url=server_url,
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


def _create_server_store_spin_feature_server(temp_dir):
    feast_server_port = free_port()
    store = _default_store(str(temp_dir), "REMOTE_ONLINE_SERVER_PROJECT")
    server_url = next(
        _start_feature_server(
            repo_path=str(store.repo_path), server_port=feast_server_port
        )
    )
    print(f"Server started successfully, {server_url}")
    return store, server_url, os.path.join(store.repo_path, "data", "registry.db")


def _default_store(temp_dir, project_name) -> FeatureStore:
    runner = CliRunner()
    result = runner.run(["init", project_name], cwd=temp_dir)
    repo_path = os.path.join(temp_dir, project_name, "feature_repo")
    assert result.returncode == 0

    result = runner.run(["--chdir", repo_path, "apply"], cwd=temp_dir)
    assert result.returncode == 0

    fs = FeatureStore(repo_path=repo_path)
    fs.materialize_incremental(
        end_date=datetime.utcnow(), feature_views=["driver_hourly_stats"]
    )
    return fs


def _create_remote_client_feature_store(
    temp_dir, server_registry_path: str, feature_server_url: str
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
    )

    result = runner.run(["--chdir", repo_path, "apply"], cwd=temp_dir)
    assert result.returncode == 0

    return FeatureStore(repo_path=repo_path)


def _overwrite_remote_client_feature_store_yaml(
    repo_path: str, registry_path: str, feature_server_url: str
):
    repo_config = os.path.join(repo_path, "feature_store.yaml")
    with open(repo_config, "w") as repo_config:
        repo_config.write(
            dedent(
                f"""
            project: REMOTE_ONLINE_CLIENT_PROJECT
            registry: {registry_path}
            provider: local
            online_store:
                path: {feature_server_url}
                type: remote
            entity_key_serialization_version: 2
            """
            )
        )


def _start_feature_server(repo_path: str, server_port: int):
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
