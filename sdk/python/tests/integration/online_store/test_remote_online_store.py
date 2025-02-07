import logging
import os
import tempfile
from textwrap import dedent

import pytest

from feast import FeatureView, OnDemandFeatureView, StreamFeatureView
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

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.rbac_remote_integration_test
@pytest.mark.parametrize(
    "tls_mode", [("True", "True"), ("True", "False"), ("False", "")], indirect=True
)
def test_remote_online_store_read(auth_config, tls_mode):
    with (
        tempfile.TemporaryDirectory() as remote_server_tmp_dir,
        tempfile.TemporaryDirectory() as remote_client_tmp_dir,
    ):
        permissions_list = [
            Permission(
                name="online_list_fv_perm",
                types=FeatureView,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.READ_ONLINE],
            ),
            Permission(
                name="online_list_odfv_perm",
                types=OnDemandFeatureView,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.READ_ONLINE],
            ),
            Permission(
                name="online_list_sfv_perm",
                types=StreamFeatureView,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.READ_ONLINE],
            ),
        ]
        server_store, server_url, registry_path = (
            _create_server_store_spin_feature_server(
                temp_dir=remote_server_tmp_dir,
                auth_config=auth_config,
                permissions_list=permissions_list,
                tls_mode=tls_mode,
            )
        )
        assert None not in (server_store, server_url, registry_path)

        client_store = _create_remote_client_feature_store(
            temp_dir=remote_client_tmp_dir,
            server_registry_path=str(registry_path),
            feature_server_url=server_url,
            auth_config=auth_config,
            tls_mode=tls_mode,
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
    temp_dir, auth_config: str, permissions_list, tls_mode
):
    store = default_store(str(temp_dir), auth_config, permissions_list)
    feast_server_port = free_port()
    is_tls_mode, tls_key_path, tls_cert_path, ca_trust_store_path = tls_mode

    server_url = next(
        start_feature_server(
            repo_path=str(store.repo_path),
            server_port=feast_server_port,
            tls_key_path=tls_key_path,
            tls_cert_path=tls_cert_path,
            ca_trust_store_path=ca_trust_store_path,
        )
    )
    if is_tls_mode:
        logger.info(
            f"Online Server started successfully in TLS(SSL) mode, {server_url}"
        )
    else:
        logger.info(
            f"Online Server started successfully in Non-TLS(SSL) mode, {server_url}"
        )

    return (
        store,
        server_url,
        os.path.join(store.repo_path, "data", "registry.db"),
    )


def _create_remote_client_feature_store(
    temp_dir,
    server_registry_path: str,
    feature_server_url: str,
    auth_config: str,
    tls_mode,
) -> FeatureStore:
    project_name = "REMOTE_ONLINE_CLIENT_PROJECT"
    runner = CliRunner()
    result = runner.run(["init", project_name], cwd=temp_dir)
    assert result.returncode == 0
    repo_path = os.path.join(temp_dir, project_name, "feature_repo")
    is_tls_mode, _, tls_cert_path, ca_trust_store_path = tls_mode
    if is_tls_mode and not ca_trust_store_path:
        _overwrite_remote_client_feature_store_yaml(
            repo_path=str(repo_path),
            registry_path=server_registry_path,
            feature_server_url=feature_server_url,
            auth_config=auth_config,
            tls_cert_path=tls_cert_path,
        )
    else:
        _overwrite_remote_client_feature_store_yaml(
            repo_path=str(repo_path),
            registry_path=server_registry_path,
            feature_server_url=feature_server_url,
            auth_config=auth_config,
        )

    if is_tls_mode and ca_trust_store_path:
        # configure trust store path only when is_tls_mode and ca_trust_store_path exists.
        os.environ["FEAST_CA_CERT_FILE_PATH"] = ca_trust_store_path

    return FeatureStore(repo_path=repo_path)


def _overwrite_remote_client_feature_store_yaml(
    repo_path: str,
    registry_path: str,
    feature_server_url: str,
    auth_config: str,
    tls_cert_path: str = "",
):
    repo_config = os.path.join(repo_path, "feature_store.yaml")

    config_content = "entity_key_serialization_version: 2\n" + auth_config
    config_content += dedent(
        f"""
    project: {PROJECT_NAME}
    registry: {registry_path}
    provider: local
    online_store:
        path: {feature_server_url}
        type: remote
    """
    )

    if tls_cert_path:
        config_content += f"    cert: {tls_cert_path}\n"

    with open(repo_config, "w") as repo_config_file:
        repo_config_file.write(config_content)
