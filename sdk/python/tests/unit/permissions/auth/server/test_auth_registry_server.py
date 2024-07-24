from datetime import datetime

import assertpy
import pandas as pd
import pytest
import yaml

from feast import FeatureStore
from feast.infra.registry.remote import RemoteRegistryConfig
from feast.permissions.permission import Permission
from feast.registry_server import start_server
from feast.repo_config import RepoConfig
from feast.wait import wait_retry_backoff  # noqa: E402
from tests.unit.permissions.auth.server import mock_utils
from tests.unit.permissions.auth.server.conftest import (
    invalid_list_entities_perm,
    list_entities_perm,
    list_fv_perm,
    list_odfv_perm,
    list_permissions_perm,
    list_sfv_perm,
)
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


def test_registry_apis(
    auth_config,
    temp_dir,
    server_port,
    start_registry_server,
    remote_feature_store,
    applied_permissions,
):
    print(f"Runnning for\n:{auth_config}")
    permissions = _test_list_permissions(remote_feature_store, applied_permissions)
    _test_list_entities(remote_feature_store, applied_permissions)
    _test_list_fvs(remote_feature_store, applied_permissions)

    if _permissions_exist_in_permission_list(
        [
            list_entities_perm,
            list_permissions_perm,
            list_fv_perm,
            list_odfv_perm,
            list_sfv_perm,
        ],
        permissions,
    ):
        _test_get_historical_features(remote_feature_store)


def _test_get_historical_features(client_fs: FeatureStore):
    entity_df = pd.DataFrame.from_dict(
        {
            # entity's join key -> entity values
            "driver_id": [1001, 1002, 1003],
            # "event_timestamp" (reserved key) -> timestamps
            "event_timestamp": [
                datetime(2021, 4, 12, 10, 59, 42),
                datetime(2021, 4, 12, 8, 12, 10),
                datetime(2021, 4, 12, 16, 40, 26),
            ],
            # (optional) label name -> label values. Feast does not process these
            "label_driver_reported_satisfaction": [1, 5, 3],
            # values we're using for an on-demand transformation
            "val_to_add": [1, 2, 3],
            "val_to_add_2": [10, 20, 30],
        }
    )

    training_df = client_fs.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
        ],
    ).to_df()
    assertpy.assert_that(training_df).is_not_none()


def _test_list_entities(client_fs: FeatureStore, permissions: list[Permission]):
    entities = client_fs.list_entities()

    if not _is_auth_enabled(client_fs) or _is_permission_enabled(
        client_fs, permissions, list_entities_perm
    ):
        assertpy.assert_that(entities).is_not_none()
        assertpy.assert_that(len(entities)).is_equal_to(1)
        assertpy.assert_that(entities[0].name).is_equal_to("driver")
    else:
        assertpy.assert_that(entities).is_not_none()
        assertpy.assert_that(len(entities)).is_equal_to(0)


def _no_permission_retrieved(permissions: list[Permission]) -> bool:
    return len(permissions) == 0


def _test_list_permissions(
    client_fs: FeatureStore, applied_permissions: list[Permission]
) -> list[Permission]:
    if _is_auth_enabled(client_fs) and _permissions_exist_in_permission_list(
        [invalid_list_entities_perm], applied_permissions
    ):
        with pytest.raises(Exception):
            client_fs.list_permissions()
        return []
    else:
        permissions = client_fs.list_permissions()

    if not _is_auth_enabled(client_fs):
        assertpy.assert_that(permissions).is_not_none()
        assertpy.assert_that(len(permissions)).is_equal_to(len(applied_permissions))
    elif _is_auth_enabled(client_fs) and _permissions_exist_in_permission_list(
        [
            list_entities_perm,
            list_permissions_perm,
            list_fv_perm,
            list_odfv_perm,
            list_sfv_perm,
        ],
        permissions,
    ):
        assertpy.assert_that(permissions).is_not_none()
        assertpy.assert_that(len(permissions)).is_equal_to(
            len(
                [
                    list_entities_perm,
                    list_permissions_perm,
                    list_fv_perm,
                    list_odfv_perm,
                    list_sfv_perm,
                ]
            )
        )
    elif _is_auth_enabled(client_fs) and _is_listing_permissions_allowed(permissions):
        assertpy.assert_that(permissions).is_not_none()
        assertpy.assert_that(len(permissions)).is_equal_to(1)

    return permissions


def _is_listing_permissions_allowed(permissions: list[Permission]) -> bool:
    return list_permissions_perm in permissions


def _is_auth_enabled(client_fs: FeatureStore) -> bool:
    return client_fs.config.auth_config.type != "no_auth"


def _test_list_fvs(client_fs: FeatureStore, permissions: list[Permission]):
    if _is_auth_enabled(client_fs) and _permissions_exist_in_permission_list(
        [invalid_list_entities_perm], permissions
    ):
        with pytest.raises(Exception):
            client_fs.list_feature_views()
        return []
    else:
        fvs = client_fs.list_feature_views()
        for fv in fvs:
            print(f"{fv.name}, {type(fv).__name__}")

    if not _is_auth_enabled(client_fs) or _is_permission_enabled(
        client_fs, permissions, list_fv_perm
    ):
        assertpy.assert_that(fvs).is_not_none()
        assertpy.assert_that(len(fvs)).is_equal_to(2)

        names = _to_names(fvs)
        assertpy.assert_that(names).contains("driver_hourly_stats")
        assertpy.assert_that(names).contains("driver_hourly_stats_fresh")
    else:
        assertpy.assert_that(fvs).is_not_none()
        assertpy.assert_that(len(fvs)).is_equal_to(0)


def _permissions_exist_in_permission_list(
    permission_to_test: list[Permission], permission_list: list[Permission]
) -> bool:
    return all(e in permission_list for e in permission_to_test)


def _is_permission_enabled(
    client_fs: FeatureStore,
    permissions: list[Permission],
    permission: Permission,
):
    return _is_auth_enabled(client_fs) and (
        _no_permission_retrieved(permissions)
        or (
            _permissions_exist_in_permission_list(
                [list_permissions_perm, permission], permissions
            )
        )
    )


def _to_names(items):
    return [i.name for i in items]
