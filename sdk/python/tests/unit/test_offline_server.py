import contextlib
import json
import os
import subprocess
import sys
import textwrap
from unittest.mock import MagicMock, mock_open, patch

import assertpy
import pyarrow as pa
import pyarrow.flight as fl
import pytest

from feast.infra.offline_stores.remote import (
    RemoteOfflineStore,
    RemoteOfflineStoreConfig,
    _create_retrieval_metadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.offline_server import (
    OfflineServer,
    _configure_grpc_fips,
    _is_fips_enabled,
)
from feast.permissions.security_manager import (
    SecurityManager,
    get_security_manager,
    no_security_manager,
    set_security_manager,
)


def test_create_retrieval_metadata_with_sql_string():
    """SQL string entity_df should produce a stub with empty keys and no timestamps."""
    sql = "SELECT driver_id, event_timestamp FROM driver_stats"
    metadata = _create_retrieval_metadata(
        feature_refs=["driver_hourly_stats:conv_rate"], entity_df=sql
    )
    assertpy.assert_that(metadata.features).is_equal_to(
        ["driver_hourly_stats:conv_rate"]
    )
    assertpy.assert_that(list(metadata.keys)).is_empty()
    assertpy.assert_that(metadata.min_event_timestamp).is_none()
    assertpy.assert_that(metadata.max_event_timestamp).is_none()


def test_remote_offline_store_sql_entity_df_routing():
    """RemoteOfflineStore.get_historical_features moves SQL into api_parameters."""
    sql = "SELECT driver_id, event_timestamp FROM driver_stats"

    mock_client = MagicMock()
    with patch(
        "feast.infra.offline_stores.remote.build_arrow_flight_client",
        return_value=mock_client,
    ):
        job = RemoteOfflineStore.get_historical_features(
            config=MagicMock(
                offline_store=RemoteOfflineStoreConfig(
                    type="remote", host="localhost", port=8815
                ),
                auth_config=MagicMock(type="no_auth"),
            ),
            feature_views=[],
            feature_refs=["driver_hourly_stats:conv_rate"],
            entity_df=sql,
            registry=MagicMock(),
            project="test",
            full_feature_names=False,
        )

    assertpy.assert_that(job.entity_df).is_none()
    assertpy.assert_that(job.api_parameters).contains_key("entity_df_sql")
    assertpy.assert_that(job.api_parameters["entity_df_sql"]).is_equal_to(sql)


def test_offline_server_get_historical_features_passes_sql_to_store():
    """OfflineServer forwards entity_df_sql to the backing offline store."""
    sql = "SELECT driver_id, event_timestamp FROM driver_stats"

    mock_job = MagicMock()
    mock_offline_store = MagicMock()
    mock_offline_store.get_historical_features.return_value = mock_job

    mock_store = MagicMock()
    mock_store.config.project = "test"

    server = MagicMock(spec=OfflineServer)
    server.offline_store = mock_offline_store
    server.store = mock_store
    server.flights = {}
    server.list_feature_views_by_name.return_value = []

    command = {
        "api": "get_historical_features",
        "command_id": "abc",
        "feature_view_names": [],
        "name_aliases": [],
        "feature_refs": ["driver_hourly_stats:conv_rate"],
        "project": "test",
        "full_feature_names": False,
        "entity_df_sql": sql,
    }

    result = OfflineServer.get_historical_features(server, command, key=None)

    assertpy.assert_that(result).is_equal_to(mock_job)
    _, kwargs = mock_offline_store.get_historical_features.call_args
    assertpy.assert_that(kwargs["entity_df"]).is_equal_to(sql)


def test_is_fips_enabled_returns_true():
    with patch("builtins.open", mock_open(read_data="1\n")):
        assert _is_fips_enabled() is True


def test_is_fips_enabled_returns_false():
    with patch("builtins.open", mock_open(read_data="0\n")):
        assert _is_fips_enabled() is False


def test_is_fips_enabled_missing_file():
    with patch("builtins.open", side_effect=FileNotFoundError):
        assert _is_fips_enabled() is False


def test_configure_grpc_fips_sets_cipher_suites():
    with (
        patch("feast.offline_server._is_fips_enabled", return_value=True),
        patch.dict(os.environ, {}, clear=False),
    ):
        os.environ.pop("GRPC_SSL_CIPHER_SUITES", None)
        _configure_grpc_fips()
        assert "GRPC_SSL_CIPHER_SUITES" in os.environ
        assert "AES128-GCM-SHA256" in os.environ["GRPC_SSL_CIPHER_SUITES"]
        del os.environ["GRPC_SSL_CIPHER_SUITES"]


def test_configure_grpc_fips_respects_existing_env():
    with (
        patch("feast.offline_server._is_fips_enabled", return_value=True),
        patch.dict(os.environ, {"GRPC_SSL_CIPHER_SUITES": "custom"}, clear=False),
    ):
        _configure_grpc_fips()
        assert os.environ["GRPC_SSL_CIPHER_SUITES"] == "custom"


def test_configure_grpc_fips_noop_without_fips():
    with (
        patch("feast.offline_server._is_fips_enabled", return_value=False),
        patch.dict(os.environ, {}, clear=False),
    ):
        os.environ.pop("GRPC_SSL_CIPHER_SUITES", None)
        _configure_grpc_fips()
        assert "GRPC_SSL_CIPHER_SUITES" not in os.environ


def test_module_level_fips_sets_env_before_pyarrow_import() -> None:
    """GRPC_SSL_CIPHER_SUITES must be set at module load time,
    before pyarrow.flight (which bundles gRPC) is imported.

    Uses a subprocess so pyarrow.flight is not already cached in
    sys.modules, which lets us verify the true import ordering.
    """
    script = textwrap.dedent("""\
        import faulthandler, io, os, sys

        # Capture a stalled import before the parent terminates this process.
        faulthandler.dump_traceback_later(120)

        # Intercept only /proc/sys/crypto/fips_enabled to simulate FIPS
        _real_open = open
        def _fips_open(file, *args, **kwargs):
            if str(file) == "/proc/sys/crypto/fips_enabled":
                return io.StringIO("1\\n")
            return _real_open(file, *args, **kwargs)

        import builtins
        builtins.open = _fips_open

        # Track import order to verify env var is set before pyarrow.flight
        original_import = builtins.__import__
        def tracking_import(name, *args, **kwargs):
            if name == "pyarrow.flight":
                assert "GRPC_SSL_CIPHER_SUITES" in os.environ, (
                    "GRPC_SSL_CIPHER_SUITES not set before pyarrow.flight import"
                )
            return original_import(name, *args, **kwargs)

        builtins.__import__ = tracking_import
        try:
            import feast.offline_server
            assert "GRPC_SSL_CIPHER_SUITES" in os.environ
            assert "AES128-GCM-SHA256" in os.environ["GRPC_SSL_CIPHER_SUITES"]
        finally:
            faulthandler.cancel_dump_traceback_later()
            builtins.__import__ = original_import
            builtins.open = _real_open
    """)
    env = os.environ.copy()
    env.pop("GRPC_SSL_CIPHER_SUITES", None)
    try:
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            env=env,
            # Cold imports can be slow on macOS under parallel test load.
            timeout=180,
        )
    except subprocess.TimeoutExpired as exc:
        # TimeoutExpired captures bytes even when subprocess.run uses text=True.
        stdout = (exc.stdout or b"").decode(errors="replace")
        stderr = (exc.stderr or b"").decode(errors="replace")
        pytest.fail(
            f"Subprocess timed out after {exc.timeout}s:\n"
            f"stdout: {stdout}\nstderr: {stderr}",
            pytrace=False,
        )
    assert result.returncode == 0, (
        f"Subprocess failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


_SERVER_HOME_PROJECT = "the_project_the_server_was_started_from"


def _server_for(command):
    """Build a mocked OfflineServer plus the flight key for a command."""
    key = ("command_id", json.dumps(command))
    server = MagicMock(spec=OfflineServer)
    server.store = MagicMock()
    server.store.set_current_project.return_value = "project-token"
    server.flights = {key: MagicMock()}
    return server, key


@contextlib.contextmanager
def _recording_security_manager(seen):
    """
    Install a SecurityManager whose registry records the project every permission
    lookup resolves to.

    Asserting on `store.set_current_project` alone would not show whether the check is
    scoped: the store and the SecurityManager hold separate ContextVars, and
    `SecurityManager.permissions` reads its own. Recording the project the registry is
    asked for is the thing the fix is actually about.
    """
    registry = MagicMock(spec=BaseRegistry)
    registry.list_permissions.side_effect = lambda project=None, **kwargs: (
        seen.append(project) or []
    )
    sm = SecurityManager(project=_SERVER_HOME_PROJECT, registry=registry)
    set_security_manager(sm)
    try:
        yield sm
    finally:
        no_security_manager()


def _permission_check():
    """Stand in for the assert_permissions call every real handler makes."""
    get_security_manager().permissions


def test_do_get_scopes_the_permission_lookup_to_the_requested_project():
    """
    The permission list is loaded per project, so it has to follow the project the
    request names. Otherwise a caller reaches every project the server serves through
    whichever project the server itself was started from.
    """
    command = {"api": "get_historical_features", "project": "project_b"}
    server, key = _server_for(command)
    seen = []

    def handler(*args, **kwargs):
        _permission_check()
        result = MagicMock()
        result.to_arrow.return_value = pa.table({"a": [1]})
        return result

    server.get_historical_features.side_effect = handler

    # do_get is wrapped by inject_user_details_decorator, which returns early when
    # the call carries no `auth` middleware.
    context = MagicMock()
    context.get_middleware.return_value = None

    with _recording_security_manager(seen) as sm:
        OfflineServer.do_get(
            server, context=context, ticket=fl.Ticket(ticket=str(key).encode())
        )
        sm.permissions  # after the dispatcher returns

    assert seen == ["project_b", _SERVER_HOME_PROJECT], (
        "the check inside the handler must resolve to the requested project, and the "
        "binding must be gone once the request is done"
    )
    server.store.set_current_project.assert_called_once_with("project_b")
    server.store.reset_current_project.assert_called_once_with("project-token")


def test_call_api_scopes_the_permission_lookup_to_the_requested_project():
    """The put-side dispatcher scopes the permission lookup the same way."""
    command = {"api": "validate_data_source", "project": "project_b"}
    server, key = _server_for(command)
    seen = []
    server.validate_data_source.side_effect = lambda *a, **k: _permission_check()

    with _recording_security_manager(seen) as sm:
        OfflineServer._call_api(server, command["api"], command, key)
        sm.permissions

    assert seen == ["project_b", _SERVER_HOME_PROJECT]
    server.store.set_current_project.assert_called_once_with("project_b")
    server.store.reset_current_project.assert_called_once_with("project-token")


def test_call_api_resets_the_project_when_the_handler_raises():
    """A failed request must not leave its project bound for the next one."""
    command = {"api": "validate_data_source", "project": "project_b"}
    server, key = _server_for(command)
    seen = []
    server.validate_data_source.side_effect = RuntimeError("boom")

    with _recording_security_manager(seen) as sm:
        with pytest.raises(RuntimeError):
            OfflineServer._call_api(server, command["api"], command, key)
        sm.permissions

    assert seen == [_SERVER_HOME_PROJECT]
    server.store.reset_current_project.assert_called_once_with("project-token")


def test_call_api_without_a_project_falls_back_to_the_servers_own():
    """
    A command that carries no project passes `None`, which the SecurityManager falls
    back from to the project it was built with -- the behaviour before this scoping
    existed.
    """
    command = {"api": "validate_data_source"}
    server, key = _server_for(command)
    seen = []
    server.validate_data_source.side_effect = lambda *a, **k: _permission_check()

    with _recording_security_manager(seen):
        OfflineServer._call_api(server, command["api"], command, key)

    assert seen == [_SERVER_HOME_PROJECT]
    server.store.set_current_project.assert_called_once_with(None)


def test_dispatchers_work_without_a_security_manager():
    """An unauthenticated deployment has no SecurityManager at all."""
    no_security_manager()
    command = {"api": "validate_data_source", "project": "project_b"}
    server, key = _server_for(command)

    OfflineServer._call_api(server, command["api"], command, key)

    server.store.set_current_project.assert_called_once_with("project_b")
    server.store.reset_current_project.assert_called_once_with("project-token")
