import json
import os
import subprocess
import sys
import textwrap
from unittest.mock import MagicMock, mock_open, patch

import assertpy
import pyarrow as pa
import pytest

from feast.infra.offline_stores.remote import (
    RemoteOfflineStore,
    RemoteOfflineStoreConfig,
    _call_exchange,
    _create_retrieval_metadata,
)
from feast.offline_server import (
    OfflineServer,
    _configure_grpc_fips,
    _is_fips_enabled,
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


# ---------------------------------------------------------------------------
# do_exchange tests — HPA-safe single-stream read path
# ---------------------------------------------------------------------------


def test_do_exchange_get_historical_features():
    """do_exchange delegates to _get_historical_features_direct for
    get_historical_features and writes the result table back."""
    import pyarrow.flight as fl

    result_table = pa.table({"col": [1, 2, 3]})
    mock_job = MagicMock()
    mock_job.to_arrow.return_value = result_table
    mock_offline_store = MagicMock()
    mock_offline_store.get_historical_features.return_value = mock_job

    mock_store = MagicMock()
    mock_store.config.project = "test"

    server = MagicMock(spec=OfflineServer)
    server.offline_store = mock_offline_store
    server.store = mock_store
    server.flights = {}
    server.list_feature_views_by_name.return_value = []
    server._validate_do_get_parameters = (
        OfflineServer._validate_do_get_parameters.__get__(server)
    )
    server._execute_read_api = OfflineServer._execute_read_api.__get__(server)
    server._get_historical_features_direct = (
        OfflineServer._get_historical_features_direct.__get__(server)
    )
    server._validate_get_historical_features_parameters = (
        OfflineServer._validate_get_historical_features_parameters.__get__(server)
    )
    server.get_historical_features = OfflineServer.get_historical_features.__get__(
        server
    )

    command = {
        "api": "get_historical_features",
        "command_id": "test-123",
        "feature_view_names": [],
        "name_aliases": [],
        "feature_refs": ["driver_hourly_stats:conv_rate"],
        "project": "test",
        "full_feature_names": False,
    }
    descriptor = fl.FlightDescriptor.for_command(json.dumps(command))

    entity_table = pa.table({"key": ["mock_key"]})
    mock_reader = MagicMock()
    mock_reader.read_all.return_value = entity_table

    mock_writer = MagicMock()

    OfflineServer.do_exchange.__wrapped__.__wrapped__(
        server, MagicMock(), descriptor, mock_reader, mock_writer
    )

    mock_writer.begin.assert_called_once_with(result_table.schema)
    mock_writer.write_table.assert_called_once_with(result_table)


def test_do_exchange_pull_all_from_table_or_query():
    """do_exchange delegates to pull_all_from_table_or_query correctly."""
    import pyarrow.flight as fl

    result_table = pa.table({"feature": [10, 20]})
    mock_job = MagicMock()
    mock_job.to_arrow.return_value = result_table

    server = MagicMock(spec=OfflineServer)
    server.flights = {}
    server._validate_do_get_parameters = (
        OfflineServer._validate_do_get_parameters.__get__(server)
    )
    server._execute_read_api = OfflineServer._execute_read_api.__get__(server)
    server.pull_all_from_table_or_query.return_value = mock_job

    command = {
        "api": "pull_all_from_table_or_query",
        "command_id": "test-456",
        "data_source_name": "ds",
        "join_key_columns": [],
        "feature_name_columns": [],
        "timestamp_field": "ts",
        "created_timestamp_column": "",
        "start_date": "2021-01-01T00:00:00",
        "end_date": "2021-12-31T00:00:00",
    }
    descriptor = fl.FlightDescriptor.for_command(json.dumps(command))

    mock_reader = MagicMock()
    mock_reader.read_all.return_value = pa.table({"key": ["mock_key"]})
    mock_writer = MagicMock()

    OfflineServer.do_exchange.__wrapped__.__wrapped__(
        server, MagicMock(), descriptor, mock_reader, mock_writer
    )

    server.pull_all_from_table_or_query.assert_called_once()
    mock_writer.begin.assert_called_once_with(result_table.schema)
    mock_writer.write_table.assert_called_once_with(result_table)


def test_call_exchange_sends_entity_df_and_reads_result():
    """_call_exchange sends entity data via do_exchange and reads the result."""
    import pandas as pd

    result_table = pa.table({"result": [1, 2, 3]})

    mock_writer = MagicMock()
    mock_reader = MagicMock()
    mock_reader._connection_retries = 0
    mock_reader.read_all.return_value = result_table

    mock_client = MagicMock()
    mock_client.do_exchange.return_value = (mock_writer, mock_reader)

    entity_df = pd.DataFrame(
        {"driver_id": [1, 2], "event_timestamp": ["2021-01-01", "2021-01-02"]}
    )

    table = _call_exchange(
        api="get_historical_features",
        api_parameters={
            "feature_refs": ["f1"],
            "project": "test",
            "full_feature_names": False,
            "feature_view_names": [],
            "name_aliases": [],
        },
        client=mock_client,
        entity_df=entity_df,
        table=None,
    )

    mock_client.do_exchange.assert_called_once()
    mock_writer.begin.assert_called_once()
    mock_writer.write_table.assert_called_once()
    mock_writer.done_writing.assert_called_once()
    assertpy.assert_that(table).is_equal_to(result_table)


def test_call_exchange_sends_empty_table_when_no_entity_df():
    """_call_exchange sends a stub table when entity_df and table are both None."""
    result_table = pa.table({"result": [42]})

    mock_writer = MagicMock()
    mock_reader = MagicMock()
    mock_reader._connection_retries = 0
    mock_reader.read_all.return_value = result_table

    mock_client = MagicMock()
    mock_client.do_exchange.return_value = (mock_writer, mock_reader)

    table = _call_exchange(
        api="pull_all_from_table_or_query",
        api_parameters={"data_source_name": "ds"},
        client=mock_client,
        entity_df=None,
        table=None,
    )

    mock_client.do_exchange.assert_called_once()
    mock_writer.begin.assert_called_once()
    call_args = mock_writer.write_table.call_args[0][0]
    assertpy.assert_that(call_args.column_names).contains("key")
    assertpy.assert_that(table).is_equal_to(result_table)
