from unittest.mock import MagicMock, patch

import pyarrow.flight as fl
import pytest
from pydantic import ValidationError

from feast.arrow_error_handler import arrow_client_error_handling_decorator
from feast.errors import PermissionNotFoundException
from feast.infra.offline_stores.remote import RemoteOfflineStoreConfig

permissionError = PermissionNotFoundException("dummy_name", "dummy_project")


@arrow_client_error_handling_decorator
def decorated_method(error):
    raise error


@pytest.mark.parametrize(
    "error, expected_raised_error",
    [
        (fl.FlightError("Flight error: "), fl.FlightError("Flight error: ")),
        (
            fl.FlightError(f"Flight error: {permissionError.to_error_detail()}"),
            permissionError,
        ),
        (fl.FlightError("Test Error"), fl.FlightError("Test Error")),
        (RuntimeError("Flight error: "), RuntimeError("Flight error: ")),
        (permissionError, permissionError),
    ],
)
def test_rest_error_handling_with_feast_exception(error, expected_raised_error):
    with pytest.raises(
        type(expected_raised_error),
        match=str(expected_raised_error),
    ):
        decorated_method(error)


class TestArrowClientRetry:
    @patch("feast.arrow_error_handler.time.sleep")
    def test_retries_on_flight_unavailable_error(self, mock_sleep):
        client = MagicMock()
        client._connection_retries = 3
        call_count = 0

        @arrow_client_error_handling_decorator
        def flaky_method(self_arg):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise fl.FlightUnavailableError("Connection refused")
            return "success"

        result = flaky_method(client)
        assert result == "success"
        assert call_count == 3
        assert mock_sleep.call_count == 2

    @patch("feast.arrow_error_handler.time.sleep")
    def test_raises_after_max_retries_exhausted(self, mock_sleep):
        client = MagicMock()
        client._connection_retries = 3

        @arrow_client_error_handling_decorator
        def always_unavailable(self_arg):
            raise fl.FlightUnavailableError("Connection refused")

        with pytest.raises(fl.FlightUnavailableError, match="Connection refused"):
            always_unavailable(client)
        assert mock_sleep.call_count == 3

    @patch("feast.arrow_error_handler.time.sleep")
    def test_respects_connection_retries_from_client(self, mock_sleep):
        client = MagicMock()
        client._connection_retries = 1
        call_count = 0

        @arrow_client_error_handling_decorator
        def method_on_client(self_arg):
            nonlocal call_count
            call_count += 1
            raise fl.FlightUnavailableError("Connection refused")

        with pytest.raises(fl.FlightUnavailableError):
            method_on_client(client)

        assert call_count == 2  # 1 initial + 1 retry
        assert mock_sleep.call_count == 1

    @patch("feast.arrow_error_handler.time.sleep")
    def test_no_retry_on_non_transient_errors(self, mock_sleep):
        client = MagicMock()
        client._connection_retries = 3
        call_count = 0

        @arrow_client_error_handling_decorator
        def method_with_error(self_arg):
            nonlocal call_count
            call_count += 1
            raise fl.FlightError("Permanent error")

        with pytest.raises(fl.FlightError, match="Permanent error"):
            method_with_error(client)

        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("feast.arrow_error_handler.time.sleep")
    def test_exponential_backoff_timing(self, mock_sleep):
        client = MagicMock()
        client._connection_retries = 3

        @arrow_client_error_handling_decorator
        def always_unavailable(self_arg):
            raise fl.FlightUnavailableError("Connection refused")

        with pytest.raises(fl.FlightUnavailableError):
            always_unavailable(client)

        wait_times = [call.args[0] for call in mock_sleep.call_args_list]
        assert wait_times == [0.5, 1.0, 2.0]

    @patch("feast.arrow_error_handler.time.sleep")
    def test_zero_retries_disables_retry(self, mock_sleep):
        client = MagicMock()
        client._connection_retries = 0
        call_count = 0

        @arrow_client_error_handling_decorator
        def method_on_client(self_arg):
            nonlocal call_count
            call_count += 1
            raise fl.FlightUnavailableError("Connection refused")

        with pytest.raises(fl.FlightUnavailableError):
            method_on_client(client)

        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("feast.arrow_error_handler.time.sleep")
    def test_no_retry_for_standalone_stream_functions(self, mock_sleep):
        """Standalone functions (write_table, read_all) where args[0] is a
        writer/reader should not retry since broken streams can't be reused."""
        writer = MagicMock(spec=[])  # no _connection_retries attribute
        call_count = 0

        @arrow_client_error_handling_decorator
        def write_table(w):
            nonlocal call_count
            call_count += 1
            raise fl.FlightUnavailableError("stream broken")

        with pytest.raises(fl.FlightUnavailableError, match="stream broken"):
            write_table(writer)

        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("feast.arrow_error_handler.time.sleep")
    def test_negative_connection_retries_treated_as_zero(self, mock_sleep):
        """Negative _connection_retries must not skip function execution."""
        client = MagicMock()
        client._connection_retries = -1
        call_count = 0

        @arrow_client_error_handling_decorator
        def method_on_client(self_arg):
            nonlocal call_count
            call_count += 1
            return "ok"

        result = method_on_client(client)
        assert result == "ok"
        assert call_count == 1
        mock_sleep.assert_not_called()

    def test_config_rejects_negative_connection_retries(self):
        with pytest.raises(ValidationError):
            RemoteOfflineStoreConfig(host="localhost", connection_retries=-1)
