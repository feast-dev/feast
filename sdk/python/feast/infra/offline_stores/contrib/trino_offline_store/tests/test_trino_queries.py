import signal
import threading
from unittest.mock import MagicMock, patch

from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import (
    Query,
)


def test_query_init_in_main_thread_registers_signals():
    """signal.signal() should work fine in main thread."""

    # Should not raise any exception in main thread
    cursor = MagicMock()

    with patch("signal.signal") as mock_signal:
        query = Query(query_text="SELECT 1", cursor=cursor)
        assert query.query_text == "SELECT 1"

        # Verify signal handlers are registered correctly
        mock_signal.assert_any_call(signal.SIGINT, query.cancel)
        mock_signal.assert_any_call(signal.SIGTERM, query.cancel)

        # Expected signal.signal to be called twice for SIGINT and SIGTERM
        assert mock_signal.call_count == 2


def test_query_init_in_worker_thread_does_not_raise():
    """Regression test: signal.signal() fails in non-main threads."""
    # signal.signal() raises ValueError when called outside the main thread.
    # This test verifies the fix guards against that by running Query.__init__
    # in a worker thread and ensuring no exception is raised.

    errors = []
    cursor = MagicMock()

    def create_query():
        try:
            query = Query(query_text="SELECT 1", cursor=cursor)
            assert query.query_text == "SELECT 1"
        except ValueError as e:
            errors.append(e)

    thread = threading.Thread(target=create_query)
    thread.start()
    thread.join()

    assert not errors, f"Unexpected ValueError in worker thread: {errors}"
