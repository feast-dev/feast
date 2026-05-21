import threading
from unittest.mock import MagicMock

from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import (
    Query,
)


def test_query_init_in_main_thread_registers_signals():
    """signal.signal() should work fine in main thread."""
    cursor = MagicMock()
    # Should not raise any exception in main thread
    query = Query(query_text="SELECT 1", cursor=cursor)
    assert query.query_text == "SELECT 1"


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

    assert not errors, f"Unexpected ValueError in worker thread: {errors[0]}"
