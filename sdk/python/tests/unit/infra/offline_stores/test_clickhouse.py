import threading
from unittest.mock import MagicMock, patch

import pytest

from feast.infra.utils.clickhouse.clickhouse_config import ClickhouseConfig
from feast.infra.utils.clickhouse.connection_utils import get_client, thread_local


@pytest.fixture
def clickhouse_config():
    """Create a test ClickHouse configuration."""
    return ClickhouseConfig(
        host="localhost",
        port=9000,
        user="default",
        password="password",
        database="test_db",
    )


@pytest.fixture(autouse=True)
def cleanup_thread_local():
    """Clean up thread_local storage after each test."""
    yield
    if hasattr(thread_local, "clickhouse_client"):
        delattr(thread_local, "clickhouse_client")


@patch("feast.infra.utils.clickhouse.connection_utils.clickhouse_connect.get_client")
def test_get_client_returns_different_objects_for_separate_threads(
    mock_get_client, clickhouse_config
):
    """
    Clickhouse client is thread-unsafe and crashes if shared between threads.
    This test ensures that get_client returns different client instances for different threads, while
    reusing the same instance within the same thread.
    """

    def create_mock_client(*args, **kwargs):
        """Create a unique mock client for each call."""
        return MagicMock()

    mock_get_client.side_effect = create_mock_client

    results = {}

    def thread_1_work():
        """Thread 1 makes 2 calls to get_client."""
        client_1a = get_client(clickhouse_config)
        client_1b = get_client(clickhouse_config)
        results["thread_1"] = (client_1a, client_1b)

    def thread_2_work():
        """Thread 2 makes 1 call to get_client."""
        client_2 = get_client(clickhouse_config)
        results["thread_2"] = client_2

    thread_1 = threading.Thread(target=thread_1_work)
    thread_2 = threading.Thread(target=thread_2_work)

    thread_1.start()
    thread_2.start()

    thread_1.join()
    thread_2.join()

    # Thread 1's two calls should return the same client (thread-local reuse)
    client_1a, client_1b = results["thread_1"]
    assert client_1a is client_1b, (
        "Same thread should get same client instance (cached)"
    )

    # Thread 2's client should be different from thread 1's client
    client_2 = results["thread_2"]
    assert client_1a is not client_2, (
        "Different threads should get different client instances (not cached)"
    )
