import logging
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from feast.infra.utils.clickhouse.clickhouse_config import ClickhouseConfig
from feast.infra.utils.clickhouse.connection_utils import get_client, thread_local

logger = logging.getLogger(__name__)


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


def test_clickhouse_config_parses_additional_client_args():
    """
    Test that ClickhouseConfig correctly parses additional_client_args from a dict,
    simulating how it would be parsed from YAML by Pydantic.
    """
    # This simulates the dict that would come from yaml.safe_load()
    raw_config = {
        "host": "localhost",
        "port": 8123,
        "database": "default",
        "user": "default",
        "password": "password",
        "additional_client_args": {
            "send_receive_timeout": 60,
            "compress": True,
            "client_name": "feast_test",
        },
    }

    # Pydantic should parse this dict into a ClickhouseConfig object
    config = ClickhouseConfig(**raw_config)

    # Verify all fields are correctly parsed
    assert config.host == "localhost"
    assert config.port == 8123
    assert config.database == "default"
    assert config.user == "default"
    assert config.password == "password"

    # Verify additional_client_args is correctly parsed as a dict
    assert config.additional_client_args is not None
    assert isinstance(config.additional_client_args, dict)
    assert config.additional_client_args["send_receive_timeout"] == 60
    assert config.additional_client_args["compress"] is True
    assert config.additional_client_args["client_name"] == "feast_test"


def test_clickhouse_config_handles_none_additional_client_args():
    """
    Test that ClickhouseConfig correctly handles when additional_client_args is not provided.
    """
    raw_config = {
        "host": "localhost",
        "port": 8123,
        "database": "default",
        "user": "default",
        "password": "password",
    }

    config = ClickhouseConfig(**raw_config)

    assert config.additional_client_args is None


class TestNonEntityRetrieval:
    """Test the non-entity retrieval logic (entity_df=None) for ClickHouse."""

    _MODULE = "feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse"

    def _call_get_historical_features(self, feature_views, **kwargs):
        """Call get_historical_features with entity_df=None, mocking the pipeline."""
        from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse import (
            ClickhouseOfflineStore,
            ClickhouseOfflineStoreConfig,
        )
        from feast.repo_config import RepoConfig

        config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=ClickhouseOfflineStoreConfig(
                type="clickhouse",
                host="localhost",
                port=9000,
                database="test_db",
                user="default",
                password="password",
            ),
        )

        end = kwargs.get("end_date", datetime(2023, 1, 7, tzinfo=timezone.utc))

        with (
            patch.multiple(
                self._MODULE,
                _upload_entity_df=MagicMock(),
                _get_entity_schema=MagicMock(
                    return_value={"event_timestamp": "timestamp"}
                ),
                _get_entity_df_event_timestamp_range=MagicMock(
                    return_value=(end - timedelta(days=1), end)
                ),
            ),
            patch(
                f"{self._MODULE}.offline_utils.get_expected_join_keys",
                return_value=[],
            ),
            patch(
                f"{self._MODULE}.offline_utils.assert_expected_columns_in_entity_df",
            ),
            patch(
                f"{self._MODULE}.offline_utils.get_feature_view_query_context",
                return_value=[],
            ),
        ):
            refs = [f"{fv.name}:feature1" for fv in feature_views]
            return ClickhouseOfflineStore.get_historical_features(
                config=config,
                feature_views=feature_views,
                feature_refs=refs,
                entity_df=None,
                registry=MagicMock(),
                project="test_project",
                **kwargs,
            )

    @staticmethod
    def _make_feature_view(name, ttl=None):
        from feast.entity import Entity
        from feast.feature_view import FeatureView, Field
        from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
            ClickhouseSource,
        )
        from feast.types import Float32

        return FeatureView(
            name=name,
            entities=[Entity(name="driver_id", join_keys=["driver_id"])],
            ttl=ttl,
            source=ClickhouseSource(
                name=f"{name}_source",
                table=f"{name}_table",
                timestamp_field="event_timestamp",
            ),
            schema=[
                Field(name="feature1", dtype=Float32),
            ],
        )

    def test_non_entity_mode_with_end_date(self):
        """entity_df=None with explicit end_date produces a valid RetrievalJob."""
        from feast.infra.offline_stores.offline_store import RetrievalJob

        fv = self._make_feature_view("test_fv")
        job = self._call_get_historical_features(
            [fv],
            end_date=datetime(2023, 1, 7, tzinfo=timezone.utc),
        )
        assert isinstance(job, RetrievalJob)

    def test_non_entity_mode_defaults_end_date(self):
        """entity_df=None without end_date defaults to now."""
        from feast.infra.offline_stores.offline_store import RetrievalJob

        fv = self._make_feature_view("test_fv")
        job = self._call_get_historical_features([fv])
        assert isinstance(job, RetrievalJob)
