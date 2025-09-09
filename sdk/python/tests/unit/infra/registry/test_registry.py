from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from feast.infra.registry.caching_registry import CachingRegistry
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto


class TestCachingRegistry(CachingRegistry):
    """Test subclass that implements abstract methods as no-ops"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._test_metadata = {}

    def get_project_metadata(self, project: str, key: str) -> str:
        return self._test_metadata.get((project, key))

    def set_project_metadata(self, project: str, key: str, value: str):
        self._test_metadata[(project, key)] = value

    def _get_any_feature_view(self, *args, **kwargs):
        pass

    def _get_data_source(self, *args, **kwargs):
        pass

    def _get_entity(self, *args, **kwargs):
        pass

    def _get_feature_service(self, *args, **kwargs):
        pass

    def _get_feature_view(self, *args, **kwargs):
        pass

    def _get_infra(self, *args, **kwargs):
        pass

    def _get_on_demand_feature_view(self, *args, **kwargs):
        pass

    def _get_permission(self, *args, **kwargs):
        pass

    def _get_project(self, *args, **kwargs):
        pass

    def _get_saved_dataset(self, *args, **kwargs):
        pass

    def _get_stream_feature_view(self, *args, **kwargs):
        pass

    def _get_validation_reference(self, *args, **kwargs):
        pass

    def _list_all_feature_views(self, *args, **kwargs):
        pass

    def _list_data_sources(self, *args, **kwargs):
        pass

    def _list_entities(self, *args, **kwargs):
        pass

    def _list_feature_services(self, *args, **kwargs):
        pass

    def _list_feature_views(self, *args, **kwargs):
        pass

    def _list_on_demand_feature_views(self, *args, **kwargs):
        pass

    def _list_permissions(self, *args, **kwargs):
        pass

    def _list_project_metadata(self, *args, **kwargs):
        pass

    def _list_projects(self, *args, **kwargs):
        pass

    def _list_saved_datasets(self, *args, **kwargs):
        pass

    def _list_stream_feature_views(self, *args, **kwargs):
        pass

    def _list_validation_references(self, *args, **kwargs):
        pass

    def apply_data_source(self, *args, **kwargs):
        pass

    def apply_entity(self, *args, **kwargs):
        pass

    def apply_feature_service(self, *args, **kwargs):
        pass

    def apply_feature_view(self, *args, **kwargs):
        pass

    def apply_materialization(self, *args, **kwargs):
        pass

    def apply_permission(self, *args, **kwargs):
        pass

    def apply_project(self, *args, **kwargs):
        pass

    def apply_saved_dataset(self, *args, **kwargs):
        pass

    def apply_user_metadata(self, *args, **kwargs):
        pass

    def apply_validation_reference(self, *args, **kwargs):
        pass

    def commit(self, *args, **kwargs):
        pass

    def delete_data_source(self, *args, **kwargs):
        pass

    def delete_entity(self, *args, **kwargs):
        pass

    def delete_feature_service(self, *args, **kwargs):
        pass

    def delete_feature_view(self, *args, **kwargs):
        pass

    def delete_permission(self, *args, **kwargs):
        pass

    def delete_project(self, *args, **kwargs):
        pass

    def delete_validation_reference(self, *args, **kwargs):
        pass

    def get_user_metadata(self, *args, **kwargs):
        pass

    def proto(self, *args, **kwargs):
        pass

    def update_infra(self, *args, **kwargs):
        pass


@pytest.fixture
def registry():
    """Fixture to create a real instance of CachingRegistry"""
    return TestCachingRegistry(
        project="test_example", cache_ttl_seconds=2, cache_mode="sync"
    )


def test_cache_expiry_triggers_refresh(registry):
    """Test that an expired cache triggers a refresh"""
    # Set cache creation time to a value that is expired
    registry.cached_registry_proto = "some_cached_data"
    registry.cached_registry_proto_created = datetime.now(timezone.utc) - timedelta(
        seconds=5
    )

    # Mock _refresh_cached_registry_if_necessary to check if it is called
    with patch.object(
        CachingRegistry,
        "_refresh_cached_registry_if_necessary",
        wraps=registry._refresh_cached_registry_if_necessary,
    ) as mock_refresh_check:
        registry._refresh_cached_registry_if_necessary()
        mock_refresh_check.assert_called_once()

    # Now check if the refresh was actually triggered
    with patch.object(
        CachingRegistry, "refresh", wraps=registry.refresh
    ) as mock_refresh:
        registry._refresh_cached_registry_if_necessary()
        mock_refresh.assert_called_once()


def test_empty_cache_refresh_with_ttl(registry):
    """Test that empty cache is refreshed when TTL > 0"""
    # Set up empty cache with TTL > 0
    registry.cached_registry_proto = RegistryProto()
    registry.cached_registry_proto_created = datetime.now(timezone.utc)
    registry.cached_registry_proto_ttl = timedelta(seconds=10)  # TTL > 0

    # Mock refresh to check if it's called
    with patch.object(
        CachingRegistry, "refresh", wraps=registry.refresh
    ) as mock_refresh:
        registry._refresh_cached_registry_if_necessary()
        # Should refresh because cache is empty and TTL > 0
        mock_refresh.assert_called_once()


def test_empty_cache_no_refresh_with_infinite_ttl(registry):
    """Test that empty cache is not refreshed when TTL = 0 (infinite)"""
    # Set up empty cache with TTL = 0 (infinite)
    registry.cached_registry_proto = RegistryProto()
    registry.cached_registry_proto_created = datetime.now(timezone.utc)
    registry.cached_registry_proto_ttl = timedelta(seconds=0)  # TTL = 0 (infinite)

    # Mock refresh to check if it's called
    with patch.object(
        CachingRegistry, "refresh", wraps=registry.refresh
    ) as mock_refresh:
        registry._refresh_cached_registry_if_necessary()
        # Should not refresh because TTL = 0 (infinite)
        mock_refresh.assert_not_called()


def test_concurrent_cache_refresh_race_condition(registry):
    """Test that concurrent requests don't skip cache refresh when cache is expired"""
    import threading
    import time

    # Set up expired cache
    registry.cached_registry_proto = RegistryProto()
    registry.cached_registry_proto_created = datetime.now(timezone.utc) - timedelta(
        seconds=5
    )
    registry.cached_registry_proto_ttl = timedelta(
        seconds=2
    )  # TTL = 2 seconds, cache is expired

    refresh_calls = []

    def mock_refresh():
        refresh_calls.append(threading.current_thread().ident)
        time.sleep(0.1)  # Simulate refresh work

    # Mock the refresh method to track calls
    with patch.object(registry, "refresh", side_effect=mock_refresh):
        # Simulate concurrent requests
        threads = []
        for i in range(3):
            thread = threading.Thread(
                target=registry._refresh_cached_registry_if_necessary
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

    # At least one thread should have called refresh (the first one to acquire the lock)
    assert len(refresh_calls) >= 1, (
        "At least one thread should have refreshed the cache"
    )


def test_skip_refresh_if_lock_held(registry):
    """Test that refresh is skipped if the lock is already held by another thread"""
    registry.cached_registry_proto = "some_cached_data"
    registry.cached_registry_proto_created = datetime.now(timezone.utc) - timedelta(
        seconds=5
    )

    # Acquire the lock manually to simulate another thread holding it
    registry._refresh_lock.acquire()
    with patch.object(
        CachingRegistry, "refresh", wraps=registry.refresh
    ) as mock_refresh:
        registry._refresh_cached_registry_if_necessary()

        # Since the lock was already held, refresh should NOT be called
        mock_refresh.assert_not_called()
    registry._refresh_lock.release()
