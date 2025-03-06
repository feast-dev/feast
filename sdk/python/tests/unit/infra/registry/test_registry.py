import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from feast.infra.registry.caching_registry import CachingRegistry


class TestCachingRegistry(CachingRegistry):
    """Test subclass that implements abstract methods as no-ops"""

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


def test_refresh_failure_triggers_alert_in_thread_mode(registry):
    """Test that refresh failures in thread mode trigger the failure handler"""

    registry._on_cache_refresh_failure = lambda e: None

    with (
        patch.object(registry, "_on_cache_refresh_failure") as mock_handler,
        patch.object(
            registry, "refresh", side_effect=Exception("Mock refresh failure")
        ),
    ):
        registry._start_thread_async_refresh(cache_ttl_seconds=1)

        time.sleep(2)

        mock_handler.assert_called_once()
