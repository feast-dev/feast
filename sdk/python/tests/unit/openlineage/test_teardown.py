"""Tests for FeatureStore._teardown_openlineage()."""

from contextvars import ContextVar
from unittest.mock import MagicMock, patch

import pytest


class TestTeardownOpenlineage:
    def _make_feature_store(
        self, ol_enabled=True, consumer_enabled=True, conn_str="sqlite:///test.db"
    ):
        """Create a mock FeatureStore with configurable OL settings."""
        from feast.feature_store import FeatureStore

        fs = object.__new__(FeatureStore)
        fs._current_project = ContextVar("current_project", default=None)

        mock_ol_config = MagicMock()
        mock_ol_config.enabled = ol_enabled

        mock_consumer = MagicMock()
        mock_consumer.enabled = consumer_enabled
        mock_consumer.connection_string = conn_str

        mock_full_config = MagicMock()
        mock_full_config.consumer = mock_consumer

        mock_ol_config.to_openlineage_config.return_value = mock_full_config

        mock_config = MagicMock()
        mock_config.project = "test_project"
        mock_config.openlineage = mock_ol_config

        fs.config = mock_config

        return fs

    def test_calls_purge_namespace(self):
        """When OL consumer is configured, teardown should purge the project namespace."""
        fs = self._make_feature_store()

        with patch("feast.openlineage.store.OpenLineageStore") as mock_store_cls:
            mock_instance = mock_store_cls.return_value
            fs._teardown_openlineage()
            mock_store_cls.assert_called_once_with(
                connection_string="sqlite:///test.db"
            )
            mock_instance.purge_namespace.assert_called_once_with(
                "test_project/test_project"
            )

    def test_no_crash_when_ol_not_configured(self):
        """When OL is not configured, teardown should silently succeed."""
        from feast.feature_store import FeatureStore

        fs = object.__new__(FeatureStore)
        fs._current_project = ContextVar("current_project", default=None)
        mock_config = MagicMock()
        mock_config.openlineage = None
        fs.config = mock_config

        fs._teardown_openlineage()

    def test_no_crash_when_consumer_disabled(self):
        """When consumer is disabled, teardown should not attempt purge."""
        fs = self._make_feature_store(consumer_enabled=False)
        fs._teardown_openlineage()

    def test_no_crash_when_ol_disabled(self):
        """When OL is disabled entirely, teardown should not attempt purge."""
        fs = self._make_feature_store(ol_enabled=False)
        fs._teardown_openlineage()

    def test_exception_is_warning_not_error(self):
        """If purge fails, it should warn, not raise."""
        fs = self._make_feature_store()

        with patch(
            "feast.openlineage.store.OpenLineageStore",
            side_effect=Exception("DB error"),
        ):
            with pytest.warns(match="Failed to clean up OpenLineage"):
                fs._teardown_openlineage()
