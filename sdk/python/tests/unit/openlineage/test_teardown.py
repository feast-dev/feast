"""Tests for FeatureStore._teardown_openlineage()."""

from contextvars import ContextVar
from unittest.mock import MagicMock

import pytest


class TestTeardownOpenlineage:
    def _make_feature_store(self, emitter=None):
        """Create a mock FeatureStore with a controllable OL emitter."""
        from feast.feature_store import FeatureStore

        fs = object.__new__(FeatureStore)
        fs._current_project = ContextVar("current_project", default=None)
        fs._openlineage_emitter = emitter

        mock_config = MagicMock()
        mock_config.project = "test_project"
        mock_config.openlineage = MagicMock(enabled=True)
        fs.config = mock_config
        return fs

    def test_calls_emitter_teardown_project(self):
        """Teardown delegates purge to the emitter."""
        emitter = MagicMock()
        fs = self._make_feature_store(emitter=emitter)
        fs._teardown_openlineage()
        emitter.teardown_project.assert_called_once_with("test_project")

    def test_no_crash_when_emitter_missing(self):
        """When OL emitter is absent, teardown should silently succeed."""
        fs = self._make_feature_store(emitter=None)
        # Force property path: no emitter init
        fs._init_openlineage_emitter = MagicMock(return_value=None)  # type: ignore
        fs._teardown_openlineage()

    def test_exception_is_warning_not_error(self):
        """If purge fails, it should warn, not raise."""
        emitter = MagicMock()
        emitter.teardown_project.side_effect = Exception("DB error")
        fs = self._make_feature_store(emitter=emitter)

        with pytest.warns(match="Failed to clean up OpenLineage"):
            fs._teardown_openlineage()
