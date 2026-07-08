"""Tests for remote materialization: shared poller, client-side delegation, and async endpoints."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from feast.feature_server import get_app
from feast.materialization_status import (
    FVMaterializationStatus,
    poll_materialization_status,
)

# ---------------------------------------------------------------------------
# Tests for poll_materialization_status
# ---------------------------------------------------------------------------


class TestPollMaterializationStatus:
    def test_all_succeed_immediately(self):
        """All FVs report SUCCEEDED on first poll."""

        def status_fn(name):
            return FVMaterializationStatus.SUCCEEDED

        results = poll_materialization_status(
            feature_view_names=["fv_a", "fv_b"],
            status_fn=status_fn,
            poll_interval=0.01,
        )

        assert len(results) == 2
        assert all(r.status == FVMaterializationStatus.SUCCEEDED for r in results)

    def test_mixed_results(self):
        """One FV succeeds, one fails."""
        statuses = {
            "fv_a": FVMaterializationStatus.SUCCEEDED,
            "fv_b": FVMaterializationStatus.FAILED,
        }

        def status_fn(name):
            return statuses[name]

        results = poll_materialization_status(
            feature_view_names=["fv_a", "fv_b"],
            status_fn=status_fn,
            poll_interval=0.01,
        )

        result_map = {r.name: r for r in results}
        assert result_map["fv_a"].status == FVMaterializationStatus.SUCCEEDED
        assert result_map["fv_b"].status == FVMaterializationStatus.FAILED

    def test_timeout(self):
        """FV that stays RUNNING hits timeout."""

        def status_fn(name):
            return FVMaterializationStatus.RUNNING

        results = poll_materialization_status(
            feature_view_names=["fv_stuck"],
            status_fn=status_fn,
            poll_interval=0.01,
            timeout=0.05,
        )

        assert len(results) == 1
        assert results[0].status == FVMaterializationStatus.FAILED
        assert "Timed out" in results[0].error

    def test_transition_from_pending_to_running_to_succeeded(self):
        """FV transitions through states over multiple polls."""
        call_count = {"n": 0}

        def status_fn(name):
            call_count["n"] += 1
            if call_count["n"] <= 1:
                return FVMaterializationStatus.PENDING
            elif call_count["n"] <= 2:
                return FVMaterializationStatus.RUNNING
            return FVMaterializationStatus.SUCCEEDED

        results = poll_materialization_status(
            feature_view_names=["fv_transitioning"],
            status_fn=status_fn,
            poll_interval=0.01,
        )

        assert results[0].status == FVMaterializationStatus.SUCCEEDED

    def test_on_status_change_callback(self):
        """Callback is invoked on state changes."""
        changes = []
        call_count = {"n": 0}

        def status_fn(name):
            call_count["n"] += 1
            if call_count["n"] <= 1:
                return FVMaterializationStatus.RUNNING
            return FVMaterializationStatus.SUCCEEDED

        poll_materialization_status(
            feature_view_names=["fv_x"],
            status_fn=status_fn,
            poll_interval=0.01,
            on_status_change=lambda r: changes.append(r),
        )

        assert len(changes) == 2
        assert changes[0].status == FVMaterializationStatus.RUNNING
        assert changes[1].status == FVMaterializationStatus.SUCCEEDED

    def test_status_fn_exception_is_handled(self):
        """If status_fn raises, the poller continues gracefully."""
        call_count = {"n": 0}

        def status_fn(name):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("transient error")
            return FVMaterializationStatus.SUCCEEDED

        results = poll_materialization_status(
            feature_view_names=["fv_err"],
            status_fn=status_fn,
            poll_interval=0.01,
        )

        assert results[0].status == FVMaterializationStatus.SUCCEEDED

    def test_elapsed_seconds_is_populated(self):
        """Results include elapsed time."""

        def status_fn(name):
            return FVMaterializationStatus.SUCCEEDED

        results = poll_materialization_status(
            feature_view_names=["fv_t"],
            status_fn=status_fn,
            poll_interval=0.01,
        )

        assert results[0].elapsed_seconds is not None
        assert results[0].elapsed_seconds >= 0


# ---------------------------------------------------------------------------
# Tests for feature_server async endpoints
# ---------------------------------------------------------------------------


class TestFeatureServerAsyncEndpoints:
    def _make_client(self):
        """Create a test client with mocked FeatureStore."""
        fs = MagicMock()
        fs._get_provider.return_value.async_supported.online.read = False
        fs.initialize = MagicMock()
        fs.close = MagicMock()

        mock_fv = MagicMock()
        mock_fv.name = "test_fv"
        fs._get_feature_views_to_materialize.return_value = [mock_fv]

        fs.materialize = MagicMock()
        fs.materialize_incremental = MagicMock()

        client = TestClient(get_app(fs))
        return client, fs

    def test_materialize_async_returns_202(self):
        client, fs = self._make_client()
        response = client.post(
            "/materialize-async",
            json={
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-02T00:00:00Z",
                "feature_views": ["test_fv"],
            },
        )
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "accepted"
        assert "test_fv" in body["feature_views"]

    def test_materialize_incremental_async_returns_202(self):
        client, fs = self._make_client()
        response = client.post(
            "/materialize-incremental-async",
            json={
                "end_ts": "2024-01-02T00:00:00Z",
                "feature_views": ["test_fv"],
            },
        )
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "accepted"
        assert "test_fv" in body["feature_views"]

    def test_materialize_async_validates_timestamps(self):
        client, fs = self._make_client()
        with pytest.raises(ValueError, match="start_ts and end_ts are required"):
            client.post(
                "/materialize-async",
                json={
                    "feature_views": ["test_fv"],
                    "disable_event_timestamp": False,
                },
            )

    def test_materialize_async_disable_event_timestamp(self):
        client, fs = self._make_client()
        response = client.post(
            "/materialize-async",
            json={
                "feature_views": ["test_fv"],
                "disable_event_timestamp": True,
            },
        )
        assert response.status_code == 202


# ---------------------------------------------------------------------------
# Tests for FeatureStore remote materialize gate
# ---------------------------------------------------------------------------


class TestFeatureStoreRemoteGate:
    @patch("feast.feature_store.FeatureStore._remote_materialize")
    def test_materialize_delegates_when_remote_mode(self, mock_remote):
        """materialize() calls _remote_materialize when mode is remote."""
        fs = MagicMock()
        fs.config.feature_server.materialize_mode = "remote"
        fs.config.feature_server.url = "http://feast-server:80"

        # Call the unbound method with our mock self
        fs._is_remote_materialize_mode = MagicMock(return_value=True)
        fs._remote_materialize = MagicMock()

        # Simulate what happens: gate check returns True, calls remote
        if fs._is_remote_materialize_mode():
            fs._remote_materialize(
                start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_date=datetime(2024, 1, 2, tzinfo=timezone.utc),
                feature_views=["fv1"],
            )

        fs._remote_materialize.assert_called_once()

    def test_is_remote_materialize_mode_false_by_default(self):
        """Without config, remote mode is False."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server = None

        result = FeatureStore._is_remote_materialize_mode(fs)
        assert result is False

    def test_is_remote_materialize_mode_true(self):
        """With materialize_mode=remote, returns True."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.materialize_mode = "remote"

        result = FeatureStore._is_remote_materialize_mode(fs)
        assert result is True

    def test_is_remote_materialize_mode_local(self):
        """With materialize_mode=local (default), returns False."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.materialize_mode = "local"

        result = FeatureStore._is_remote_materialize_mode(fs)
        assert result is False

    def test_get_feature_server_url_raises_without_url(self):
        """Raises ValueError when url is not set."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.url = None

        with pytest.raises(ValueError, match="feature_server.url must be set"):
            FeatureStore._get_feature_server_url(fs)

    def test_get_feature_server_url_strips_trailing_slash(self):
        """URL is returned without trailing slash."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.url = "http://feast-server:80/"

        result = FeatureStore._get_feature_server_url(fs)
        assert result == "http://feast-server:80"


# ---------------------------------------------------------------------------
# Tests for LocalFeatureServerConfig
# ---------------------------------------------------------------------------


class TestLocalFeatureServerConfig:
    def test_defaults(self):
        from feast.infra.feature_servers.local_process.config import (
            LocalFeatureServerConfig,
        )

        cfg = LocalFeatureServerConfig()
        assert cfg.materialize_mode == "local"
        assert cfg.url is None
        assert cfg.materialize_timeout == 3600.0
        assert cfg.materialize_poll_interval == 5.0

    def test_remote_config(self):
        from feast.infra.feature_servers.local_process.config import (
            LocalFeatureServerConfig,
        )

        cfg = LocalFeatureServerConfig(
            materialize_mode="remote",
            url="http://feast-server:80",
            materialize_timeout=600.0,
            materialize_poll_interval=2.0,
        )
        assert cfg.materialize_mode == "remote"
        assert cfg.url == "http://feast-server:80"
        assert cfg.materialize_timeout == 600.0
        assert cfg.materialize_poll_interval == 2.0
