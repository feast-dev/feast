"""Tests for remote materialization: shared poller, client-side delegation, and async endpoints."""

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from feast.feature_server import get_app
from feast.feature_view import FeatureViewState
from feast.materialization_status import (
    FVMaterializationStatus,
    poll_materialization_status,
)

# ---------------------------------------------------------------------------
# Tests for poll_materialization_status
# ---------------------------------------------------------------------------


class TestPollMaterializationStatus:
    def test_all_succeed_immediately(self):
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

    def test_transient_exception_is_handled(self):
        """Transient errors from status_fn don't crash the poller."""
        call_count = {"n": 0}

        def status_fn(name):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise ConnectionError("transient network error")
            return FVMaterializationStatus.SUCCEEDED

        results = poll_materialization_status(
            feature_view_names=["fv_err"],
            status_fn=status_fn,
            poll_interval=0.01,
        )

        assert results[0].status == FVMaterializationStatus.SUCCEEDED

    def test_elapsed_seconds_is_populated(self):
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
        fs = MagicMock()
        fs._get_provider.return_value.async_supported.online.read = False
        fs.initialize = MagicMock()
        fs.close = MagicMock()
        fs.project = "test_project"

        mock_fv = MagicMock()
        mock_fv.name = "test_fv"
        mock_fv.state = FeatureViewState.GENERATED
        fs._get_feature_views_to_materialize.return_value = [mock_fv]
        fs.registry.get_feature_view.return_value = mock_fv

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

    def test_materialize_async_uses_force_local(self):
        """Verify the server passes _force_local=True to prevent recursion."""
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
        # Give the background task time to execute
        time.sleep(0.1)
        fs.materialize.assert_called_once()
        call_kwargs = fs.materialize.call_args[1]
        assert call_kwargs["_force_local"] is True

    def test_materialize_async_409_if_already_materializing(self):
        """Return 409 if FV is already in MATERIALIZING state."""
        client, fs = self._make_client()
        mock_fv = MagicMock()
        mock_fv.state = FeatureViewState.MATERIALIZING
        fs.registry.get_feature_view.return_value = mock_fv

        response = client.post(
            "/materialize-async",
            json={
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-02T00:00:00Z",
                "feature_views": ["test_fv"],
            },
        )
        assert response.status_code == 409

    def test_materialize_async_forwards_version(self):
        """Version parameter is forwarded to store.materialize."""
        client, fs = self._make_client()
        response = client.post(
            "/materialize-async",
            json={
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-02T00:00:00Z",
                "feature_views": ["test_fv"],
                "version": "v2",
            },
        )
        assert response.status_code == 202
        time.sleep(0.1)
        call_kwargs = fs.materialize.call_args[1]
        assert call_kwargs["version"] == "v2"

    def test_materialize_async_validates_timestamp_order(self):
        """start_ts must be before end_ts."""
        client, fs = self._make_client()
        with pytest.raises(ValueError, match="must be before"):
            client.post(
                "/materialize-async",
                json={
                    "start_ts": "2024-01-02T00:00:00Z",
                    "end_ts": "2024-01-01T00:00:00Z",
                    "feature_views": ["test_fv"],
                },
            )


# ---------------------------------------------------------------------------
# Tests for FeatureStore remote materialize gate
# ---------------------------------------------------------------------------


class TestFeatureStoreRemoteGate:
    def test_materialize_calls_remote_when_mode_is_remote(self):
        """materialize() delegates to _remote_materialize when mode is remote."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs._is_remote_materialize_mode = MagicMock(return_value=True)
        fs._remote_materialize = MagicMock()

        # Call the actual materialize method (unbound) with our mock
        FeatureStore.materialize(
            fs,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 2, tzinfo=timezone.utc),
            feature_views=["fv1"],
        )

        fs._remote_materialize.assert_called_once()

    def test_materialize_skips_remote_when_force_local(self):
        """materialize() with _force_local=True skips remote even if configured."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs._is_remote_materialize_mode = MagicMock(return_value=True)
        fs._remote_materialize = MagicMock()

        # _force_local should bypass the remote gate and proceed to local logic.
        # This will fail on the local path (mocked), but _remote_materialize shouldn't be called.
        try:
            FeatureStore.materialize(
                fs,
                start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_date=datetime(2024, 1, 2, tzinfo=timezone.utc),
                feature_views=["fv1"],
                _force_local=True,
            )
        except Exception:
            pass

        fs._remote_materialize.assert_not_called()

    def test_is_remote_materialize_mode_false_by_default(self):
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server = None
        result = FeatureStore._is_remote_materialize_mode(fs)
        assert result is False

    def test_is_remote_materialize_mode_true(self):
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.materialize_mode = "remote"
        result = FeatureStore._is_remote_materialize_mode(fs)
        assert result is True

    def test_is_remote_materialize_mode_local(self):
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.materialize_mode = "local"
        result = FeatureStore._is_remote_materialize_mode(fs)
        assert result is False

    def test_get_feature_server_url_raises_without_url(self):
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.url = None
        with pytest.raises(ValueError, match="feature_server.url must be set"):
            FeatureStore._get_feature_server_url(fs)

    def test_get_feature_server_url_strips_trailing_slash(self):
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.url = "http://feast-server:80/"
        result = FeatureStore._get_feature_server_url(fs)
        assert result == "http://feast-server:80"

    def test_get_feature_server_url_validates_scheme(self):
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.url = "ftp://feast-server:80"
        with pytest.raises(ValueError, match="http or https"):
            FeatureStore._get_feature_server_url(fs)


# ---------------------------------------------------------------------------
# Tests for state mapping with seen_materializing tracking
# ---------------------------------------------------------------------------


class TestRegistryStatusMapping:
    def test_generated_before_materializing_is_pending(self):
        """GENERATED state before we've ever seen MATERIALIZING = PENDING."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.materialize_mode = "remote"
        fs.config.feature_server.url = "http://server:80"
        fs.config.feature_server.materialize_timeout = 1.0
        fs.config.feature_server.materialize_poll_interval = 0.01
        fs.config.feature_server.http_timeout = 5
        fs.config.auth_config = None
        fs.project = "test"

        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp
        fs._get_remote_http_session = MagicMock(return_value=mock_session)
        fs._get_feature_server_url = MagicMock(return_value="http://server:80")

        # Simulate: GENERATED → MATERIALIZING → AVAILABLE_ONLINE
        call_count = {"n": 0}

        def mock_get_fv(name, project, allow_cache=False):
            call_count["n"] += 1
            fv = MagicMock()
            if call_count["n"] <= 2:
                fv.state = FeatureViewState.GENERATED
            elif call_count["n"] <= 4:
                fv.state = FeatureViewState.MATERIALIZING
            else:
                fv.state = FeatureViewState.AVAILABLE_ONLINE
            return fv

        fs.registry.get_feature_view = mock_get_fv

        FeatureStore._remote_materialize_common(
            fs, "/materialize-async", {"feature_views": ["fv1"]}, ["fv1"]
        )

    def test_generated_after_materializing_is_failed(self):
        """GENERATED state after we've seen MATERIALIZING = FAILED (rollback)."""
        from feast.feature_store import FeatureStore

        fs = MagicMock()
        fs.config.feature_server.materialize_mode = "remote"
        fs.config.feature_server.url = "http://server:80"
        fs.config.feature_server.materialize_timeout = 1.0
        fs.config.feature_server.materialize_poll_interval = 0.01
        fs.config.feature_server.http_timeout = 5
        fs.config.auth_config = None
        fs.project = "test"

        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp
        fs._get_remote_http_session = MagicMock(return_value=mock_session)
        fs._get_feature_server_url = MagicMock(return_value="http://server:80")

        # Simulate: MATERIALIZING → GENERATED (rollback = failure)
        call_count = {"n": 0}

        def mock_get_fv(name, project, allow_cache=False):
            call_count["n"] += 1
            fv = MagicMock()
            if call_count["n"] <= 2:
                fv.state = FeatureViewState.MATERIALIZING
            else:
                fv.state = FeatureViewState.GENERATED
            return fv

        fs.registry.get_feature_view = mock_get_fv

        with pytest.raises(Exception, match="Remote materialization failed"):
            FeatureStore._remote_materialize_common(
                fs, "/materialize-async", {"feature_views": ["fv1"]}, ["fv1"]
            )


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
