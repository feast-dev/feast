# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from feast.metrics import (
    feature_freshness_seconds,
    materialization_duration_seconds,
    materialization_result_total,
    online_features_entity_count,
    online_features_request_count,
    online_store_read_duration_seconds,
    push_request_count,
    request_count,
    request_latency,
    track_materialization,
    track_online_features_entities,
    track_online_store_read,
    track_push,
    track_request_latency,
    track_transformation,
    track_write_transformation,
    transformation_duration_seconds,
    update_feature_freshness,
    write_transformation_duration_seconds,
)


@pytest.fixture(autouse=True)
def _enable_metrics():
    """Enable all metric categories for each test, then restore."""
    import feast.metrics as m

    original = m._config
    m._config = m._MetricsFlags(
        enabled=True,
        resource=True,
        request=True,
        online_features=True,
        push=True,
        materialization=True,
        freshness=True,
    )
    yield
    m._config = original


class TestTrackRequestLatency:
    def test_success_increments_counter_and_records_latency(self):
        before_count = request_count.labels(
            endpoint="/test", status="success"
        )._value.get()

        with track_request_latency("/test"):
            pass

        after_count = request_count.labels(
            endpoint="/test", status="success"
        )._value.get()
        assert after_count == before_count + 1

    def test_error_increments_error_counter(self):
        before_count = request_count.labels(
            endpoint="/test-err", status="error"
        )._value.get()

        with pytest.raises(ValueError):
            with track_request_latency("/test-err"):
                raise ValueError("boom")

        after_count = request_count.labels(
            endpoint="/test-err", status="error"
        )._value.get()
        assert after_count == before_count + 1

    def test_latency_is_recorded(self):
        before_sum = request_latency.labels(
            endpoint="/test-latency", feature_count="", feature_view_count=""
        )._sum.get()

        with track_request_latency("/test-latency"):
            import time

            time.sleep(0.01)

        after_sum = request_latency.labels(
            endpoint="/test-latency", feature_count="", feature_view_count=""
        )._sum.get()
        assert after_sum > before_sum

    def test_feature_count_and_feature_view_count_labels(self):
        """Latency histogram carries feature_count and feature_view_count labels."""
        label_set = dict(
            endpoint="/get-online-features",
            feature_count="5",
            feature_view_count="2",
        )
        before_sum = request_latency.labels(**label_set)._sum.get()

        with track_request_latency(
            "/get-online-features", feature_count="5", feature_view_count="2"
        ):
            pass

        after_sum = request_latency.labels(**label_set)._sum.get()
        assert after_sum > before_sum

    def test_default_labels_are_empty_string(self):
        """Non-online-features endpoints get empty-string labels by default."""
        label_set = dict(
            endpoint="/materialize", feature_count="", feature_view_count=""
        )
        before_sum = request_latency.labels(**label_set)._sum.get()

        with track_request_latency("/materialize"):
            pass

        after_sum = request_latency.labels(**label_set)._sum.get()
        assert after_sum > before_sum

    def test_labels_updated_via_yielded_context(self):
        """Labels set on the yielded context are used in the final metrics."""
        label_set = dict(
            endpoint="/ctx-update", feature_count="3", feature_view_count="1"
        )
        before_sum = request_latency.labels(**label_set)._sum.get()

        with track_request_latency("/ctx-update") as ctx:
            ctx.feature_count = "3"
            ctx.feature_view_count = "1"

        after_sum = request_latency.labels(**label_set)._sum.get()
        assert after_sum > before_sum

    def test_error_before_labels_set_still_records(self):
        """Errors before labels are updated still record with default labels."""
        before_count = request_count.labels(
            endpoint="/early-fail", status="error"
        )._value.get()

        with pytest.raises(RuntimeError):
            with track_request_latency("/early-fail") as _ctx:
                raise RuntimeError("auth failed")

        after_count = request_count.labels(
            endpoint="/early-fail", status="error"
        )._value.get()
        assert after_count == before_count + 1

        recorded_sum = request_latency.labels(
            endpoint="/early-fail", feature_count="", feature_view_count=""
        )._sum.get()
        assert recorded_sum > 0


class TestMetricsOptIn:
    """Verify that when a category is disabled, its helpers are true no-ops."""

    @staticmethod
    def _all_off():
        import feast.metrics as m

        m._config = m._MetricsFlags()  # everything False

    def test_track_request_latency_noop_when_disabled(self):
        self._all_off()
        label_set = dict(
            endpoint="/disabled-test", feature_count="", feature_view_count=""
        )
        before_sum = request_latency.labels(**label_set)._sum.get()

        with track_request_latency("/disabled-test"):
            pass

        assert request_latency.labels(**label_set)._sum.get() == before_sum

    def test_track_online_features_entities_noop_when_disabled(self):
        self._all_off()
        before = online_features_request_count._value.get()
        track_online_features_entities(100)
        assert online_features_request_count._value.get() == before

    def test_track_push_noop_when_disabled(self):
        self._all_off()
        before = push_request_count.labels(
            push_source="src", mode="online"
        )._value.get()
        track_push("src", "online")
        assert (
            push_request_count.labels(push_source="src", mode="online")._value.get()
            == before
        )

    def test_track_materialization_noop_when_disabled(self):
        self._all_off()
        before = materialization_result_total.labels(
            feature_view="fv_disabled", status="success"
        )._value.get()
        track_materialization("fv_disabled", success=True, duration_seconds=1.0)
        assert (
            materialization_result_total.labels(
                feature_view="fv_disabled", status="success"
            )._value.get()
            == before
        )


class TestGranularCategoryControl:
    """Verify individual category toggles work independently."""

    def test_request_disabled_but_push_enabled(self):
        import feast.metrics as m

        m._config = m._MetricsFlags(
            enabled=True,
            request=False,
            push=True,
            resource=True,
            online_features=True,
            materialization=True,
            freshness=True,
        )

        # request should be no-op
        label_set = dict(
            endpoint="/granular-req", feature_count="", feature_view_count=""
        )
        before_req = request_latency.labels(**label_set)._sum.get()
        with track_request_latency("/granular-req"):
            pass
        assert request_latency.labels(**label_set)._sum.get() == before_req

        # push should still record
        before_push = push_request_count.labels(
            push_source="s", mode="online"
        )._value.get()
        track_push("s", "online")
        assert (
            push_request_count.labels(push_source="s", mode="online")._value.get()
            == before_push + 1
        )

    def test_online_features_disabled_but_materialization_enabled(self):
        import feast.metrics as m

        m._config = m._MetricsFlags(
            enabled=True,
            online_features=False,
            materialization=True,
            resource=True,
            request=True,
            push=True,
            freshness=True,
        )

        # online_features should be no-op
        before_of = online_features_request_count._value.get()
        track_online_features_entities(50)
        assert online_features_request_count._value.get() == before_of

        # materialization should still record
        before_mat = materialization_result_total.labels(
            feature_view="fv_gran", status="success"
        )._value.get()
        track_materialization("fv_gran", success=True, duration_seconds=1.0)
        assert (
            materialization_result_total.labels(
                feature_view="fv_gran", status="success"
            )._value.get()
            == before_mat + 1
        )

    def test_only_resource_enabled(self):
        """When only resource is on, all request-path helpers are no-ops."""
        import feast.metrics as m

        m._config = m._MetricsFlags(
            enabled=True,
            resource=True,
            request=False,
            online_features=False,
            push=False,
            materialization=False,
            freshness=False,
        )

        label_set = dict(endpoint="/res-only", feature_count="", feature_view_count="")
        before_req = request_latency.labels(**label_set)._sum.get()
        before_of = online_features_request_count._value.get()
        before_push = push_request_count.labels(
            push_source="x", mode="offline"
        )._value.get()
        before_mat = materialization_result_total.labels(
            feature_view="fv_res", status="success"
        )._value.get()

        with track_request_latency("/res-only"):
            pass
        track_online_features_entities(10)
        track_push("x", "offline")
        track_materialization("fv_res", success=True, duration_seconds=1.0)

        assert request_latency.labels(**label_set)._sum.get() == before_req
        assert online_features_request_count._value.get() == before_of
        assert (
            push_request_count.labels(push_source="x", mode="offline")._value.get()
            == before_push
        )
        assert (
            materialization_result_total.labels(
                feature_view="fv_res", status="success"
            )._value.get()
            == before_mat
        )


class TestMetricsYamlConfig:
    """Verify metrics config in feature_store.yaml is respected.

    We mock out everything past the metrics-gate check in ``start_server``
    so these tests never actually launch a real HTTP server.
    """

    @staticmethod
    def _call_start_server(mock_store, cli_metrics: bool):
        """Call start_server with enough mocking to avoid side-effects."""
        from feast.feature_server import start_server

        with (
            patch("feast.feature_server.feast_metrics") as mock_fm,
            patch("feast.feature_server.str_to_auth_manager_type"),
            patch("feast.feature_server.init_security_manager"),
            patch("feast.feature_server.init_auth_manager"),
            patch(
                "feast.feature_server.FeastServeApplication",
                side_effect=RuntimeError("stop"),
            )
            if hasattr(__import__("sys"), "platform")
            and __import__("sys").platform != "win32"
            else patch("uvicorn.run", side_effect=RuntimeError("stop")),
        ):
            try:
                start_server(
                    store=mock_store,
                    host="127.0.0.1",
                    port=6566,
                    no_access_log=True,
                    workers=1,
                    worker_connections=1000,
                    max_requests=1000,
                    max_requests_jitter=50,
                    keep_alive_timeout=30,
                    registry_ttl_sec=60,
                    tls_key_path="",
                    tls_cert_path="",
                    metrics=cli_metrics,
                )
            except (RuntimeError, Exception):
                pass
            return mock_fm

    def test_metrics_enabled_from_yaml_config(self):
        """start_server enables metrics when config has metrics.enabled=True,
        even though the CLI flag is False."""
        from types import SimpleNamespace

        metrics_cfg = SimpleNamespace(enabled=True)
        fs_cfg = SimpleNamespace(metrics=metrics_cfg)
        mock_store = MagicMock()
        mock_store.config = SimpleNamespace(feature_server=fs_cfg)

        mock_fm = self._call_start_server(mock_store, cli_metrics=False)
        mock_fm.build_metrics_flags.assert_called_once_with(metrics_cfg)
        mock_fm.start_metrics_server.assert_called_once()

    def test_cli_flag_enables_metrics_without_yaml_config(self):
        """start_server enables metrics when --metrics is passed even without
        any feature_server config section."""
        from types import SimpleNamespace

        mock_store = MagicMock()
        mock_store.config = SimpleNamespace(feature_server=None)

        mock_fm = self._call_start_server(mock_store, cli_metrics=True)
        mock_fm.build_metrics_flags.assert_called_once_with(None)
        mock_fm.start_metrics_server.assert_called_once()

    def test_metrics_not_started_when_both_disabled(self):
        """start_server does NOT start metrics when neither CLI nor config enables it."""
        from types import SimpleNamespace

        mock_store = MagicMock()
        mock_store.config = SimpleNamespace(
            feature_server=SimpleNamespace(metrics=SimpleNamespace(enabled=False)),
        )

        mock_fm = self._call_start_server(mock_store, cli_metrics=False)
        mock_fm.start_metrics_server.assert_not_called()

    def test_metrics_not_started_when_config_is_none(self):
        """start_server does NOT start metrics when feature_server config is None
        and CLI flag is also False."""
        from types import SimpleNamespace

        mock_store = MagicMock()
        mock_store.config = SimpleNamespace(feature_server=None)

        mock_fm = self._call_start_server(mock_store, cli_metrics=False)
        mock_fm.start_metrics_server.assert_not_called()


class TestTrackOnlineFeaturesEntities:
    def test_increments_request_count(self):
        before = online_features_request_count._value.get()
        track_online_features_entities(10)
        assert online_features_request_count._value.get() == before + 1

    def test_records_entity_count(self):
        before_count = online_features_entity_count._sum.get()
        track_online_features_entities(42)
        assert online_features_entity_count._sum.get() >= before_count + 42


class TestTrackPush:
    def test_increments_push_counter(self):
        before = push_request_count.labels(
            push_source="my_source", mode="online"
        )._value.get()
        track_push("my_source", "online")
        assert (
            push_request_count.labels(
                push_source="my_source", mode="online"
            )._value.get()
            == before + 1
        )


class TestTrackMaterialization:
    def test_success_counter(self):
        before = materialization_result_total.labels(
            feature_view="fv1", status="success"
        )._value.get()
        track_materialization("fv1", success=True, duration_seconds=1.5)
        assert (
            materialization_result_total.labels(
                feature_view="fv1", status="success"
            )._value.get()
            == before + 1
        )

    def test_failure_counter(self):
        before = materialization_result_total.labels(
            feature_view="fv2", status="failure"
        )._value.get()
        track_materialization("fv2", success=False, duration_seconds=0.5)
        assert (
            materialization_result_total.labels(
                feature_view="fv2", status="failure"
            )._value.get()
            == before + 1
        )

    def test_duration_histogram(self):
        before_sum = materialization_duration_seconds.labels(
            feature_view="fv3"
        )._sum.get()
        track_materialization("fv3", success=True, duration_seconds=3.7)
        after_sum = materialization_duration_seconds.labels(
            feature_view="fv3"
        )._sum.get()
        assert pytest.approx(after_sum - before_sum, abs=0.01) == 3.7


class TestUpdateFeatureFreshness:
    def test_sets_freshness_for_materialized_views(self):
        mock_fv = MagicMock()
        mock_fv.name = "test_fv"
        mock_fv.most_recent_end_time = datetime.now(tz=timezone.utc) - timedelta(
            minutes=5
        )

        mock_sfv = MagicMock()
        mock_sfv.name = "test_sfv"
        mock_sfv.most_recent_end_time = datetime.now(tz=timezone.utc) - timedelta(
            minutes=5
        )

        mock_store = MagicMock()
        mock_store.project = "test_project"
        mock_store.list_feature_views.return_value = [mock_fv]
        mock_store.list_stream_feature_views.return_value = [mock_sfv]

        update_feature_freshness(mock_store)

        staleness = feature_freshness_seconds.labels(
            feature_view="test_fv", project="test_project"
        )._value.get()
        assert 280 < staleness < 320

        sfv_staleness = feature_freshness_seconds.labels(
            feature_view="test_sfv", project="test_project"
        )._value.get()
        assert 280 < sfv_staleness < 320

    def test_skips_unmaterialized_views(self):
        mock_fv = MagicMock()
        mock_fv.name = "unmaterialized_fv"
        mock_fv.most_recent_end_time = None

        mock_store = MagicMock()
        mock_store.project = "test_project"
        mock_store.list_feature_views.return_value = [mock_fv]
        mock_store.list_stream_feature_views.return_value = []

        update_feature_freshness(mock_store)

    def test_handles_naive_datetime(self):
        mock_fv = MagicMock()
        mock_fv.name = "naive_fv"
        # Simulate a naive UTC datetime (no tzinfo), as Feast typically stores
        naive_utc_now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
        mock_fv.most_recent_end_time = naive_utc_now - timedelta(hours=1)

        mock_store = MagicMock()
        mock_store.project = "test_project"
        mock_store.list_feature_views.return_value = [mock_fv]
        mock_store.list_stream_feature_views.return_value = []

        update_feature_freshness(mock_store)

        staleness = feature_freshness_seconds.labels(
            feature_view="naive_fv", project="test_project"
        )._value.get()
        assert 3500 < staleness < 3700

    def test_handles_registry_errors_gracefully(self):
        mock_store = MagicMock()
        mock_store.list_feature_views.side_effect = Exception("registry down")

        update_feature_freshness(mock_store)


class TestResolveFeatureCounts:
    """Verify _resolve_feature_counts for both feature-ref lists and FeatureService."""

    def test_feature_ref_list(self):
        from feast.feature_server import _resolve_feature_counts

        refs = ["driver_fv:conv_rate", "driver_fv:acc_rate", "vehicle_fv:mileage"]
        feat_count, fv_count = _resolve_feature_counts(refs)
        assert feat_count == "3"
        assert fv_count == "2"

    def test_single_feature_view(self):
        from feast.feature_server import _resolve_feature_counts

        refs = ["fv1:a", "fv1:b", "fv1:c"]
        feat_count, fv_count = _resolve_feature_counts(refs)
        assert feat_count == "3"
        assert fv_count == "1"

    def test_empty_list(self):
        from feast.feature_server import _resolve_feature_counts

        feat_count, fv_count = _resolve_feature_counts([])
        assert feat_count == "0"
        assert fv_count == "0"

    def test_feature_service(self):
        from feast.feature_server import _resolve_feature_counts

        proj1 = MagicMock()
        proj1.features = [MagicMock(), MagicMock()]
        proj2 = MagicMock()
        proj2.features = [MagicMock()]

        fs_svc = MagicMock()
        fs_svc.feature_view_projections = [proj1, proj2]

        from feast.feature_service import FeatureService

        fs_svc.__class__ = FeatureService

        feat_count, fv_count = _resolve_feature_counts(fs_svc)
        assert feat_count == "3"
        assert fv_count == "2"


class TestFeatureServerMetricsIntegration:
    """Test that feature server endpoints record metrics."""

    @pytest.fixture
    def mock_fs_factory(self):
        from tests.foo_provider import FooProvider

        def builder(**async_support):
            provider = FooProvider.with_async_support(**async_support)
            fs = MagicMock()
            fs._get_provider.return_value = provider
            from feast.online_response import OnlineResponse
            from feast.protos.feast.serving.ServingService_pb2 import (
                GetOnlineFeaturesResponse,
            )

            empty_response = OnlineResponse(GetOnlineFeaturesResponse(results=[]))
            fs.get_online_features = MagicMock(return_value=empty_response)
            fs.push = MagicMock()
            fs.get_online_features_async = MagicMock(return_value=empty_response)
            fs.push_async = MagicMock()
            return fs

        return builder

    def test_get_online_features_records_metrics(self, mock_fs_factory):
        from fastapi.testclient import TestClient

        from feast.feature_server import get_app

        fs = mock_fs_factory(online_read=False)
        client = TestClient(get_app(fs))

        before_req = request_count.labels(
            endpoint="/get-online-features", status="success"
        )._value.get()
        before_entity = online_features_request_count._value.get()

        client.post(
            "/get-online-features",
            json={
                "features": ["fv:feat1"],
                "entities": {"id": [1, 2, 3]},
            },
        )

        assert (
            request_count.labels(
                endpoint="/get-online-features", status="success"
            )._value.get()
            == before_req + 1
        )
        assert online_features_request_count._value.get() == before_entity + 1

    @pytest.mark.parametrize(
        "features,expected_feat_count,expected_fv_count",
        [
            (["fv1:a"], "1", "1"),
            (["fv1:a", "fv1:b", "fv2:c"], "3", "2"),
            (
                ["fv1:a", "fv1:b", "fv2:c", "fv2:d", "fv3:e"],
                "5",
                "3",
            ),
        ],
        ids=["1_feat_1_fv", "3_feats_2_fvs", "5_feats_3_fvs"],
    )
    def test_latency_labels_with_varying_request_sizes(
        self, mock_fs_factory, features, expected_feat_count, expected_fv_count
    ):
        """Verify feature_count and feature_view_count labels change with request size."""
        from fastapi.testclient import TestClient

        from feast.feature_server import get_app

        fs = mock_fs_factory(online_read=False)
        client = TestClient(get_app(fs))

        label_set = dict(
            endpoint="/get-online-features",
            feature_count=expected_feat_count,
            feature_view_count=expected_fv_count,
        )
        before_sum = request_latency.labels(**label_set)._sum.get()

        client.post(
            "/get-online-features",
            json={
                "features": features,
                "entities": {"id": [1]},
            },
        )

        after_sum = request_latency.labels(**label_set)._sum.get()
        assert after_sum > before_sum

    def test_push_records_metrics(self, mock_fs_factory):
        from fastapi.testclient import TestClient

        from feast.feature_server import get_app
        from feast.utils import _utc_now

        fs = mock_fs_factory(online_write=False)
        client = TestClient(get_app(fs))

        before = push_request_count.labels(
            push_source="driver_locations_push", mode="online"
        )._value.get()

        client.post(
            "/push",
            json={
                "push_source_name": "driver_locations_push",
                "df": {
                    "driver_lat": [42.0],
                    "driver_long": ["42.0"],
                    "driver_id": [123],
                    "event_timestamp": [str(_utc_now())],
                    "created_timestamp": [str(_utc_now())],
                },
                "to": "online",
            },
        )

        assert (
            push_request_count.labels(
                push_source="driver_locations_push", mode="online"
            )._value.get()
            == before + 1
        )


class TestBuildMetricsFlags:
    """Verify build_metrics_flags correctly maps MetricsConfig to _MetricsFlags."""

    def test_no_config_enables_all(self):
        from feast.metrics import build_metrics_flags

        flags = build_metrics_flags(None)
        assert flags.enabled is True
        assert flags.resource is True
        assert flags.request is True
        assert flags.online_features is True
        assert flags.push is True
        assert flags.materialization is True
        assert flags.freshness is True

    def test_selective_disable(self):
        from types import SimpleNamespace

        from feast.metrics import build_metrics_flags

        mc = SimpleNamespace(
            enabled=True,
            resource=True,
            request=False,
            online_features=True,
            push=False,
            materialization=True,
            freshness=False,
        )
        flags = build_metrics_flags(mc)
        assert flags.enabled is True
        assert flags.resource is True
        assert flags.request is False
        assert flags.online_features is True
        assert flags.push is False
        assert flags.materialization is True
        assert flags.freshness is False

    def test_all_categories_disabled(self):
        from types import SimpleNamespace

        from feast.metrics import build_metrics_flags

        mc = SimpleNamespace(
            enabled=True,
            resource=False,
            request=False,
            online_features=False,
            push=False,
            materialization=False,
            freshness=False,
        )
        flags = build_metrics_flags(mc)
        assert flags.enabled is True
        assert flags.resource is False
        assert flags.request is False


class TestCleanupMultiprocessDir:
    """Verify the atexit handler only deletes the temp dir in the owner process."""

    def test_cleanup_skipped_in_forked_child(self, tmp_path):
        """Simulate a forked worker: _owns_mp_dir=True but _owner_pid != current PID."""
        import feast.metrics as m

        original_dir = m._prometheus_mp_dir
        original_owns = m._owns_mp_dir
        original_pid = m._owner_pid

        fake_dir = tmp_path / "feast_metrics_test"
        fake_dir.mkdir()

        m._prometheus_mp_dir = str(fake_dir)
        m._owns_mp_dir = True
        m._owner_pid = -1  # Different from os.getpid()

        try:
            m._cleanup_multiprocess_dir()
            assert fake_dir.exists(), (
                "Directory should NOT be deleted when _owner_pid != os.getpid()"
            )
        finally:
            m._prometheus_mp_dir = original_dir
            m._owns_mp_dir = original_owns
            m._owner_pid = original_pid

    def test_cleanup_runs_in_owner_process(self, tmp_path):
        """The owner process (matching PID) should delete the directory."""
        import os

        import feast.metrics as m

        original_dir = m._prometheus_mp_dir
        original_owns = m._owns_mp_dir
        original_pid = m._owner_pid

        fake_dir = tmp_path / "feast_metrics_test"
        fake_dir.mkdir()

        m._prometheus_mp_dir = str(fake_dir)
        m._owns_mp_dir = True
        m._owner_pid = os.getpid()

        try:
            m._cleanup_multiprocess_dir()
            assert not fake_dir.exists(), (
                "Directory SHOULD be deleted when _owner_pid == os.getpid()"
            )
        finally:
            m._prometheus_mp_dir = original_dir
            m._owns_mp_dir = original_owns
            m._owner_pid = original_pid


class TestTrackOnlineStoreRead:
    """Tests for the online store read duration metric."""

    def test_records_duration(self):
        before_sum = online_store_read_duration_seconds._sum.get()

        track_online_store_read(0.123)

        assert online_store_read_duration_seconds._sum.get() >= before_sum + 0.123

    def test_noop_when_online_features_disabled(self):
        import feast.metrics as m

        m._config = m._MetricsFlags(enabled=True, online_features=False)

        before_sum = online_store_read_duration_seconds._sum.get()

        track_online_store_read(0.5)

        assert online_store_read_duration_seconds._sum.get() == before_sum

        m._config = m._MetricsFlags(
            enabled=True,
            resource=True,
            request=True,
            online_features=True,
            push=True,
            materialization=True,
            freshness=True,
        )


class TestTrackTransformation:
    """Tests for the ODFV transformation duration metric."""

    def test_records_python_mode(self):
        labels = ("my_odfv", "python")
        before = transformation_duration_seconds._metrics.get(labels, None)
        before_sum = before._sum.get() if before else 0.0

        track_transformation("my_odfv", "python", 0.042)

        sample = transformation_duration_seconds._metrics[labels]
        assert sample._sum.get() >= before_sum + 0.042

    def test_records_pandas_mode(self):
        labels = ("my_odfv", "pandas")
        before = transformation_duration_seconds._metrics.get(labels, None)
        before_sum = before._sum.get() if before else 0.0

        track_transformation("my_odfv", "pandas", 0.15)

        sample = transformation_duration_seconds._metrics[labels]
        assert sample._sum.get() >= before_sum + 0.15

    def test_noop_when_online_features_disabled(self):
        import feast.metrics as m

        m._config = m._MetricsFlags(enabled=True, online_features=False)

        labels = ("disabled_odfv", "python")
        before = transformation_duration_seconds._metrics.get(labels, None)
        before_sum = before._sum.get() if before else 0.0

        track_transformation("disabled_odfv", "python", 1.0)

        sample = transformation_duration_seconds._metrics.get(labels, None)
        after_sum = sample._sum.get() if sample else 0.0
        assert after_sum == before_sum

        m._config = m._MetricsFlags(
            enabled=True,
            resource=True,
            request=True,
            online_features=True,
            push=True,
            materialization=True,
            freshness=True,
        )

    def test_multiple_odfvs_tracked_independently(self):
        labels_a = ("odfv_a", "python")
        labels_b = ("odfv_b", "pandas")
        before_a = transformation_duration_seconds._metrics.get(labels_a, None)
        before_a_sum = before_a._sum.get() if before_a else 0.0
        before_b = transformation_duration_seconds._metrics.get(labels_b, None)
        before_b_sum = before_b._sum.get() if before_b else 0.0

        track_transformation("odfv_a", "python", 0.01)
        track_transformation("odfv_b", "pandas", 0.05)

        sample_a = transformation_duration_seconds._metrics[labels_a]
        sample_b = transformation_duration_seconds._metrics[labels_b]
        assert sample_a._sum.get() >= before_a_sum + 0.01
        assert sample_b._sum.get() >= before_b_sum + 0.05


class TestTrackWriteTransformation:
    """Tests for the write-path ODFV transformation duration metric."""

    def test_records_python_mode(self):
        labels = ("write_odfv", "python")
        before = write_transformation_duration_seconds._metrics.get(labels, None)
        before_sum = before._sum.get() if before else 0.0

        track_write_transformation("write_odfv", "python", 0.033)

        sample = write_transformation_duration_seconds._metrics[labels]
        assert sample._sum.get() >= before_sum + 0.033

    def test_records_pandas_mode(self):
        labels = ("write_odfv", "pandas")
        before = write_transformation_duration_seconds._metrics.get(labels, None)
        before_sum = before._sum.get() if before else 0.0

        track_write_transformation("write_odfv", "pandas", 0.12)

        sample = write_transformation_duration_seconds._metrics[labels]
        assert sample._sum.get() >= before_sum + 0.12

    def test_noop_when_online_features_disabled(self):
        import feast.metrics as m

        m._config = m._MetricsFlags(enabled=True, online_features=False)

        labels = ("disabled_write_odfv", "python")
        before = write_transformation_duration_seconds._metrics.get(labels, None)
        before_sum = before._sum.get() if before else 0.0

        track_write_transformation("disabled_write_odfv", "python", 1.0)

        sample = write_transformation_duration_seconds._metrics.get(labels, None)
        after_sum = sample._sum.get() if sample else 0.0
        assert after_sum == before_sum

        m._config = m._MetricsFlags(
            enabled=True,
            resource=True,
            request=True,
            online_features=True,
            push=True,
            materialization=True,
            freshness=True,
        )

    def test_separate_from_read_transform_metric(self):
        """Write and read transform metrics are independent histograms."""
        read_labels = ("shared_odfv", "python")
        write_labels = ("shared_odfv", "python")

        read_before = transformation_duration_seconds._metrics.get(read_labels, None)
        read_before_sum = read_before._sum.get() if read_before else 0.0
        write_before = write_transformation_duration_seconds._metrics.get(
            write_labels, None
        )
        write_before_sum = write_before._sum.get() if write_before else 0.0

        track_transformation("shared_odfv", "python", 0.01)
        track_write_transformation("shared_odfv", "python", 0.05)

        read_after = transformation_duration_seconds._metrics[read_labels]
        write_after = write_transformation_duration_seconds._metrics[write_labels]

        read_delta = read_after._sum.get() - read_before_sum
        write_delta = write_after._sum.get() - write_before_sum

        assert abs(read_delta - 0.01) < 0.001
        assert abs(write_delta - 0.05) < 0.001
