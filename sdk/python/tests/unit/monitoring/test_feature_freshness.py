from datetime import datetime, timezone
from unittest.mock import MagicMock

from feast.monitoring.monitoring_service import (
    MonitoringService,
    _newest_event_in_window,
)
from feast.types import PrimitiveFeastType


class TestNewestEventInWindow:
    def test_uses_source_max_when_inside_window(self):
        max_ts = datetime(2025, 3, 26, 14, 30, tzinfo=timezone.utc)
        start = datetime(2025, 3, 25, 14, 30, tzinfo=timezone.utc)
        end = datetime(2025, 3, 26, 14, 30, tzinfo=timezone.utc)
        assert _newest_event_in_window(max_ts, start, end) == max_ts

    def test_clamps_to_window_end_when_source_is_newer(self):
        max_ts = datetime(2025, 4, 1, tzinfo=timezone.utc)
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 15, tzinfo=timezone.utc)
        assert _newest_event_in_window(max_ts, start, end) == end

    def test_returns_none_when_source_is_before_window(self):
        max_ts = datetime(2024, 12, 1, tzinfo=timezone.utc)
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 15, tzinfo=timezone.utc)
        assert _newest_event_in_window(max_ts, start, end) is None


def test_auto_compute_persists_newest_event_timestamp():
    field = MagicMock()
    field.name = "conv_rate"
    field.dtype = PrimitiveFeastType.FLOAT64
    fv = MagicMock()
    fv.name = "driver_stats"
    fv.features = [field]
    fv.entities = []
    fv.batch_source.timestamp_field = "event_timestamp"
    fv.batch_source.created_timestamp_column = ""

    store = MagicMock()
    store.config.project = "test_project"
    store.registry.list_feature_views.return_value = [fv]
    store.registry.list_entities.return_value = []
    store.registry.list_feature_services.return_value = []
    store.registry.get_feature_view.return_value = fv

    newest = datetime(2025, 3, 27, 14, 30, tzinfo=timezone.utc)
    provider = store._get_provider.return_value
    provider.offline_store.get_monitoring_max_timestamp.side_effect = None
    provider.offline_store.get_monitoring_max_timestamp.return_value = newest
    provider.offline_store.compute_monitoring_metrics.side_effect = None
    provider.offline_store.compute_monitoring_metrics.return_value = [
        {
            "feature_name": "conv_rate",
            "feature_type": "numeric",
            "row_count": 5,
            "null_count": 0,
            "null_rate": 0.0,
            "mean": 0.5,
            "stddev": 0.2,
            "min_val": 0.1,
            "max_val": 0.9,
            "p50": 0.5,
            "p75": 0.7,
            "p90": 0.9,
            "p95": 0.9,
            "p99": 0.9,
            "histogram": None,
        },
    ]
    provider.offline_store.query_monitoring_metrics.return_value = []

    result = MonitoringService(store).auto_compute(project="test_project")
    assert result["status"] == "completed"

    feature_saves = [
        call
        for call in provider.offline_store.save_monitoring_metrics.call_args_list
        if call.args[1] == "feature"
    ]
    assert feature_saves
    saved = feature_saves[0].args[2]
    assert all(row["max_event_timestamp"] == newest for row in saved)
