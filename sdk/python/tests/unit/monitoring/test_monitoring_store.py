from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from feast.monitoring.monitoring_store import (
    _FEATURE_METRICS_TABLE,
    _FEATURE_SERVICE_METRICS_TABLE,
    _FEATURE_VIEW_METRICS_TABLE,
    MonitoringStore,
)


def _mock_pg_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


@pytest.fixture
def store():
    return MonitoringStore(MagicMock())


class TestEnsureTables:
    @patch("feast.monitoring.monitoring_store._get_conn")
    def test_creates_three_tables(self, mock_get_conn, store):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn

        store.ensure_tables()

        assert mock_cursor.execute.call_count == 3
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any(_FEATURE_METRICS_TABLE in c for c in calls)
        assert any(_FEATURE_VIEW_METRICS_TABLE in c for c in calls)
        assert any(_FEATURE_SERVICE_METRICS_TABLE in c for c in calls)
        mock_conn.commit.assert_called_once()


class TestSaveAndGetMetrics:
    @patch("feast.monitoring.monitoring_store._get_conn")
    def test_save_feature_metrics_upsert(self, mock_get_conn, store):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn

        metrics = [
            {
                "project_id": "proj",
                "feature_view_name": "fv1",
                "feature_name": "feat1",
                "metric_date": date(2025, 1, 1),
                "granularity": "daily",
                "data_source_type": "batch",
                "computed_at": datetime.now(timezone.utc),
                "is_baseline": False,
                "feature_type": "numeric",
                "row_count": 100,
                "null_count": 5,
                "null_rate": 0.05,
                "mean": 10.0,
                "stddev": 2.0,
                "min_val": 1.0,
                "max_val": 20.0,
                "p50": 10.0,
                "p75": 15.0,
                "p90": 18.0,
                "p95": 19.0,
                "p99": 19.9,
                "histogram": {"bins": [0, 10, 20], "counts": [40, 60]},
            }
        ]
        store.save_feature_metrics(metrics)

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch("feast.monitoring.monitoring_store._get_conn")
    def test_save_empty_list_is_noop(self, mock_get_conn, store):
        store.save_feature_metrics([])
        mock_get_conn.assert_not_called()

    @patch("feast.monitoring.monitoring_store._get_conn")
    def test_get_feature_metrics_with_granularity_filter(self, mock_get_conn, store):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn
        mock_cursor.fetchall.return_value = []

        store.get_feature_metrics(
            project="proj",
            feature_view_name="fv1",
            granularity="weekly",
            data_source_type="batch",
        )

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert "weekly" in params
        assert "batch" in params

    @patch("feast.monitoring.monitoring_store._get_conn")
    def test_get_feature_metrics_date_range(self, mock_get_conn, store):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn
        mock_cursor.fetchall.return_value = []

        store.get_feature_metrics(
            project="proj",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
        )

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert date(2025, 1, 1) in params
        assert date(2025, 1, 31) in params


class TestBaseline:
    @patch("feast.monitoring.monitoring_store._get_conn")
    def test_get_baseline_filters_is_baseline(self, mock_get_conn, store):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn
        mock_cursor.fetchall.return_value = []

        store.get_baseline(project="proj", feature_view_name="fv1")

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert True in params

    @patch("feast.monitoring.monitoring_store._get_conn")
    def test_clear_baseline(self, mock_get_conn, store):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn

        store.clear_baseline(project="proj", feature_view_name="fv1")

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert "proj" in params
        assert "fv1" in params
        mock_conn.commit.assert_called_once()


class TestHistogramSerialization:
    @patch("feast.monitoring.monitoring_store._get_conn")
    def test_histogram_json_serialized_on_save(self, mock_get_conn, store):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn

        histogram_data = {"bins": [0.0, 5.0, 10.0], "counts": [3, 7]}
        metrics = [
            {
                "project_id": "proj",
                "feature_view_name": "fv1",
                "feature_name": "feat1",
                "metric_date": date(2025, 1, 1),
                "granularity": "daily",
                "data_source_type": "batch",
                "computed_at": datetime.now(timezone.utc),
                "is_baseline": False,
                "feature_type": "numeric",
                "row_count": 10,
                "null_count": 0,
                "null_rate": 0.0,
                "mean": 5.0,
                "stddev": 2.0,
                "min_val": 0.0,
                "max_val": 10.0,
                "p50": 5.0,
                "p75": 7.5,
                "p90": 9.0,
                "p95": 9.5,
                "p99": 9.9,
                "histogram": histogram_data,
            }
        ]
        store.save_feature_metrics(metrics)

        import json

        call_values = mock_cursor.execute.call_args[0][1]
        assert json.dumps(histogram_data) in [
            v for v in call_values if isinstance(v, str)
        ]
