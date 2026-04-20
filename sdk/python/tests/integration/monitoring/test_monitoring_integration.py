"""Integration tests for the monitoring feature.

Tests cover:
- Auto-compute (all granularities from source timestamps)
- Compute baseline (idempotent)
- Transient compute
- DQM job lifecycle
- CLI commands
- REST API endpoints
- RBAC enforcement
- Compute engine dispatch (SQL push-down vs Python fallback)
- Log source monitoring (feature serving logs)
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from click.testing import CliRunner

from feast.monitoring.monitoring_service import VALID_GRANULARITIES, MonitoringService
from feast.types import PrimitiveFeastType

# ------------------------------------------------------------------ #
#  Shared helpers
# ------------------------------------------------------------------ #


def _mock_pg_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def _make_feature_field(name, dtype):
    field = MagicMock()
    field.name = name
    field.dtype = dtype
    return field


def _make_feature_view(name, features, entities=None, batch_source=None):
    fv = MagicMock()
    fv.name = name
    fv.features = features
    fv.entities = entities or []
    if batch_source is None:
        batch_source = MagicMock()
        batch_source.timestamp_field = "event_timestamp"
        batch_source.created_timestamp_column = ""
    fv.batch_source = batch_source
    return fv


def _make_feature_service(name, fv_names, logging_config=None, feature_map=None):
    """Create a mock FeatureService.

    Args:
        feature_map: optional dict mapping view_name -> list of feature names.
            Used to build realistic projections with features and name_to_use().
    """
    fs = MagicMock()
    fs.name = name
    fs.feature_view_projections = [MagicMock(name=n) for n in fv_names]
    for proj, n in zip(fs.feature_view_projections, fv_names):
        proj.name = n
        proj.name_to_use.return_value = n
        if feature_map and n in feature_map:
            feats = []
            for fname in feature_map[n]:
                f = MagicMock()
                f.name = fname
                feats.append(f)
            proj.features = feats
        else:
            proj.features = []
    fs.logging_config = logging_config
    return fs


def _make_logging_config_with_source(log_table_schema):
    """Create a mock LoggingConfig whose destination.to_data_source() returns a DataSource."""
    logging_config = MagicMock()
    mock_data_source = MagicMock()
    mock_data_source.timestamp_field = "__log_timestamp"
    mock_data_source.created_timestamp_column = ""
    logging_config.destination.to_data_source.return_value = mock_data_source
    return logging_config, mock_data_source


def _make_mock_store(feature_views, feature_services=None):
    """Create a mock FeatureStore with offline store that uses Python fallback."""
    store = MagicMock()
    store.project = "test_project"
    store.config.project = "test_project"
    store.config.offline_store = MagicMock()

    store.registry.list_feature_views.return_value = feature_views
    store.registry.list_entities.return_value = []
    store.registry.list_feature_services.return_value = feature_services or []

    if feature_views:
        store.registry.get_feature_view.return_value = feature_views[0]

    if feature_services:
        store.registry.get_feature_service.return_value = feature_services[0]

    arrow_table = pa.table(
        {
            "conv_rate": [0.1, 0.5, 0.9, 0.3, 0.7],
            "acc_rate": [0.8, 0.6, 0.4, 0.9, 0.2],
            "city": ["NYC", "LA", "NYC", "SF", "LA"],
            "event_timestamp": [
                datetime(2025, 3, 25, tzinfo=timezone.utc),
                datetime(2025, 3, 26, tzinfo=timezone.utc),
                datetime(2025, 3, 26, tzinfo=timezone.utc),
                datetime(2025, 3, 27, tzinfo=timezone.utc),
                datetime(2025, 3, 27, tzinfo=timezone.utc),
            ],
        }
    )

    mock_retrieval = MagicMock()
    mock_retrieval.to_arrow.return_value = arrow_table

    provider = MagicMock()
    provider.offline_store.pull_all_from_table_or_query.return_value = mock_retrieval
    provider.offline_store.compute_monitoring_metrics.side_effect = NotImplementedError
    provider.offline_store.get_monitoring_max_timestamp.side_effect = (
        NotImplementedError
    )

    # Storage methods: no-op by default (save does nothing, query returns [])
    provider.offline_store.ensure_monitoring_tables.return_value = None
    provider.offline_store.save_monitoring_metrics.return_value = None
    provider.offline_store.query_monitoring_metrics.return_value = []
    provider.offline_store.clear_monitoring_baseline.return_value = None

    store._get_provider.return_value = provider

    return store


# ------------------------------------------------------------------ #
#  Test: Auto-compute
# ------------------------------------------------------------------ #


class TestAutoCompute:
    def test_auto_compute_all_granularities(self):
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        result = svc.auto_compute(project="test_project")

        assert result["status"] == "completed"
        assert result["computed_feature_views"] == 1
        assert len(result["granularities"]) == len(VALID_GRANULARITIES)
        for g in VALID_GRANULARITIES:
            assert g in result["granularities"]

        provider = store._get_provider.return_value
        provider.offline_store.save_monitoring_metrics.assert_called()

    def test_auto_compute_specific_view(self):
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        result = svc.auto_compute(
            project="test_project",
            feature_view_name="driver_stats",
        )

        assert result["status"] == "completed"
        assert result["computed_feature_views"] == 1


# ------------------------------------------------------------------ #
#  Test: Compute baseline
# ------------------------------------------------------------------ #


class TestComputeBaseline:
    def test_compute_baseline_for_new_features(self):
        fv = _make_feature_view(
            "driver_stats",
            [
                _make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64),
                _make_feature_field("city", PrimitiveFeastType.STRING),
            ],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        result = svc.compute_baseline(project="test_project")

        assert result["status"] == "completed"
        assert result["is_baseline"] is True
        assert result["computed_features"] == 2

        provider = store._get_provider.return_value
        provider.offline_store.clear_monitoring_baseline.assert_called()
        provider.offline_store.save_monitoring_metrics.assert_called()

    def test_baseline_idempotent_skips_existing(self):
        fv = _make_feature_view(
            "driver_stats",
            [
                _make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64),
                _make_feature_field("acc_rate", PrimitiveFeastType.FLOAT64),
            ],
        )
        store = _make_mock_store([fv])

        # Simulate conv_rate already has baseline via query_monitoring_metrics
        provider = store._get_provider.return_value
        provider.offline_store.query_monitoring_metrics.return_value = [
            {
                "project_id": "test_project",
                "feature_view_name": "driver_stats",
                "feature_name": "conv_rate",
                "metric_date": "2025-01-01",
                "granularity": "daily",
                "data_source_type": "batch",
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "is_baseline": True,
                "feature_type": "numeric",
                "row_count": 100,
                "null_count": 0,
                "null_rate": 0.0,
                "mean": 5.0,
                "stddev": 1.0,
                "min_val": 0.0,
                "max_val": 10.0,
                "p50": 5.0,
                "p75": 7.5,
                "p90": 9.0,
                "p95": 9.5,
                "p99": 9.9,
                "histogram": None,
            },
        ]

        svc = MonitoringService(store)
        result = svc.compute_baseline(project="test_project")

        # Only acc_rate should be computed (conv_rate already has baseline)
        assert result["computed_features"] == 1


# ------------------------------------------------------------------ #
#  Test: Transient compute
# ------------------------------------------------------------------ #


class TestTransientCompute:
    def test_transient_returns_metrics_without_saving(self):
        fv = _make_feature_view(
            "driver_stats",
            [
                _make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64),
                _make_feature_field("city", PrimitiveFeastType.STRING),
            ],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        result = svc.compute_transient(
            project="test_project",
            feature_view_name="driver_stats",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 15),
        )

        assert result["status"] == "completed"
        assert result["start_date"] == "2025-01-01"
        assert result["end_date"] == "2025-01-15"
        assert len(result["metrics"]) == 2

        # Transient should NOT call save
        provider = store._get_provider.return_value
        provider.offline_store.save_monitoring_metrics.assert_not_called()

    def test_transient_empty_features(self):
        fv = _make_feature_view(
            "fv",
            [_make_feature_field("ts", PrimitiveFeastType.UNIX_TIMESTAMP)],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        result = svc.compute_transient(
            project="test_project",
            feature_view_name="fv",
        )
        assert result["metrics"] == []


# ------------------------------------------------------------------ #
#  Test: DQM Job Manager
# ------------------------------------------------------------------ #


class TestDQMJobManager:
    @patch("feast.monitoring.dqm_job_manager._get_conn")
    def test_submit_and_get_job(self, mock_get_conn):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn

        from feast.monitoring.dqm_job_manager import DQMJobManager

        mgr = DQMJobManager(MagicMock())
        job_id = mgr.submit(
            project="test_project",
            job_type="auto_compute",
            feature_view_name="driver_stats",
        )

        assert job_id is not None
        assert len(job_id) == 36  # UUID format
        mock_cursor.execute.assert_called_once()

    @patch("feast.monitoring.dqm_job_manager._get_conn")
    def test_update_status(self, mock_get_conn):
        mock_conn, mock_cursor = _mock_pg_conn()
        mock_get_conn.return_value = mock_conn

        from feast.monitoring.dqm_job_manager import JOB_STATUS_RUNNING, DQMJobManager

        mgr = DQMJobManager(MagicMock())
        mgr.update_status("test-job-id", JOB_STATUS_RUNNING)

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()


# ------------------------------------------------------------------ #
#  Test: CLI
# ------------------------------------------------------------------ #


class TestComputeMetricsCLI:
    def test_help(self):
        from feast.cli.monitor import monitor_cmd

        runner = CliRunner()
        result = runner.invoke(monitor_cmd, ["run", "--help"])
        assert result.exit_code == 0
        assert "--granularity" in result.output
        assert "--set-baseline" in result.output
        assert "--feature-view" in result.output

    @patch("feast.cli.monitor.create_feature_store")
    @patch("feast.monitoring.monitoring_service.MonitoringService.auto_compute")
    def test_run_auto_mode(self, mock_auto, mock_create_store):
        from feast.cli.monitor import monitor_cmd

        mock_store = MagicMock()
        mock_store.project = "proj"
        mock_create_store.return_value = mock_store

        mock_auto.return_value = {
            "status": "completed",
            "computed_feature_views": 2,
            "computed_features": 5,
            "granularities": list(VALID_GRANULARITIES),
            "duration_ms": 1200,
        }

        runner = CliRunner()
        result = runner.invoke(monitor_cmd, ["run"])

        assert result.exit_code == 0
        assert "Auto-computing" in result.output
        assert "Features computed: 5" in result.output
        mock_auto.assert_called_once()

    @patch("feast.cli.monitor.create_feature_store")
    @patch("feast.monitoring.monitoring_service.MonitoringService.compute_metrics")
    def test_run_explicit_granularity(self, mock_compute, mock_create_store):
        from feast.cli.monitor import monitor_cmd

        mock_store = MagicMock()
        mock_store.project = "proj"
        mock_create_store.return_value = mock_store

        mock_compute.return_value = {
            "status": "completed",
            "granularity": "weekly",
            "computed_features": 3,
            "computed_feature_views": 1,
            "computed_feature_services": 1,
            "metric_dates": ["2025-01-01"],
            "duration_ms": 500,
        }

        runner = CliRunner()
        result = runner.invoke(
            monitor_cmd,
            [
                "run",
                "--granularity",
                "weekly",
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-07",
            ],
        )

        assert result.exit_code == 0
        assert "Granularity: weekly" in result.output


# ------------------------------------------------------------------ #
#  Test: REST API
# ------------------------------------------------------------------ #


class TestRESTEndpoints:
    @pytest.fixture
    def app(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from feast.api.registry.rest.monitoring import get_monitoring_router

        mock_handler = MagicMock()
        mock_server = MagicMock()

        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        mock_server.store = _make_mock_store([fv])

        app = FastAPI()
        app.include_router(get_monitoring_router(mock_handler, mock_server))

        return TestClient(app), mock_server

    @patch("feast.monitoring.dqm_job_manager._get_conn")
    @patch("feast.api.registry.rest.monitoring.assert_permissions")
    def test_auto_compute_endpoint(self, mock_perms, mock_job_conn, app):
        client, _ = app

        mock_job_conn_val, mock_job_cursor = _mock_pg_conn()
        mock_job_conn.return_value = mock_job_conn_val
        mock_job_cursor.fetchone.return_value = (
            "test-job-id",
            "test_project",
            None,
            "auto_compute",
            "pending",
            None,
            datetime.now(timezone.utc),
            None,
            None,
            None,
            None,
        )

        response = client.post(
            "/monitoring/auto_compute",
            json={"project": "test_project"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"
        assert "job_id" in data

    @patch("feast.api.registry.rest.monitoring.assert_permissions")
    def test_transient_compute_endpoint(self, mock_perms, app):
        client, _ = app

        response = client.post(
            "/monitoring/compute/transient",
            json={
                "project": "test_project",
                "feature_view_name": "driver_stats",
                "start_date": "2025-01-05",
                "end_date": "2025-01-20",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"
        assert len(data["metrics"]) >= 1

    @patch("feast.api.registry.rest.monitoring.assert_permissions")
    def test_get_metrics_with_granularity(self, mock_perms, app):
        client, _ = app

        response = client.get(
            "/monitoring/metrics/features",
            params={"project": "test_project", "granularity": "weekly"},
        )

        assert response.status_code == 200

    @patch("feast.api.registry.rest.monitoring.assert_permissions")
    def test_get_timeseries(self, mock_perms, app):
        client, _ = app

        response = client.get(
            "/monitoring/metrics/timeseries",
            params={
                "project": "test_project",
                "feature_view_name": "driver_stats",
                "granularity": "daily",
            },
        )

        assert response.status_code == 200


# ------------------------------------------------------------------ #
#  Test: RBAC enforcement
# ------------------------------------------------------------------ #


class TestRBACEnforcement:
    @pytest.fixture
    def app(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from feast.api.registry.rest.monitoring import get_monitoring_router

        mock_handler = MagicMock()
        mock_server = MagicMock()

        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        mock_server.store = _make_mock_store([fv])

        app = FastAPI()
        app.include_router(get_monitoring_router(mock_handler, mock_server))

        return TestClient(app), mock_server

    @patch("feast.monitoring.dqm_job_manager._get_conn")
    @patch("feast.api.registry.rest.monitoring.assert_permissions")
    def test_compute_requires_update(self, mock_perms, mock_job_conn, app):
        client, _ = app
        mock_conn, _ = _mock_pg_conn()
        mock_job_conn.return_value = mock_conn

        from feast.permissions.action import AuthzedAction

        client.post(
            "/monitoring/compute",
            json={
                "project": "test_project",
                "feature_view_name": "driver_stats",
            },
        )

        mock_perms.assert_called()
        call_args = mock_perms.call_args
        assert AuthzedAction.UPDATE in call_args.kwargs.get(
            "actions", call_args[1].get("actions", [])
        )

    @patch("feast.api.registry.rest.monitoring.assert_permissions")
    def test_transient_requires_describe(self, mock_perms, app):
        client, _ = app

        from feast.permissions.action import AuthzedAction

        client.post(
            "/monitoring/compute/transient",
            json={
                "project": "test_project",
                "feature_view_name": "driver_stats",
            },
        )

        mock_perms.assert_called()
        call_args = mock_perms.call_args
        assert AuthzedAction.DESCRIBE in call_args.kwargs.get(
            "actions", call_args[1].get("actions", [])
        )

    @patch("feast.api.registry.rest.monitoring.assert_permissions")
    def test_read_requires_describe(self, mock_perms, app):
        client, _ = app

        from feast.permissions.action import AuthzedAction

        client.get(
            "/monitoring/metrics/features",
            params={"project": "test_project", "feature_view_name": "driver_stats"},
        )

        mock_perms.assert_called()
        call_args = mock_perms.call_args
        assert AuthzedAction.DESCRIBE in call_args.kwargs.get(
            "actions", call_args[1].get("actions", [])
        )


# ------------------------------------------------------------------ #
#  Test: SQL push-down dispatch
# ------------------------------------------------------------------ #


class TestComputeEngineDispatch:
    """Verify that MonitoringService prefers SQL push-down and falls back
    to Python-based computation when the offline store doesn't support it."""

    def _make_store_with_pushdown(self, pushdown_result):
        """Create a mock store where the offline store supports push-down."""
        fv = _make_feature_view(
            "driver_stats",
            [
                _make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64),
                _make_feature_field("city", PrimitiveFeastType.STRING),
            ],
        )
        store = _make_mock_store([fv])
        provider = store._get_provider.return_value
        provider.offline_store.compute_monitoring_metrics.side_effect = None
        provider.offline_store.compute_monitoring_metrics.return_value = pushdown_result
        provider.offline_store.get_monitoring_max_timestamp.side_effect = None
        provider.offline_store.get_monitoring_max_timestamp.return_value = datetime(
            2025, 3, 27, tzinfo=timezone.utc
        )
        return store, fv

    def test_uses_sql_pushdown_when_available(self):
        """When the offline store supports compute_monitoring_metrics,
        pull_all_from_table_or_query should NOT be called."""
        sql_result = [
            {
                "feature_name": "conv_rate",
                "feature_type": "numeric",
                "row_count": 100,
                "null_count": 2,
                "null_rate": 0.02,
                "mean": 0.5,
                "stddev": 0.2,
                "min_val": 0.0,
                "max_val": 1.0,
                "p50": 0.5,
                "p75": 0.75,
                "p90": 0.9,
                "p95": 0.95,
                "p99": 0.99,
                "histogram": {
                    "bins": [0.0, 0.5, 1.0],
                    "counts": [50, 50],
                    "bin_width": 0.5,
                },
            },
        ]
        store, _ = self._make_store_with_pushdown(sql_result)
        svc = MonitoringService(store)

        result = svc.compute_transient(
            project="test_project",
            feature_view_name="driver_stats",
            feature_names=["conv_rate"],
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 15),
        )

        assert result["status"] == "completed"
        assert len(result["metrics"]) == 1
        assert result["metrics"][0]["mean"] == 0.5

        provider = store._get_provider.return_value
        provider.offline_store.compute_monitoring_metrics.assert_called_once()
        provider.offline_store.pull_all_from_table_or_query.assert_not_called()

    def test_falls_back_to_python_when_not_supported(self):
        """When compute_monitoring_metrics raises NotImplementedError,
        the service falls back to pulling data + Python compute."""
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        store = _make_mock_store([fv])

        svc = MonitoringService(store)
        result = svc.compute_transient(
            project="test_project",
            feature_view_name="driver_stats",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 15),
        )

        assert result["status"] == "completed"
        assert len(result["metrics"]) == 1
        assert result["metrics"][0]["feature_name"] == "conv_rate"

        provider = store._get_provider.return_value
        provider.offline_store.pull_all_from_table_or_query.assert_called()

    def test_auto_compute_uses_pushdown_for_max_timestamp(self):
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        store = _make_mock_store([fv])
        provider = store._get_provider.return_value

        provider.offline_store.get_monitoring_max_timestamp.side_effect = None
        provider.offline_store.get_monitoring_max_timestamp.return_value = datetime(
            2025, 3, 27, tzinfo=timezone.utc
        )
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

        svc = MonitoringService(store)
        result = svc.auto_compute(project="test_project")

        assert result["status"] == "completed"
        provider.offline_store.get_monitoring_max_timestamp.assert_called()
        provider.offline_store.compute_monitoring_metrics.assert_called()
        provider.offline_store.pull_all_from_table_or_query.assert_not_called()


# ------------------------------------------------------------------ #
#  Test: Native storage dispatch
# ------------------------------------------------------------------ #


class TestNativeStorageDispatch:
    """Verify that MonitoringService uses OfflineStore for all storage
    operations (save, query, clear_baseline, ensure_tables)."""

    def test_save_goes_through_offline_store(self):
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        svc.compute_metrics(
            project="test_project",
            granularity="daily",
        )

        provider = store._get_provider.return_value
        provider.offline_store.ensure_monitoring_tables.assert_called()
        provider.offline_store.save_monitoring_metrics.assert_called()

        save_calls = provider.offline_store.save_monitoring_metrics.call_args_list
        metric_types_saved = {c[0][1] for c in save_calls}
        assert "feature" in metric_types_saved
        assert "feature_view" in metric_types_saved

    def test_query_goes_through_offline_store(self):
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        svc.get_feature_metrics(project="test_project", granularity="daily")

        provider = store._get_provider.return_value
        provider.offline_store.query_monitoring_metrics.assert_called()
        call_args = provider.offline_store.query_monitoring_metrics.call_args
        assert call_args[1]["metric_type"] == "feature"

    def test_baseline_clear_goes_through_offline_store(self):
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        svc.compute_baseline(project="test_project")

        provider = store._get_provider.return_value
        provider.offline_store.clear_monitoring_baseline.assert_called()

    def test_transient_does_not_save(self):
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        store = _make_mock_store([fv])
        svc = MonitoringService(store)

        svc.compute_transient(
            project="test_project",
            feature_view_name="driver_stats",
        )

        provider = store._get_provider.return_value
        provider.offline_store.save_monitoring_metrics.assert_not_called()


# ------------------------------------------------------------------ #
#  Test: Log source monitoring
# ------------------------------------------------------------------ #


class TestLogSourceMonitoring:
    """Verify that monitoring can compute metrics from feature serving logs."""

    # Realistic log column names follow the {view}__{feature} convention
    # produced by FeatureServiceLoggingSource.get_schema().
    _LOG_SCHEMA = pa.schema(
        [
            ("driver_id", pa.int64()),
            ("driver_stats__conv_rate", pa.float64()),
            ("driver_stats__conv_rate__timestamp", pa.timestamp("us", tz="UTC")),
            ("driver_stats__conv_rate__status", pa.int32()),
            ("driver_stats__city", pa.utf8()),
            ("driver_stats__city__timestamp", pa.timestamp("us", tz="UTC")),
            ("driver_stats__city__status", pa.int32()),
            ("__log_timestamp", pa.timestamp("us", tz="UTC")),
            ("__log_date", pa.date32()),
            ("__request_id", pa.utf8()),
        ]
    )

    def _make_log_store(self):
        """Create a mock store with a feature service that has logging configured."""
        fv = _make_feature_view(
            "driver_stats",
            [
                _make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64),
                _make_feature_field("city", PrimitiveFeastType.STRING),
            ],
        )

        logging_config, log_data_source = _make_logging_config_with_source(
            self._LOG_SCHEMA
        )

        fs = _make_feature_service(
            "driver_service",
            ["driver_stats"],
            logging_config=logging_config,
            feature_map={"driver_stats": ["conv_rate", "city"]},
        )
        store = _make_mock_store([fv], feature_services=[fs])

        log_arrow_table = pa.table(
            {
                "driver_stats__conv_rate": [0.1, 0.5, 0.9, 0.3, 0.7],
                "driver_stats__city": ["NYC", "LA", "NYC", "SF", "LA"],
                "__log_timestamp": [
                    datetime(2025, 3, 25, tzinfo=timezone.utc),
                    datetime(2025, 3, 26, tzinfo=timezone.utc),
                    datetime(2025, 3, 26, tzinfo=timezone.utc),
                    datetime(2025, 3, 27, tzinfo=timezone.utc),
                    datetime(2025, 3, 27, tzinfo=timezone.utc),
                ],
            }
        )

        mock_log_retrieval = MagicMock()
        mock_log_retrieval.to_arrow.return_value = log_arrow_table

        provider = store._get_provider.return_value
        provider.offline_store.pull_all_from_table_or_query.return_value = (
            mock_log_retrieval
        )

        entity_col = MagicMock()
        entity_col.name = "driver_id"
        fv.entity_columns = [entity_col]

        return store, fs

    def test_compute_log_metrics(self):
        store, fs = self._make_log_store()
        svc = MonitoringService(store)

        with patch(
            "feast.monitoring.monitoring_service.FeatureServiceLoggingSource"
        ) as mock_cls:
            mock_instance = MagicMock()
            mock_instance.get_schema.return_value = self._LOG_SCHEMA
            mock_cls.return_value = mock_instance

            result = svc.compute_log_metrics(
                project="test_project",
                feature_service_name="driver_service",
                start_date=date(2025, 3, 25),
                end_date=date(2025, 3, 27),
                granularity="daily",
            )

        assert result["status"] == "completed"
        assert result["data_source_type"] == "log"
        assert result["computed_features"] == 2

        provider = store._get_provider.return_value
        provider.offline_store.save_monitoring_metrics.assert_called()

        save_calls = provider.offline_store.save_monitoring_metrics.call_args_list
        feature_calls = [c for c in save_calls if c[0][1] == "feature"]
        assert len(feature_calls) >= 1
        saved_metrics = feature_calls[0][0][2]
        assert all(m["data_source_type"] == "log" for m in saved_metrics)
        # Feature names normalized: driver_stats__conv_rate -> conv_rate
        saved_names = {m["feature_name"] for m in saved_metrics}
        assert saved_names == {"conv_rate", "city"}
        # Feature view name is the actual view, not the service
        assert all(m["feature_view_name"] == "driver_stats" for m in saved_metrics)

        # Feature service aggregate saved to the service table
        svc_calls = [c for c in save_calls if c[0][1] == "feature_service"]
        assert len(svc_calls) >= 1
        svc_metric = svc_calls[0][0][2][0]
        assert svc_metric["feature_service_name"] == "driver_service"
        assert svc_metric["data_source_type"] == "log"
        assert svc_metric["total_features"] == 2

    def test_compute_log_metrics_no_logging_config(self):
        fv = _make_feature_view(
            "driver_stats",
            [_make_feature_field("conv_rate", PrimitiveFeastType.FLOAT64)],
        )
        fs = _make_feature_service("no_log_service", ["driver_stats"])
        fs.logging_config = None
        store = _make_mock_store([fv], feature_services=[fs])
        svc = MonitoringService(store)

        result = svc.compute_log_metrics(
            project="test_project",
            feature_service_name="no_log_service",
        )

        assert result["status"] == "skipped"
        assert "no logging configured" in result["reason"]

    def test_auto_compute_log_metrics(self):
        store, fs = self._make_log_store()
        svc = MonitoringService(store)

        with patch(
            "feast.monitoring.monitoring_service.FeatureServiceLoggingSource"
        ) as mock_cls:
            mock_instance = MagicMock()
            mock_instance.get_schema.return_value = self._LOG_SCHEMA
            mock_cls.return_value = mock_instance

            result = svc.auto_compute_log_metrics(project="test_project")

        assert result["status"] == "completed"
        assert result["data_source_type"] == "log"
        assert result["computed_feature_services"] == 1
        assert len(result["granularities"]) == len(VALID_GRANULARITIES)

    def test_log_metrics_tagged_differently_from_batch(self):
        """Log metrics should have data_source_type='log', batch should have 'batch'."""
        store, fs = self._make_log_store()
        svc = MonitoringService(store)

        with patch(
            "feast.monitoring.monitoring_service.FeatureServiceLoggingSource"
        ) as mock_cls:
            mock_instance = MagicMock()
            mock_instance.get_schema.return_value = self._LOG_SCHEMA
            mock_cls.return_value = mock_instance

            svc.compute_log_metrics(
                project="test_project",
                feature_service_name="driver_service",
                granularity="daily",
            )

        provider = store._get_provider.return_value
        save_calls = provider.offline_store.save_monitoring_metrics.call_args_list
        feature_calls = [c for c in save_calls if c[0][1] == "feature"]
        for call in feature_calls:
            for m in call[0][2]:
                assert m["data_source_type"] == "log"
                assert m["feature_view_name"] == "driver_stats"
                assert m["feature_name"] in ("conv_rate", "city")
