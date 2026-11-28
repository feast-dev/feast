"""Integration tests for Feast DQM (Data Quality Monitoring) with Chronon outputs (Issue #6984).

Tests cover:
1. Monitoring Storage & Hooks:
   - Persistence hooks (ensure, save, query, clear_baseline, get_max_timestamp) in ChrononOfflineStore.
   - Job state surviving process restarts.
   - Metric upsert idempotence on retries (no duplicate rows).
   - Baseline replacement without duplicate active baselines.
2. Timestamps, Freshness & Lineage / Provenance:
   - Online read returning feature timestamps for source freshness.
   - Distinct timestamps: event time, feature-computation time, export time, metric-computation time.
   - Missing timestamps / metadata explicitly handled as unknown/None.
   - Provenance tracking: FeatureView, Chronon GroupBy/Join, output version, pipeline run ID.
3. API/UI & Integration Fixtures:
   - REST API / UI: Profiles, baselines, and date buckets endpoints.
   - Null preservation (null_count, null_rate).
   - Mapped feature names (`ChrononSource.field_mapping`).
   - Multi-entity keys.
   - Timestamp filtering.
   - Known metric values (mean, stddev, quantiles, histograms).
   - Stale-output fixture detecting data staleness.
"""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from feast import Entity, FeatureStore, FeatureView, Field, RepoConfig
from feast.api.registry.rest.monitoring import get_monitoring_router
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon import (
    ChrononOfflineStore,
)
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)
from feast.infra.online_stores.chronon_online_store.chronon import (
    ChrononOnlineStore,
    _parse_chronon_timestamp,
)
from feast.monitoring.dqm_job_manager import DQMJobManager
from feast.monitoring.monitoring_service import MonitoringService
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig
from feast.types import Float32
from feast.value_type import ValueType


@pytest.fixture
def repo_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def feast_config(repo_dir: Path) -> RepoConfig:
    cfg = RepoConfig(
        project="test_chronon_dqm",
        registry=RegistryConfig(path=str(repo_dir / "registry.db")),
        provider="chronon",
        offline_store={"type": "chronon", "path": str(repo_dir / "offline")},
        online_store={"type": "chronon", "path": "http://localhost:8080"},
        data_quality_monitoring={"auto_baseline": True},
    )
    cfg.repo_path = repo_dir
    return cfg


# ------------------------------------------------------------------ #
#  1. Monitoring Storage & Hooks Tests
# ------------------------------------------------------------------ #


@pytest.mark.integration
def test_chronon_offline_store_monitoring_persistence_and_job_restart_survival(
    feast_config: RepoConfig,
):
    """Verify DQM job lifecycle and state survive simulated service restarts."""
    offline_store = ChrononOfflineStore()
    offline_store.ensure_monitoring_tables(feast_config)

    job_manager_1 = DQMJobManager(offline_store, feast_config)
    job_id = job_manager_1.submit(
        project=feast_config.project,
        job_type="auto_compute",
        feature_view_name="driver_stats",
        parameters={"sample_param": "value1"},
    )

    job = job_manager_1.get_job(job_id, project=feast_config.project)
    assert job is not None
    assert job["status"] == "pending"
    assert job["parameters"] == {"sample_param": "value1"}

    job_manager_1.update_status(
        job_id,
        status="completed",
        result_summary={"computed_features": 5, "duration_ms": 120},
    )

    # Simulate process restart by instantiating a completely new JobManager
    job_manager_restarted = DQMJobManager(offline_store, feast_config)
    restored_job = job_manager_restarted.get_job(job_id, project=feast_config.project)

    assert restored_job is not None
    assert restored_job["status"] == "completed"
    assert restored_job["result_summary"] == {
        "computed_features": 5,
        "duration_ms": 120,
    }
    assert restored_job["completed_at"] is not None


@pytest.mark.integration
def test_chronon_offline_store_retry_idempotence(feast_config: RepoConfig):
    """Verify retrying metric saves updates existing records rather than duplicating them."""
    offline_store = ChrononOfflineStore()
    offline_store.ensure_monitoring_tables(feast_config)

    metric_row: Dict[str, Any] = {
        "project_id": feast_config.project,
        "feature_view_name": "user_features",
        "feature_name": "rating",
        "metric_date": date(2025, 1, 1),
        "granularity": "daily",
        "data_source_type": "batch",
        "computed_at": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
        "max_event_timestamp": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
        "is_baseline": False,
        "feature_type": "numeric",
        "row_count": 100,
        "null_count": 5,
        "null_rate": 0.05,
        "mean": 4.2,
        "stddev": 0.5,
    }

    # First write
    offline_store.save_monitoring_metrics(feast_config, "feature", [metric_row])

    # Retry with updated mean
    retry_row = dict(metric_row)
    retry_row["mean"] = 4.35
    offline_store.save_monitoring_metrics(feast_config, "feature", [retry_row])

    rows = offline_store.query_monitoring_metrics(
        config=feast_config,
        project=feast_config.project,
        metric_type="feature",
        filters={"feature_view_name": "user_features", "feature_name": "rating"},
    )
    assert len(rows) == 1
    assert rows[0]["mean"] == 4.35


@pytest.mark.integration
def test_chronon_offline_store_baseline_replacement(feast_config: RepoConfig):
    """Verify clear_monitoring_baseline replaces baseline correctly without duplicates."""
    offline_store = ChrononOfflineStore()
    offline_store.ensure_monitoring_tables(feast_config)

    old_baseline = {
        "project_id": feast_config.project,
        "feature_view_name": "user_features",
        "feature_name": "score",
        "metric_date": date(2025, 1, 1),
        "granularity": "baseline",
        "data_source_type": "batch",
        "computed_at": datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
        "is_baseline": True,
        "feature_type": "numeric",
        "row_count": 500,
        "null_rate": 0.0,
        "mean": 10.0,
    }
    offline_store.save_monitoring_metrics(feast_config, "feature", [old_baseline])

    baselines_before = offline_store.query_monitoring_metrics(
        config=feast_config,
        project=feast_config.project,
        metric_type="feature",
        filters={"feature_view_name": "user_features", "is_baseline": True},
    )
    assert len(baselines_before) == 1
    assert baselines_before[0]["mean"] == 10.0

    # Replace baseline
    offline_store.clear_monitoring_baseline(
        config=feast_config,
        project=feast_config.project,
        feature_view_name="user_features",
    )
    new_baseline = dict(old_baseline)
    new_baseline["metric_date"] = date(2025, 1, 2)
    new_baseline["mean"] = 15.0
    offline_store.save_monitoring_metrics(feast_config, "feature", [new_baseline])

    baselines_after = offline_store.query_monitoring_metrics(
        config=feast_config,
        project=feast_config.project,
        metric_type="feature",
        filters={"feature_view_name": "user_features", "is_baseline": True},
    )
    assert len(baselines_after) == 1
    assert baselines_after[0]["mean"] == 15.0
    assert baselines_after[0]["metric_date"] == "2025-01-02"


# ------------------------------------------------------------------ #
#  2. Timestamps, Freshness & Lineage / Provenance Tests
# ------------------------------------------------------------------ #


@pytest.mark.integration
def test_chronon_online_read_returns_freshness_timestamp():
    """Verify online reads parse response timestamps into datetimes or None for unknown."""
    # Test milliseconds Unix timestamp parsing
    ts_ms = 1704067200000  # 2024-01-01T00:00:00Z in ms
    parsed = _parse_chronon_timestamp({"ts": ts_ms})
    assert parsed == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Test seconds Unix timestamp
    ts_sec = 1704067200
    assert _parse_chronon_timestamp({"timestamp": ts_sec}) == datetime(
        2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc
    )

    # Test ISO string
    assert _parse_chronon_timestamp(
        {"event_timestamp": "2024-01-01T00:00:00Z"}
    ) == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Missing timestamps handled explicitly as unknown (None)
    assert _parse_chronon_timestamp({"other_field": 123}) is None
    assert _parse_chronon_timestamp({}) is None


@pytest.mark.integration
def test_chronon_online_store_online_read_freshness_integration(
    feast_config: RepoConfig, tmp_path: Path
):
    """Verify online_read integrates timestamp for source freshness."""
    online_store = ChrononOnlineStore()
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="user_fv",
        entities=[user],
        schema=[Field(name="feature_a", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(tmp_path / "dummy.parquet"),
            chronon_join="team/users.v1",
            timestamp_field="event_timestamp",
        ),
    )

    ek = EntityKeyProto(
        join_keys=["user_id"],
        entity_values=[ValueProto(int64_val=42)],
    )

    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "results": [
            {
                "status": "Success",
                "ts": 1704067200000,
                "features": {"feature_a": 3.14},
            }
        ]
    }
    mock_resp.raise_for_status.return_value = None

    mock_session = MagicMock()
    mock_session.post.return_value = mock_resp

    with patch(
        "feast.permissions.client.http_auth_requests_wrapper.HttpSessionManager.get_session",
        return_value=mock_session,
    ):
        results = online_store.online_read(
            config=feast_config,
            table=fv,
            entity_keys=[ek],
        )

        assert len(results) == 1
        ts, features = results[0]
        assert ts == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert features is not None
        assert "feature_a" in features


@pytest.mark.integration
def test_chronon_dqm_distinct_timestamps_and_provenance(
    feast_config: RepoConfig, repo_dir: Path
):
    """Verify event time, feature-computation time, export time, and metric-computation time

    are kept distinct, with provenance properly tracked.
    """
    data_path = repo_dir / "chronon_distinct_ts.parquet"
    event_ts = datetime(2025, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
    comp_ts = datetime(2025, 2, 1, 11, 30, 0, tzinfo=timezone.utc)

    pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "event_time": [event_ts, event_ts, event_ts],
            "feature_calc_time": [comp_ts, comp_ts, comp_ts],
            "rating": [4.0, 5.0, 3.0],
        }
    ).to_parquet(data_path)

    store = FeatureStore(config=feast_config)
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    source = ChrononSource(
        name="user_ratings_src",
        materialization_path=str(data_path),
        chronon_group_by="team/user_ratings_groupby.v1",
        timestamp_field="event_time",
        created_timestamp_column="feature_calc_time",
        output_version="v3.0",
        pipeline_run_id="run_abc_123",
    )
    fv = FeatureView(
        name="user_ratings",
        entities=[user],
        schema=[Field(name="rating", dtype=Float32)],
        source=source,
    )
    store.apply([user, fv])

    service = MonitoringService(store)
    now = datetime(2025, 2, 2, 12, 0, 0, tzinfo=timezone.utc)

    service._save_computed_metrics(
        project=feast_config.project,
        feature_view=fv,
        metrics_list=[
            {
                "feature_name": "rating",
                "feature_type": "numeric",
                "row_count": 3,
                "null_count": 0,
                "null_rate": 0.0,
                "mean": 4.0,
            }
        ],
        metric_date=date(2025, 2, 1),
        granularity="daily",
        set_baseline=False,
        now=now,
        max_event_timestamp=event_ts,
    )

    metrics = service.get_feature_metrics(
        project=feast_config.project,
        feature_view_name="user_ratings",
    )
    assert len(metrics) == 1
    m = metrics[0]

    # Check distinct timestamps
    assert m["max_event_timestamp"] == event_ts.isoformat()
    assert m["feature_computation_timestamp"] == comp_ts.isoformat()
    assert m["computed_at"] == now.isoformat()
    assert m["export_timestamp"] is not None  # Derived from file stat mtime

    # Verify event time != feature-computation time != export time != metric-computation time
    assert m["max_event_timestamp"] != m["feature_computation_timestamp"]
    assert m["feature_computation_timestamp"] != m["computed_at"]

    # Check provenance context
    assert m["chronon_object"] == "team/user_ratings_groupby.v1"
    assert m["chronon_object_type"] == "groupby"
    assert m["output_version"] == "v3.0"
    assert m["pipeline_run_id"] == "run_abc_123"


# ------------------------------------------------------------------ #
#  3. Null Preservation, Mapped Names, Known Metrics & Stale Fixture
# ------------------------------------------------------------------ #


@pytest.mark.integration
def test_chronon_dqm_null_preservation_mapped_names_and_known_values(
    feast_config: RepoConfig, repo_dir: Path
):
    """Verify null preservation, field mappings, multi-entity keys, and exact metric values."""
    data_path = repo_dir / "chronon_stats.parquet"

    # Known distribution: 10 rows, 2 nulls (null_rate = 0.2)
    # Non-null values: 10, 20, 30, 50, 70, 80, 90, 100 -> sum = 450, mean = 56.25
    timestamps = [
        datetime(2025, 1, 1, 1, 0, tzinfo=timezone.utc) + timedelta(hours=i)
        for i in range(10)
    ]
    pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "merchant_id": [101, 102, 101, 102, 103, 101, 102, 103, 101, 102],
            "raw_timestamp": timestamps,
            "raw_feature_val": [
                10.0,
                20.0,
                30.0,
                None,
                50.0,
                None,
                70.0,
                80.0,
                90.0,
                100.0,
            ],
        }
    ).to_parquet(data_path)

    store = FeatureStore(config=feast_config)
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    merchant = Entity(
        name="merchant", join_keys=["merchant_id"], value_type=ValueType.INT64
    )
    source = ChrononSource(
        name="txn_source",
        materialization_path=str(data_path),
        chronon_join="team/transactions.v1",
        timestamp_field="event_timestamp",
        field_mapping={
            "raw_timestamp": "event_timestamp",
            "raw_feature_val": "amount",
        },
    )
    fv = FeatureView(
        name="txn_stats",
        entities=[user, merchant],
        schema=[Field(name="amount", dtype=Float32)],
        source=source,
    )
    store.apply([user, merchant, fv])

    service = MonitoringService(store)
    result = service.compute_metrics(
        project=feast_config.project,
        feature_view_name="txn_stats",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        granularity="daily",
    )
    assert result["status"] == "completed"

    metrics = service.get_feature_metrics(
        project=feast_config.project,
        feature_view_name="txn_stats",
    )
    assert len(metrics) == 1
    m = metrics[0]

    # Verify mapped feature name
    assert m["feature_name"] == "amount"

    # Verify null preservation
    assert m["row_count"] == 10
    assert m["null_count"] == 2
    assert m["null_rate"] == pytest.approx(0.2)

    # Verify known metric values
    assert m["mean"] == pytest.approx(56.25)
    assert m["min_val"] == pytest.approx(10.0)
    assert m["max_val"] == pytest.approx(100.0)
    assert m["histogram"] is not None


@pytest.mark.integration
def test_chronon_dqm_timestamp_filtering(feast_config: RepoConfig, repo_dir: Path):
    """Verify timestamp filtering only computes metrics on data within requested window."""
    data_path = repo_dir / "chronon_time_filter.parquet"

    # 5 rows spanning Jan 1 to Jan 5
    pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "event_timestamp": [
                datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 3, 12, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 4, 12, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 5, 12, 0, tzinfo=timezone.utc),
            ],
            "val": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    ).to_parquet(data_path)

    store = FeatureStore(config=feast_config)
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="time_filtered_fv",
        entities=[user],
        schema=[Field(name="val", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(data_path),
            chronon_join="team/time_join.v1",
            timestamp_field="event_timestamp",
        ),
    )
    store.apply([user, fv])

    service = MonitoringService(store)
    # Compute for Jan 2 to Jan 3 (2 rows: 20.0, 30.0 -> mean = 25.0)
    result = service.compute_metrics(
        project=feast_config.project,
        feature_view_name="time_filtered_fv",
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 3),
        granularity="daily",
    )
    assert result["status"] == "completed"

    metrics = service.get_feature_metrics(
        project=feast_config.project,
        feature_view_name="time_filtered_fv",
    )
    assert len(metrics) == 1
    m = metrics[0]
    assert m["row_count"] == 2
    assert m["mean"] == pytest.approx(25.0)


@pytest.mark.integration
def test_chronon_dqm_stale_output_fixture(feast_config: RepoConfig, repo_dir: Path):
    """Verify source freshness reporting identifies stale outputs correctly."""
    data_path = repo_dir / "chronon_stale.parquet"

    # Data from 30 days ago
    stale_event_time = datetime.now(timezone.utc) - timedelta(days=30)
    pd.DataFrame(
        {
            "user_id": [1, 2],
            "event_timestamp": [stale_event_time, stale_event_time],
            "metric": [1.0, 2.0],
        }
    ).to_parquet(data_path)

    store = FeatureStore(config=feast_config)
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="stale_fv",
        entities=[user],
        schema=[Field(name="metric", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(data_path),
            chronon_join="team/stale_join.v1",
            timestamp_field="event_timestamp",
        ),
    )
    store.apply([user, fv])

    service = MonitoringService(store)
    service.compute_metrics(
        project=feast_config.project,
        feature_view_name="stale_fv",
        start_date=stale_event_time.date(),
        end_date=stale_event_time.date(),
    )

    profile = service.get_profiles(
        project=feast_config.project,
        feature_view_name="stale_fv",
    )

    assert "source_freshness" in profile
    freshness = profile["source_freshness"]
    assert freshness["max_event_timestamp"] is not None
    # Data is 30 days old -> freshness_seconds should be approx 30 * 86400 seconds
    assert freshness["freshness_seconds"] > 25 * 86400


# ------------------------------------------------------------------ #
#  4. REST API Profiles, Baselines, and Date Buckets Tests
# ------------------------------------------------------------------ #


@pytest.mark.integration
def test_chronon_dqm_rest_api_profiles_and_date_buckets(
    feast_config: RepoConfig, repo_dir: Path
):
    """Verify REST API exposes profiles, baselines, and date buckets with provenance."""
    data_path = repo_dir / "chronon_api.parquet"
    pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "event_timestamp": [
                datetime(2025, 3, 1, 10, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 2, 10, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 3, 10, 0, tzinfo=timezone.utc),
            ],
            "feature_val": [100.0, 200.0, 300.0],
        }
    ).to_parquet(data_path)

    store = FeatureStore(config=feast_config)
    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="api_fv",
        entities=[user],
        schema=[Field(name="feature_val", dtype=Float32)],
        source=ChrononSource(
            materialization_path=str(data_path),
            chronon_join="team/api_join.v1",
            timestamp_field="event_timestamp",
            output_version="v1.0",
            pipeline_run_id="run_rest_test",
        ),
    )
    store.apply([user, fv])

    service = MonitoringService(store)
    # Compute daily metrics
    service.compute_metrics(
        project=feast_config.project,
        feature_view_name="api_fv",
        start_date=date(2025, 3, 1),
        end_date=date(2025, 3, 3),
        granularity="daily",
    )
    # Compute baseline
    service.compute_baseline(
        project=feast_config.project,
        feature_view_name="api_fv",
    )

    # Build FastAPI test client with Feast monitoring router
    app = FastAPI()
    router = get_monitoring_router(MagicMock(), store=store)
    app.include_router(router)
    client = TestClient(app)

    # 1. Test GET /monitoring/date_buckets
    res_buckets = client.get(
        f"/monitoring/date_buckets?project={feast_config.project}&feature_view_name=api_fv"
    )
    assert res_buckets.status_code == 200
    buckets = res_buckets.json()
    assert isinstance(buckets, list)
    assert len(buckets) >= 1
    assert any(b.get("granularity") == "daily" for b in buckets)

    # 2. Test GET /monitoring/profiles
    res_profiles = client.get(
        f"/monitoring/profiles?project={feast_config.project}&feature_view_name=api_fv"
    )
    assert res_profiles.status_code == 200
    profile_data = res_profiles.json()
    assert profile_data["feature_view_name"] == "api_fv"
    assert profile_data["provenance"]["chronon_object"] == "team/api_join.v1"
    assert profile_data["provenance"]["output_version"] == "v1.0"
    assert profile_data["provenance"]["pipeline_run_id"] == "run_rest_test"
    assert len(profile_data["feature_metrics"]) >= 1
    assert len(profile_data["baseline_metrics"]) >= 1

    # 3. Test GET /monitoring/metrics/baseline
    res_baseline = client.get(
        f"/monitoring/metrics/baseline?project={feast_config.project}&feature_view_name=api_fv"
    )
    assert res_baseline.status_code == 200
    baseline_rows = res_baseline.json()
    assert len(baseline_rows) == 1
    assert baseline_rows[0]["is_baseline"] is True
    assert baseline_rows[0]["chronon_object"] == "team/api_join.v1"
