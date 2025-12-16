from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import pandas as pd

from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores import dask as dask_mod
from feast.infra.offline_stores.dask import (
    DaskOfflineStore,
    DaskOfflineStoreConfig,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64, ValueType


def _mock_dask_offline_store_config():
    return DaskOfflineStoreConfig(type="dask")


def _mock_entity():
    return [
        Entity(
            name="driver_id",
            join_keys=["driver_id"],
            description="Driver ID",
            value_type=ValueType.INT64,
        )
    ]


def _mock_feature_view(name: str = "driver_stats", ttl: timedelta = None):
    """Helper to create mock feature views with configurable TTL"""
    return FeatureView(
        name=name,
        entities=_mock_entity(),
        schema=[
            Field(name="conv_rate", dtype=Float32),
        ],
        source=FileSource(
            path="dummy.parquet",  # not read in this test
            timestamp_field="event_timestamp",
        ),
        ttl=ttl,
    )


def test_dask_non_entity_historical_retrieval_accepts_dates():
    repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_dask_offline_store_config(),
    )

    fv = _mock_feature_view()

    retrieval_job = DaskOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=None,  # start/end-only mode
        registry=MagicMock(),
        project="test_project",
        full_feature_names=False,
        start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2023, 1, 2, tzinfo=timezone.utc),
    )

    # Should return a RetrievalJob

    assert isinstance(retrieval_job, RetrievalJob)


class TestNonEntityRetrieval:
    """
    Test suite for non-entity retrieval functionality (entity_df=None)

    This test suite comprehensively covers the new non-entity retrieval mode
    for Dask offline store, which enables retrieving features for specified
    time ranges without requiring an entity DataFrame.

    These tests use monkeypatching to inject real data and validate actual
    retrieval behavior, not just API signatures.
    """

    def test_dask_non_entity_snapshot_ttl_and_dedup(self, monkeypatch):
        # Controlled feature data with dedup (same event ts, newer created ts wins) and TTL filtering
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1, 2, 2],
                "event_timestamp": pd.to_datetime(
                    [
                        "2025-01-01T10:00:00Z",
                        "2025-01-01T10:00:00Z",  # duplicate event ts, newer created_ts
                        "2024-12-30T10:00:00Z",  # outside TTL (1 day) relative to end_date
                        "2025-01-01T12:00:00Z",  # within TTL window
                        "2025-01-02T11:00:00Z",  # after end_date
                    ]
                ),
                "created_ts": pd.to_datetime(
                    [
                        "2025-01-01T10:00:01Z",
                        "2025-01-01T10:00:02Z",  # should be selected for (1, 2025-01-01 10:00)
                        "2024-12-30T10:00:00Z",
                        "2025-01-01T12:00:00Z",
                        "2025-01-02T11:00:00Z",
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.05, 0.3, 0.4],
            }
        )
        ddf = dd.from_pandas(src, npartitions=1)

        # Monkeypatch the datasource reader used by DaskOfflineStore to return our in-memory data
        monkeypatch.setattr(dask_mod, "_read_datasource", lambda ds, repo_path: ddf)

        fv = FeatureView(
            name="driver_stats",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float32),
            ],
            source=FileSource(
                path="unused",
                timestamp_field="event_timestamp",
                created_timestamp_column="created_ts",
            ),
            ttl=timedelta(days=1),
        )

        repo_config = RepoConfig(
            project="proj",
            registry="unused",
            provider="local",
            offline_store=_mock_dask_offline_store_config(),
        )

        end = datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=7)

        job = DaskOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["driver_stats:conv_rate"],
            entity_df=None,
            registry=MagicMock(),
            project="proj",
            full_feature_names=False,
            start_date=start,
            end_date=end,
        )

        df = job.to_df()

        # Two entities are present
        assert set(df["driver_id"]) == {1, 2}
        out = df.set_index("driver_id")["conv_rate"].to_dict()
        # Dedup by created_ts for entity 1 at same event_timestamp
        assert out[1] == 0.2
        # TTL-enforced latest for entity 2 within the window
        assert out[2] == 0.3

        # Event timestamps are tz-aware UTC
        assert "UTC" in str(df["event_timestamp"].dtype)

        # Metadata reflects the requested window in non-entity mode
        assert job.metadata.min_event_timestamp == start
        assert job.metadata.max_event_timestamp == end

    def test_non_entity_mode_with_both_dates_retrieves_data(self, monkeypatch):
        """Test non-entity retrieval with both dates actually retrieves and filters data correctly"""

        # Create test data spanning the date range
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1, 2],
                "event_timestamp": pd.to_datetime(
                    [
                        "2023-01-01T08:00:00Z",  # Before start_date
                        "2023-01-03T10:00:00Z",  # Within range
                        "2023-01-05T12:00:00Z",  # Within range
                        "2023-01-08T14:00:00Z",  # After end_date
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.3, 0.4],
            }
        )
        ddf = dd.from_pandas(src, npartitions=1)
        monkeypatch.setattr(dask_mod, "_read_datasource", lambda ds, repo_path: ddf)

        repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_dask_offline_store_config(),
        )

        fv = FeatureView(
            name="test_fv",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float32),
            ],
            source=FileSource(path="unused", timestamp_field="event_timestamp"),
            ttl=None,  # No TTL - should include all rows within date range
        )

        start_date = datetime(2023, 1, 2, tzinfo=timezone.utc)
        end_date = datetime(2023, 1, 7, tzinfo=timezone.utc)

        job = DaskOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["test_fv:conv_rate"],
            entity_df=None,
            registry=MagicMock(),
            project="test_project",
            start_date=start_date,
            end_date=end_date,
        )

        df = job.to_df()

        # Verify metadata
        assert job.metadata.min_event_timestamp == start_date
        assert job.metadata.max_event_timestamp == end_date

        # Verify data: should only include rows within the date range
        # Row at 2023-01-01 (before start) and 2023-01-08 (after end) should be filtered out
        assert len(df) >= 1
        # Should have data for entities that have rows within the range
        assert set(df["driver_id"]) == {1}  # Only driver 1 has data in range
        # Verify the latest value for driver 1 within the range
        driver1_data = df[df["driver_id"] == 1]
        assert len(driver1_data) >= 1
        # Latest value should be 0.3 (from 2023-01-05)
        assert 0.3 in driver1_data["conv_rate"].values

    def test_non_entity_mode_with_end_date_only_calculates_start_from_ttl(
        self, monkeypatch
    ):
        """Test that start_date is calculated from max TTL when only end_date provided"""

        # Create data with rows at different times
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2023-01-05T08:00:00Z",  # Outside 1-day TTL from end_date
                        "2023-01-06T10:00:00Z",  # Within 1-day TTL
                        "2023-01-07T12:00:00Z",  # At end_date
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.3],
            }
        )
        ddf = dd.from_pandas(src, npartitions=1)
        monkeypatch.setattr(dask_mod, "_read_datasource", lambda ds, repo_path: ddf)

        repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_dask_offline_store_config(),
        )

        # Feature view with 1-day TTL
        fv = FeatureView(
            name="test_fv",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float32),
            ],
            source=FileSource(path="unused", timestamp_field="event_timestamp"),
            ttl=timedelta(days=1),
        )

        end_date = datetime(2023, 1, 7, 12, 0, tzinfo=timezone.utc)

        job = DaskOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["test_fv:conv_rate"],
            entity_df=None,
            registry=MagicMock(),
            project="test_project",
            end_date=end_date,
            # start_date not provided - should be calculated from TTL
        )

        df = job.to_df()

        # Verify start_date was calculated from TTL (1 day before end_date)
        expected_start = end_date - timedelta(days=1)
        assert job.metadata.min_event_timestamp == expected_start
        assert job.metadata.max_event_timestamp == end_date

        # Verify TTL filtering: row at 2023-01-05 (outside TTL) should be filtered out
        # Only rows within 1 day of end_date should be present
        driver1_data = df[df["driver_id"] == 1]
        # Should have latest value from within TTL window
        assert len(driver1_data) >= 1
        # Latest value should be 0.3 (from 2023-01-07)
        assert 0.3 in driver1_data["conv_rate"].values

    @patch("feast.infra.offline_stores.dask.datetime")
    def test_no_dates_provided_defaults_to_current_time_and_filters_data(
        self, mock_datetime, monkeypatch
    ):
        """Test that default date calculation works and filters data correctly"""

        fixed_now = datetime(2023, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_now

        # Create data with rows at different times relative to "now"
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2023-01-05T12:00:00Z",  # 2 days before "now" - outside 1-day TTL
                        "2023-01-06T18:00:00Z",  # ~18 hours before "now" - within 1-day TTL
                        "2023-01-07T11:00:00Z",  # 1 hour before "now" - within 1-day TTL
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.3],
            }
        )
        ddf = dd.from_pandas(src, npartitions=1)
        monkeypatch.setattr(dask_mod, "_read_datasource", lambda ds, repo_path: ddf)

        repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_dask_offline_store_config(),
        )

        fv = FeatureView(
            name="test_fv",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float32),
            ],
            source=FileSource(path="unused", timestamp_field="event_timestamp"),
            ttl=timedelta(days=1),
        )

        job = DaskOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["test_fv:conv_rate"],
            entity_df=None,
            registry=MagicMock(),
            project="test_project",
            # No start_date or end_date provided
        )

        df = job.to_df()

        # Verify end_date defaulted to current time
        assert job.metadata.max_event_timestamp == fixed_now
        # Verify start_date was calculated from TTL
        expected_start = fixed_now - timedelta(days=1)
        assert job.metadata.min_event_timestamp == expected_start

        # Verify data filtering: row at 2023-01-05 (outside TTL) should be filtered out
        driver1_data = df[df["driver_id"] == 1]
        assert len(driver1_data) >= 1
        # Latest value should be 0.3 (from 2023-01-07)
        assert 0.3 in driver1_data["conv_rate"].values

    def test_ttl_filtering_excludes_old_rows(self, monkeypatch):
        """Test that TTL filtering correctly excludes rows outside the TTL window"""

        # Create data with rows inside and outside TTL window
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2025-01-01T08:00:00Z",  # Outside 1-hour TTL (end_date is 10:00)
                        "2025-01-01T09:30:00Z",  # Within 1-hour TTL
                        "2025-01-01T10:00:00Z",  # At end_date
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.3],
            }
        )
        ddf = dd.from_pandas(src, npartitions=1)
        monkeypatch.setattr(dask_mod, "_read_datasource", lambda ds, repo_path: ddf)

        fv = FeatureView(
            name="driver_stats",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float32),
            ],
            source=FileSource(path="unused", timestamp_field="event_timestamp"),
            ttl=timedelta(hours=1),
        )

        repo_config = RepoConfig(
            project="proj",
            registry="unused",
            provider="local",
            offline_store=_mock_dask_offline_store_config(),
        )

        end = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=1)

        job = DaskOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["driver_stats:conv_rate"],
            entity_df=None,
            registry=MagicMock(),
            project="proj",
            full_feature_names=False,
            start_date=start,
            end_date=end,
        )

        df = job.to_df()

        # Only rows within TTL window should be present
        # Row at 08:00 should be filtered out (outside 1-hour TTL)
        driver1_data = df[df["driver_id"] == 1]
        assert len(driver1_data) >= 1
        # Latest value should be 0.3 (from 10:00) or 0.2 (from 09:30)
        assert (
            0.2 in driver1_data["conv_rate"].values
            or 0.3 in driver1_data["conv_rate"].values
        )
        # Value 0.1 (from 08:00, outside TTL) should NOT be present
        assert 0.1 not in driver1_data["conv_rate"].values

    def test_multiple_feature_views_with_different_ttls(self, monkeypatch):
        """Test retrieval with multiple feature views having different TTLs"""
        # Create data for two feature views
        src1 = pd.DataFrame(
            {
                "driver_id": [1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2025-01-01T08:00:00Z",  # Outside 1-hour TTL
                        "2025-01-01T09:30:00Z",  # Within 1-hour TTL
                    ]
                ),
                "age": [25, 26],
            }
        )

        src2 = pd.DataFrame(
            {
                "driver_id": [1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2025-01-01T08:00:00Z",  # Within 1-day TTL
                        "2025-01-01T09:30:00Z",  # Within 1-day TTL
                    ]
                ),
                "total_trips": [100, 101],
            }
        )

        ddf1 = dd.from_pandas(src1, npartitions=1)
        ddf2 = dd.from_pandas(src2, npartitions=1)

        call_count = [0]

        def mock_read_datasource(ds, repo_path):
            call_count[0] += 1
            if call_count[0] == 1:
                return ddf1
            return ddf2

        monkeypatch.setattr(dask_mod, "_read_datasource", mock_read_datasource)

        fv1 = FeatureView(
            name="user_fv",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="age", dtype=Float32),
            ],
            source=FileSource(path="unused1", timestamp_field="event_timestamp"),
            ttl=timedelta(hours=1),
        )

        fv2 = FeatureView(
            name="transaction_fv",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="total_trips", dtype=Float32),
            ],
            source=FileSource(path="unused2", timestamp_field="event_timestamp"),
            ttl=timedelta(days=1),
        )

        repo_config = RepoConfig(
            project="proj",
            registry="unused",
            provider="local",
            offline_store=_mock_dask_offline_store_config(),
        )

        end = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=1)

        job = DaskOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv1, fv2],
            feature_refs=["user_fv:age", "transaction_fv:total_trips"],
            entity_df=None,
            registry=MagicMock(),
            project="proj",
            full_feature_names=False,
            start_date=start,
            end_date=end,
        )

        df = job.to_df()

        # Both feature views should be processed
        assert "age" in df.columns or "user_fv__age" in df.columns
        assert (
            "total_trips" in df.columns or "transaction_fv__total_trips" in df.columns
        )

        # Verify TTL filtering worked correctly
        # For user_fv (1-hour TTL): only row at 09:30 should be present
        # For transaction_fv (1-day TTL): both rows should be present, latest is 101
        if "age" in df.columns:
            age_values = df["age"].values
            # Should have 26 (from 09:30, within 1-hour TTL), not 25 (from 08:00)
            assert 26 in age_values
            assert 25 not in age_values

        if "total_trips" in df.columns:
            trips_values = df["total_trips"].values
            # Should have 101 (latest within 1-day TTL)
            assert 101 in trips_values
