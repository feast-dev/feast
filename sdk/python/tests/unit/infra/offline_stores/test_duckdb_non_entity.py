from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import ibis
import pandas as pd

from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores import duckdb as duckdb_mod
from feast.infra.offline_stores.duckdb import (
    DuckDBOfflineStore,
    DuckDBOfflineStoreConfig,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64, ValueType


def _mock_duckdb_offline_store_config():
    return DuckDBOfflineStoreConfig(type="duckdb")


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
    return FeatureView(
        name=name,
        entities=_mock_entity(),
        schema=[
            Field(name="conv_rate", dtype=Float32),
        ],
        source=FileSource(
            path="dummy.parquet",
            timestamp_field="event_timestamp",
        ),
        ttl=ttl,
    )


def _mock_data_source_reader(src_df):
    """Return a data_source_reader that wraps a pandas DataFrame as an ibis memtable."""

    def reader(data_source, repo_path):
        return ibis.memtable(src_df)

    return reader


def test_duckdb_non_entity_historical_retrieval_accepts_dates(monkeypatch):
    src = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.to_datetime(["2023-01-01T12:00:00Z"]),
            "conv_rate": [0.5],
        }
    )
    monkeypatch.setattr(duckdb_mod, "_read_data_source", _mock_data_source_reader(src))

    repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_duckdb_offline_store_config(),
    )

    fv = _mock_feature_view()

    retrieval_job = DuckDBOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=None,
        registry=MagicMock(),
        project="test_project",
        full_feature_names=False,
        start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2023, 1, 2, tzinfo=timezone.utc),
    )

    assert isinstance(retrieval_job, RetrievalJob)


class TestNonEntityRetrieval:
    """Test suite for non-entity retrieval functionality (entity_df=None) in DuckDB offline store."""

    def test_duckdb_non_entity_snapshot_ttl_and_dedup(self, monkeypatch):
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1, 2, 2],
                "event_timestamp": pd.to_datetime(
                    [
                        "2025-01-01T10:00:00Z",
                        "2025-01-01T10:00:00Z",
                        "2024-12-30T10:00:00Z",
                        "2025-01-01T12:00:00Z",
                        "2025-01-02T11:00:00Z",
                    ]
                ),
                "created_ts": pd.to_datetime(
                    [
                        "2025-01-01T10:00:01Z",
                        "2025-01-01T10:00:02Z",
                        "2024-12-30T10:00:00Z",
                        "2025-01-01T12:00:00Z",
                        "2025-01-02T11:00:00Z",
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.05, 0.3, 0.4],
            }
        )
        monkeypatch.setattr(
            duckdb_mod, "_read_data_source", _mock_data_source_reader(src)
        )

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
            offline_store=_mock_duckdb_offline_store_config(),
        )

        end = datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=7)

        job = DuckDBOfflineStore.get_historical_features(
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

        assert set(df["driver_id"]) == {1, 2}
        out = df.set_index("driver_id")["conv_rate"].to_dict()
        assert out[1] == 0.2
        assert out[2] == 0.3

    def test_non_entity_mode_with_both_dates_retrieves_data(self, monkeypatch):
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1, 2],
                "event_timestamp": pd.to_datetime(
                    [
                        "2023-01-01T08:00:00Z",
                        "2023-01-03T10:00:00Z",
                        "2023-01-05T12:00:00Z",
                        "2023-01-08T14:00:00Z",
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.3, 0.4],
            }
        )
        monkeypatch.setattr(
            duckdb_mod, "_read_data_source", _mock_data_source_reader(src)
        )

        repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_duckdb_offline_store_config(),
        )

        fv = FeatureView(
            name="test_fv",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float32),
            ],
            source=FileSource(path="unused", timestamp_field="event_timestamp"),
            ttl=None,
        )

        start_date = datetime(2023, 1, 2, tzinfo=timezone.utc)
        end_date = datetime(2023, 1, 7, tzinfo=timezone.utc)

        job = DuckDBOfflineStore.get_historical_features(
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

        assert job.metadata.min_event_timestamp == end_date
        assert job.metadata.max_event_timestamp == end_date

        assert len(df) >= 1
        assert set(df["driver_id"]) == {1}
        driver1_data = df[df["driver_id"] == 1]
        assert 0.3 in driver1_data["conv_rate"].values

    def test_non_entity_mode_with_end_date_only_calculates_start_from_ttl(
        self, monkeypatch
    ):
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2023-01-05T08:00:00Z",
                        "2023-01-06T10:00:00Z",
                        "2023-01-07T12:00:00Z",
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.3],
            }
        )
        monkeypatch.setattr(
            duckdb_mod, "_read_data_source", _mock_data_source_reader(src)
        )

        repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_duckdb_offline_store_config(),
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

        end_date = datetime(2023, 1, 7, 12, 0, tzinfo=timezone.utc)

        job = DuckDBOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["test_fv:conv_rate"],
            entity_df=None,
            registry=MagicMock(),
            project="test_project",
            end_date=end_date,
        )

        df = job.to_df()

        driver1_data = df[df["driver_id"] == 1]
        assert len(driver1_data) >= 1
        assert 0.3 in driver1_data["conv_rate"].values

    @patch("feast.utils.datetime")
    def test_no_dates_provided_defaults_to_current_time_and_filters_data(
        self, mock_datetime, monkeypatch
    ):
        fixed_now = datetime(2023, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_now
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2023-01-05T12:00:00Z",
                        "2023-01-06T18:00:00Z",
                        "2023-01-07T11:00:00Z",
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.3],
            }
        )
        monkeypatch.setattr(
            duckdb_mod, "_read_data_source", _mock_data_source_reader(src)
        )

        repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_duckdb_offline_store_config(),
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

        job = DuckDBOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["test_fv:conv_rate"],
            entity_df=None,
            registry=MagicMock(),
            project="test_project",
        )

        df = job.to_df()

        driver1_data = df[df["driver_id"] == 1]
        assert len(driver1_data) >= 1
        assert 0.3 in driver1_data["conv_rate"].values

    def test_ttl_filtering_excludes_old_rows(self, monkeypatch):
        src = pd.DataFrame(
            {
                "driver_id": [1, 1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2025-01-01T08:00:00Z",
                        "2025-01-01T09:30:00Z",
                        "2025-01-01T10:00:00Z",
                    ]
                ),
                "conv_rate": [0.1, 0.2, 0.3],
            }
        )
        monkeypatch.setattr(
            duckdb_mod, "_read_data_source", _mock_data_source_reader(src)
        )

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
            offline_store=_mock_duckdb_offline_store_config(),
        )

        end = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=1)

        job = DuckDBOfflineStore.get_historical_features(
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

        driver1_data = df[df["driver_id"] == 1]
        assert len(driver1_data) >= 1
        assert (
            0.2 in driver1_data["conv_rate"].values
            or 0.3 in driver1_data["conv_rate"].values
        )
        assert 0.1 not in driver1_data["conv_rate"].values

    def test_multiple_feature_views_with_different_ttls(self, monkeypatch):
        src1 = pd.DataFrame(
            {
                "driver_id": [1, 1],
                "event_timestamp": pd.to_datetime(
                    [
                        "2025-01-01T08:00:00Z",
                        "2025-01-01T09:30:00Z",
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
                        "2025-01-01T08:00:00Z",
                        "2025-01-01T09:30:00Z",
                    ]
                ),
                "total_trips": [100, 101],
            }
        )

        def mock_read_data_source(data_source, repo_path):
            if data_source.path == "unused1":
                return ibis.memtable(src1)
            return ibis.memtable(src2)

        monkeypatch.setattr(duckdb_mod, "_read_data_source", mock_read_data_source)

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
            offline_store=_mock_duckdb_offline_store_config(),
        )

        end = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
        start = end - timedelta(days=1)

        job = DuckDBOfflineStore.get_historical_features(
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

        assert "age" in df.columns or "user_fv__age" in df.columns
        assert (
            "total_trips" in df.columns or "transaction_fv__total_trips" in df.columns
        )

        if "age" in df.columns:
            age_values = df["age"].values
            assert 26 in age_values
            assert 25 not in age_values

        if "total_trips" in df.columns:
            trips_values = df["total_trips"].values
            assert 101 in trips_values

    def test_entity_df_still_works(self, monkeypatch):
        """Verify standard entity_df path is not broken by the changes."""
        src = pd.DataFrame(
            {
                "driver_id": [1, 2],
                "event_timestamp": pd.to_datetime(
                    ["2025-01-01T10:00:00Z", "2025-01-01T11:00:00Z"]
                ),
                "conv_rate": [0.5, 0.6],
            }
        )
        monkeypatch.setattr(
            duckdb_mod, "_read_data_source", _mock_data_source_reader(src)
        )

        repo_config = RepoConfig(
            project="proj",
            registry="unused",
            provider="local",
            offline_store=_mock_duckdb_offline_store_config(),
        )

        fv = FeatureView(
            name="driver_stats",
            entities=_mock_entity(),
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="conv_rate", dtype=Float32),
            ],
            source=FileSource(path="unused", timestamp_field="event_timestamp"),
            ttl=timedelta(days=1),
        )

        entity_df = pd.DataFrame(
            {
                "driver_id": [1, 2],
                "event_timestamp": pd.to_datetime(
                    ["2025-01-01T12:00:00Z", "2025-01-01T12:00:00Z"]
                ),
            }
        )

        job = DuckDBOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[fv],
            feature_refs=["driver_stats:conv_rate"],
            entity_df=entity_df,
            registry=MagicMock(),
            project="proj",
            full_feature_names=False,
        )

        df = job.to_df()

        assert set(df["driver_id"]) == {1, 2}
        assert len(df) == 2
