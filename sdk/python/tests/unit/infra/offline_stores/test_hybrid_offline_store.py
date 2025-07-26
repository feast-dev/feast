import datetime
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow
import pytest

from feast import FeatureView
from feast.data_source import DataSource
from feast.infra.offline_stores.bigquery import BigQueryOfflineStoreConfig
from feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store import (
    HybridOfflineStore,
    HybridOfflineStoreConfig,
    OfflineStoreConfig,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig


class MockDataSource(DataSource):
    """Mock data source for testing."""

    def __init__(
        self,
        name="mock",
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        field_mapping=None,
        date_partition_column=None,
        description="",
        tags=None,
        owner="",
    ):
        self.name = name
        self.timestamp_field = timestamp_field
        self.created_timestamp_column = created_timestamp_column or ""
        self.field_mapping = field_mapping or {}
        self.date_partition_column = date_partition_column or ""
        self.description = description
        self.tags = tags or {}
        self.owner = owner

    @staticmethod
    def from_proto(proto):
        raise NotImplementedError("Not implemented for testing")

    def to_proto(self):
        raise NotImplementedError("Not implemented for testing")


class BigquerySource(MockDataSource):
    """Mock BigQuery source for testing. The name matches the pattern expected by HybridOfflineStore."""

    pass


class FileSource(MockDataSource):
    """Mock File source for testing. The name matches the pattern expected by HybridOfflineStore."""

    pass


class MockRetrievalJob:
    """Mock retrieval job for testing."""

    def __init__(self, df=None, arrow=None):
        self._df = df if df is not None else pd.DataFrame()
        self._arrow = (
            arrow if arrow is not None else pyarrow.Table.from_pandas(self._df)
        )

    def to_df(self, timeout=None):
        return self._df

    def to_arrow(self, timeout=None):
        return self._arrow


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        data={
            "entity_id": [1, 2, 3],
            "feature_value": [10.0, 20.0, 30.0],
            "event_timestamp": [
                datetime.datetime(2021, 1, 1),
                datetime.datetime(2021, 1, 2),
                datetime.datetime(2021, 1, 3),
            ],
        }
    )


@pytest.fixture
def bigquery_offline_store():
    """Create a mocked BigQuery offline store."""
    mock_store = Mock()
    mock_store.pull_latest_from_table_or_query.return_value = MockRetrievalJob(
        df=pd.DataFrame({"entity_id": [1, 2], "feature": [1.0, 2.0]})
    )
    mock_store.get_historical_features.return_value = MockRetrievalJob(
        df=pd.DataFrame({"entity_id": [1, 2], "feature": [1.0, 2.0]})
    )
    return mock_store


@pytest.fixture
def file_offline_store():
    """Create a mocked File offline store."""
    mock_store = Mock()
    mock_store.pull_latest_from_table_or_query.return_value = MockRetrievalJob(
        df=pd.DataFrame({"entity_id": [3, 4], "feature": [3.0, 4.0]})
    )
    mock_store.pull_all_from_table_or_query.return_value = MockRetrievalJob(
        df=pd.DataFrame({"entity_id": [3, 4], "feature": [3.0, 4.0]})
    )
    mock_store.get_historical_features.return_value = MockRetrievalJob(
        df=pd.DataFrame({"entity_id": [3, 4], "feature": [3.0, 4.0]})
    )
    return mock_store


@pytest.fixture
def repo_config():
    """Create a repo config with hybrid offline store."""
    return RepoConfig(
        registry="registry.db",
        project="test_project",
        provider="local",
        entity_key_serialization_version=3,  # Use recommended version to avoid deprecation warning
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=HybridOfflineStoreConfig(
            type="feast.infra.offline_stores.hybrid_offline_store.HybridOfflineStore",
            offline_stores=[
                OfflineStoreConfig(type="bigquery", conf={"dataset": "feast_test"}),
                OfflineStoreConfig(type="file", conf={"path": "/tmp/feast_test"}),
            ],
        ),
    )


@pytest.fixture
def mock_feature_view():
    """Create a mocked feature view."""
    feature_view = Mock(spec=FeatureView)
    feature_view.batch_source = BigquerySource()
    return feature_view


@pytest.fixture
def mock_feature_views():
    """Create mocked feature views with the same source type."""
    fv1 = Mock(spec=FeatureView)
    fv1.batch_source = BigquerySource(name="source1")
    fv2 = Mock(spec=FeatureView)
    fv2.batch_source = BigquerySource(name="source2")
    return [fv1, fv2]


@pytest.fixture
def mock_mixed_feature_views():
    """Create mocked feature views with different source types."""
    fv1 = Mock(spec=FeatureView)
    fv1.batch_source = BigquerySource()
    fv2 = Mock(spec=FeatureView)
    fv2.batch_source = FileSource()
    return [fv1, fv2]


class TestHybridOfflineStore:
    def test_get_store_and_config_by_source(self, repo_config, monkeypatch):
        """Test getting the correct store for a data source."""
        # Mock the get_offline_store_from_config function
        mock_get_store = Mock()
        mock_get_store.return_value = Mock()

        # Mock the get_offline_config_from_type function
        mock_config_type = Mock()
        mock_config_type.return_value = Mock()
        mock_config_type.return_value.return_value = BigQueryOfflineStoreConfig(
            type="bigquery", dataset="feast_test"
        )

        monkeypatch.setattr(
            "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_store_from_config",
            mock_get_store,
        )
        monkeypatch.setattr(
            "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_config_from_type",
            mock_config_type,
        )

        # Test with BigQuery source
        bigquery_source = BigquerySource()
        store, config = HybridOfflineStore._get_store_and_config_by_source(
            repo_config, bigquery_source
        )

        # Verify correct config type was requested
        mock_config_type.assert_called_with("bigquery")

        # Verify correct store was requested
        mock_get_store.assert_called_once()

        # Test with an unknown source type (should raise ValueError)
        unknown_source = Mock()
        unknown_source.__class__.__name__ = "UnknownSource"

        with pytest.raises(
            ValueError,
            match=r"No offline store configuration found for source type 'UnknownSource'",
        ):
            HybridOfflineStore._get_store_and_config_by_source(
                repo_config, unknown_source
            )

    def test_validate_feature_views_source_type(
        self, mock_feature_views, mock_mixed_feature_views
    ):
        """Test validation of feature view source types."""
        # Should pass with single feature view
        HybridOfflineStore._validate_feature_views_source_type([mock_feature_views[0]])

        # Should pass with multiple feature views of same type
        HybridOfflineStore._validate_feature_views_source_type(mock_feature_views)

        # Should fail with mixed feature view types
        with pytest.raises(
            ValueError, match="All feature views must have the same source type"
        ):
            HybridOfflineStore._validate_feature_views_source_type(
                mock_mixed_feature_views
            )

    @patch(
        "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_store_from_config"
    )
    def test_pull_latest_from_table_or_query(
        self, mock_get_store, repo_config, bigquery_offline_store
    ):
        """Test pull_latest_from_table_or_query routes to correct store."""
        mock_get_store.return_value = bigquery_offline_store

        data_source = BigquerySource()
        start_date = datetime.datetime(2021, 1, 1)
        end_date = datetime.datetime(2021, 1, 2)

        result = HybridOfflineStore.pull_latest_from_table_or_query(
            config=repo_config,
            data_source=data_source,
            join_key_columns=["entity_id"],
            feature_name_columns=["feature"],
            timestamp_field="event_timestamp",
            created_timestamp_column=None,
            start_date=start_date,
            end_date=end_date,
        )

        # Verify the BigQuery store was called with correct parameters
        bigquery_offline_store.pull_latest_from_table_or_query.assert_called_once()
        call_kwargs = (
            bigquery_offline_store.pull_latest_from_table_or_query.call_args.kwargs
        )
        assert call_kwargs["data_source"] == data_source
        assert call_kwargs["join_key_columns"] == ["entity_id"]
        assert call_kwargs["feature_name_columns"] == ["feature"]
        assert call_kwargs["timestamp_field"] == "event_timestamp"

        # Verify result
        df = result.to_df()
        assert len(df) == 2
        assert "entity_id" in df.columns
        assert "feature" in df.columns

    @patch(
        "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_store_from_config"
    )
    def test_get_historical_features_same_source(
        self, mock_get_store, repo_config, bigquery_offline_store, mock_feature_views
    ):
        """Test get_historical_features with feature views of the same source type."""
        mock_get_store.return_value = bigquery_offline_store

        entity_df = pd.DataFrame({"entity_id": [1, 2]})

        result = HybridOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=mock_feature_views,
            feature_refs=["feature_view:feature"],
            entity_df=entity_df,
            registry=Mock(),
            project="test_project",
            full_feature_names=True,
        )

        # Verify the BigQuery store was called with correct parameters
        bigquery_offline_store.get_historical_features.assert_called_once()
        call_kwargs = bigquery_offline_store.get_historical_features.call_args.kwargs
        assert call_kwargs["feature_views"] == mock_feature_views
        assert call_kwargs["feature_refs"] == ["feature_view:feature"]
        assert call_kwargs["project"] == "test_project"
        assert call_kwargs[
            "full_feature_names"
        ]  # Use truth test instead of comparing to True

        # Verify result
        df = result.to_df()
        assert len(df) == 2
        assert "entity_id" in df.columns
        assert "feature" in df.columns

    @patch(
        "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_store_from_config"
    )
    def test_get_historical_features_mixed_sources(
        self, mock_get_store, repo_config, mock_mixed_feature_views
    ):
        """Test get_historical_features with feature views of different source types."""
        with pytest.raises(
            ValueError, match="All feature views must have the same source type"
        ):
            HybridOfflineStore.get_historical_features(
                config=repo_config,
                feature_views=mock_mixed_feature_views,
                feature_refs=["feature_view:feature"],
                entity_df=pd.DataFrame({"entity_id": [1, 2]}),
                registry=Mock(),
                project="test_project",
                full_feature_names=True,
            )

    @patch(
        "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_store_from_config"
    )
    @patch(
        "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_config_from_type"
    )
    @patch(
        "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.RepoConfig"
    )
    def test_pull_all_from_table_or_query(
        self,
        mock_repo_config,
        mock_config_type,
        mock_get_store,
        repo_config,
        file_offline_store,
    ):
        """Test pull_all_from_table_or_query routes to correct store."""
        # Set up the mock for config type conversion
        mock_config = Mock()
        mock_config_type.return_value = Mock()
        mock_config_type.return_value.return_value = mock_config

        # Set up the RepoConfig mock to avoid validation errors
        mock_repo_config.return_value = repo_config

        # Set up the store mock
        mock_get_store.return_value = file_offline_store

        data_source = FileSource()

        result = HybridOfflineStore.pull_all_from_table_or_query(
            config=repo_config,
            data_source=data_source,
            join_key_columns=["entity_id"],
            feature_name_columns=["feature"],
            timestamp_field="event_timestamp",
        )

        # Verify the correct config type was requested
        mock_config_type.assert_called_with("file")

        # Verify the File store was called with correct parameters
        file_offline_store.pull_all_from_table_or_query.assert_called_once()
        call_kwargs = file_offline_store.pull_all_from_table_or_query.call_args.kwargs
        assert call_kwargs["data_source"] == data_source
        assert call_kwargs["join_key_columns"] == ["entity_id"]
        assert call_kwargs["feature_name_columns"] == ["feature"]
        assert call_kwargs["timestamp_field"] == "event_timestamp"

        # Verify result
        df = result.to_df()
        assert len(df) == 2
        assert "entity_id" in df.columns
        assert "feature" in df.columns

    @patch(
        "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_store_from_config"
    )
    def test_offline_write_batch(
        self,
        mock_get_store,
        repo_config,
        bigquery_offline_store,
        mock_feature_view,
        sample_df,
    ):
        """Test offline_write_batch routes to correct store."""
        mock_get_store.return_value = bigquery_offline_store

        table = pyarrow.Table.from_pandas(sample_df)
        progress_callback = Mock()

        HybridOfflineStore.offline_write_batch(
            config=repo_config,
            feature_view=mock_feature_view,
            table=table,
            progress=progress_callback,
        )

        # Verify the BigQuery store was called with correct parameters
        bigquery_offline_store.offline_write_batch.assert_called_once()
        call_kwargs = bigquery_offline_store.offline_write_batch.call_args.kwargs
        assert call_kwargs["feature_view"] == mock_feature_view
        assert call_kwargs["table"] == table
        assert call_kwargs["progress"] == progress_callback

    @patch(
        "feast.infra.offline_stores.hybrid_offline_store.hybrid_offline_store.get_offline_store_from_config"
    )
    def test_prepare_store_config(self, mock_get_store, repo_config):
        """Test store config preparation."""
        store_config = repo_config.offline_store.offline_stores[0]
        result = HybridOfflineStore._prepare_store_config(repo_config, store_config)

        # Verify key transformations
        assert result["registry"] == repo_config.__dict__["registry_config"]

        # The offline_store config will include type, so we need to check individual expected keys
        assert result["offline_store"]["dataset"] == store_config.conf["dataset"]
        assert result["offline_store"]["type"] == store_config.type

        assert result["online_store"] == repo_config.__dict__["online_config"]
