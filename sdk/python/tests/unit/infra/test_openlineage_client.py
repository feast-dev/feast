"""Tests for OpenLineage client functionality."""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest

from feast.data_source import FileSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_view import FeatureView
from feast.field import Field
from feast.lineage.openlineage_client import OpenLineageClient
from feast.repo_config import OpenLineageConfig
from feast.types import Float32


class TestOpenLineageClient:
    def test_client_disabled_by_default(self):
        """Test that OpenLineage client is disabled by default."""
        config = OpenLineageConfig()
        assert config.enabled is False

        client = OpenLineageClient(config)
        assert client._client is None

    def test_client_initialization_with_console_transport(self):
        """Test OpenLineage client initialization with console transport."""
        config = OpenLineageConfig(
            enabled=True,
            transport_type="console",
            transport_config={},
        )

        with patch("feast.lineage.openlineage_client.OpenLineageClient") as mock_ol_client:
            client = OpenLineageClient(config)
            # Client should attempt initialization
            assert config.enabled is True

    def test_client_handles_missing_openlineage_library(self):
        """Test that client handles missing OpenLineage library gracefully."""
        config = OpenLineageConfig(
            enabled=True,
            transport_type="http",
            transport_config={"url": "http://localhost:5000"},
        )

        with patch(
            "feast.lineage.openlineage_client.OpenLineageClient",
            side_effect=ImportError("openlineage not found"),
        ):
            client = OpenLineageClient(config)
            # Should disable itself when library is not available
            assert config.enabled is False
            assert client._client is None

    def test_emit_materialize_start_event_when_disabled(self):
        """Test that no event is emitted when client is disabled."""
        config = OpenLineageConfig(enabled=False)
        client = OpenLineageClient(config)

        # Create a simple feature view
        file_source = FileSource(path="data/driver_stats.parquet", name="driver_source")
        driver_entity = Entity(name="driver", join_keys=["driver_id"])
        
        feature_view = FeatureView(
            name="driver_hourly_stats",
            entities=[driver_entity],
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="acc_rate", dtype=Float32),
            ],
            source=file_source,
        )

        start_date = datetime.now(timezone.utc) - timedelta(days=1)
        end_date = datetime.now(timezone.utc)

        result = client.emit_materialize_start_event(
            feature_view=feature_view,
            start_date=start_date,
            end_date=end_date,
            project="test_project",
        )

        assert result is None

    @patch("feast.lineage.openlineage_client.OLClient")
    def test_emit_materialize_start_event_success(self, mock_ol_client_class):
        """Test successful emission of materialization START event."""
        config = OpenLineageConfig(
            enabled=True,
            transport_type="console",
            emit_materialization_events=True,
        )

        # Mock the OpenLineage client
        mock_client_instance = Mock()
        mock_ol_client_class.return_value = mock_client_instance

        with patch("feast.lineage.openlineage_client.get_default_factory") as mock_factory:
            mock_transport = Mock()
            mock_factory.return_value.create.return_value = mock_transport

            client = OpenLineageClient(config)
            client._client = mock_client_instance

            # Create a simple feature view
            file_source = FileSource(path="data/driver_stats.parquet", name="driver_source")
            driver_entity = Entity(name="driver", join_keys=["driver_id"])
            
            feature_view = FeatureView(
                name="driver_hourly_stats",
                entities=[driver_entity],
                schema=[
                    Field(name="conv_rate", dtype=Float32),
                    Field(name="acc_rate", dtype=Float32),
                ],
                source=file_source,
            )

            start_date = datetime.now(timezone.utc) - timedelta(days=1)
            end_date = datetime.now(timezone.utc)

            with patch("feast.lineage.openlineage_client.RunEvent"):
                run_id = client.emit_materialize_start_event(
                    feature_view=feature_view,
                    start_date=start_date,
                    end_date=end_date,
                    project="test_project",
                )

                # Should return a run ID
                assert run_id is not None
                assert isinstance(run_id, str)

                # Client emit should have been called
                assert mock_client_instance.emit.called

    @patch("feast.lineage.openlineage_client.OLClient")
    def test_emit_materialize_complete_event_success(self, mock_ol_client_class):
        """Test successful emission of materialization COMPLETE event."""
        config = OpenLineageConfig(
            enabled=True,
            transport_type="console",
            emit_materialization_events=True,
        )

        # Mock the OpenLineage client
        mock_client_instance = Mock()
        mock_ol_client_class.return_value = mock_client_instance

        with patch("feast.lineage.openlineage_client.get_default_factory") as mock_factory:
            mock_transport = Mock()
            mock_factory.return_value.create.return_value = mock_transport

            client = OpenLineageClient(config)
            client._client = mock_client_instance

            # Create a simple feature view
            file_source = FileSource(path="data/driver_stats.parquet", name="driver_source")
            driver_entity = Entity(name="driver", join_keys=["driver_id"])
            
            feature_view = FeatureView(
                name="driver_hourly_stats",
                entities=[driver_entity],
                schema=[
                    Field(name="conv_rate", dtype=Float32),
                    Field(name="acc_rate", dtype=Float32),
                ],
                source=file_source,
            )

            start_date = datetime.now(timezone.utc) - timedelta(days=1)
            end_date = datetime.now(timezone.utc)
            run_id = str(uuid.uuid4())

            with patch("feast.lineage.openlineage_client.RunEvent"):
                client.emit_materialize_complete_event(
                    feature_view=feature_view,
                    start_date=start_date,
                    end_date=end_date,
                    project="test_project",
                    run_id=run_id,
                    success=True,
                )

                # Client emit should have been called
                assert mock_client_instance.emit.called

    @patch("feast.lineage.openlineage_client.OLClient")
    def test_emit_materialize_fail_event(self, mock_ol_client_class):
        """Test emission of materialization FAIL event."""
        config = OpenLineageConfig(
            enabled=True,
            transport_type="console",
            emit_materialization_events=True,
        )

        # Mock the OpenLineage client
        mock_client_instance = Mock()
        mock_ol_client_class.return_value = mock_client_instance

        with patch("feast.lineage.openlineage_client.get_default_factory") as mock_factory:
            mock_transport = Mock()
            mock_factory.return_value.create.return_value = mock_transport

            client = OpenLineageClient(config)
            client._client = mock_client_instance

            # Create a simple feature view
            file_source = FileSource(path="data/driver_stats.parquet", name="driver_source")
            driver_entity = Entity(name="driver", join_keys=["driver_id"])
            
            feature_view = FeatureView(
                name="driver_hourly_stats",
                entities=[driver_entity],
                schema=[
                    Field(name="conv_rate", dtype=Float32),
                    Field(name="acc_rate", dtype=Float32),
                ],
                source=file_source,
            )

            start_date = datetime.now(timezone.utc) - timedelta(days=1)
            end_date = datetime.now(timezone.utc)
            run_id = str(uuid.uuid4())

            with patch("feast.lineage.openlineage_client.RunEvent"):
                client.emit_materialize_complete_event(
                    feature_view=feature_view,
                    start_date=start_date,
                    end_date=end_date,
                    project="test_project",
                    run_id=run_id,
                    success=False,
                )

                # Client emit should have been called
                assert mock_client_instance.emit.called

    def test_openlineage_config_defaults(self):
        """Test OpenLineageConfig default values."""
        config = OpenLineageConfig()

        assert config.enabled is False
        assert config.transport_type == "http"
        assert config.transport_config == {}
        assert config.namespace == "feast"
        assert config.emit_materialization_events is True
        assert config.emit_retrieval_events is False

    def test_openlineage_config_custom_values(self):
        """Test OpenLineageConfig with custom values."""
        config = OpenLineageConfig(
            enabled=True,
            transport_type="kafka",
            transport_config={"bootstrap.servers": "localhost:9092", "topic": "lineage"},
            namespace="my_feast",
            emit_materialization_events=False,
            emit_retrieval_events=True,
        )

        assert config.enabled is True
        assert config.transport_type == "kafka"
        assert config.transport_config["bootstrap.servers"] == "localhost:9092"
        assert config.namespace == "my_feast"
        assert config.emit_materialization_events is False
        assert config.emit_retrieval_events is True

    def test_get_dataset_name(self):
        """Test dataset name generation from data source."""
        config = OpenLineageConfig(enabled=False)
        client = OpenLineageClient(config)

        # Test with named source
        file_source = FileSource(path="data/driver_stats.parquet", name="driver_source")
        name = client._get_dataset_name(file_source, "driver_features", "input")
        assert name == "driver_source"

        # Test with unnamed source (fallback)
        unnamed_source = FileSource(path="data/stats.parquet")
        name = client._get_dataset_name(unnamed_source, "driver_features", "input")
        assert name == "driver_features_input"

    def test_get_source_uri(self):
        """Test URI extraction from data sources."""
        config = OpenLineageConfig(enabled=False)
        client = OpenLineageClient(config)

        # Test with path-based source
        file_source = FileSource(path="data/driver_stats.parquet", name="driver_source")
        uri = client._get_source_uri(file_source)
        assert uri == "data/driver_stats.parquet"
