"""Integration test for MlflowDatasetSource with get_historical_features.

Uses mocked MLflow calls and a real DuckDB offline store to exercise
the full DataSource → to_arrow → ibis memtable → get_historical_features path.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.infra.data_sources.mlflow import MlflowDatasetSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Float64, String
from feast.value_type import ValueType


@pytest.fixture
def tmp_feast_repo(tmp_path):
    """Create a temporary Feast repo with DuckDB offline store."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    now = datetime.now(tz=timezone.utc)
    entity_df = pd.DataFrame(
        {
            "record_id": ["r1", "r2", "r3"],
            "event_timestamp": [
                now - timedelta(hours=3),
                now - timedelta(hours=2),
                now - timedelta(hours=1),
            ],
        }
    )

    feature_df = pd.DataFrame(
        {
            "record_id": ["r1", "r2", "r3"],
            "score": [0.9, 0.8, 0.95],
            "category": ["A", "B", "A"],
            "event_timestamp": [
                now - timedelta(hours=3),
                now - timedelta(hours=2),
                now - timedelta(hours=1),
            ],
        }
    )

    feature_path = str(data_dir / "features.parquet")
    pq.write_table(pa.Table.from_pandas(feature_df), feature_path)

    entity_path = str(data_dir / "entities.parquet")
    pq.write_table(pa.Table.from_pandas(entity_df), entity_path)

    return tmp_path, feature_path, entity_path, feature_df, entity_df


class TestArtifactModeGetHistoricalFeatures:
    """Test artifact-mode MlflowDatasetSource end-to-end with DuckDB."""

    def test_get_historical_features_artifact_mode(self, tmp_feast_repo):
        """Verify that get_historical_features works with an artifact-mode source.

        Mocks MlflowDatasetSource.to_arrow() to return a known PyArrow table,
        exercising the full DuckDB ibis path without needing a real MLflow server.
        """
        tmp_path, feature_path, _, _, entity_df = tmp_feast_repo

        now = datetime.now(tz=timezone.utc)
        expected_data = pa.table(
            {
                "record_id": ["r1", "r2", "r3"],
                "score": [0.9, 0.8, 0.95],
                "category": ["A", "B", "A"],
                "event_timestamp": [
                    now - timedelta(hours=3),
                    now - timedelta(hours=2),
                    now - timedelta(hours=1),
                ],
            }
        )

        batch_source = FileSource(path=feature_path, timestamp_field="event_timestamp")
        mlflow_src = MlflowDatasetSource(
            name="artifact_src",
            run_id="test-run-123",
            artifact_path="outputs/artifact.parquet",
            artifact_format="parquet",
            batch_source=batch_source,
            timestamp_field="event_timestamp",
        )

        entity = Entity(
            name="record_id",
            join_keys=["record_id"],
            value_type=ValueType.STRING,
        )
        fv = FeatureView(
            name="artifact_features",
            entities=[entity],
            schema=[
                Field(name="score", dtype=Float64),
                Field(name="category", dtype=String),
            ],
            source=mlflow_src,
            ttl=timedelta(days=1),
        )

        assert isinstance(fv.batch_source, MlflowDatasetSource)

        config = RepoConfig(
            project="test_project",
            registry=str(tmp_path / "registry.db"),
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=str(tmp_path / "online.db")),
            offline_store="duckdb",
            entity_key_serialization_version=3,
        )

        store = FeatureStore(config=config)
        store.apply([entity, fv])

        with patch.object(MlflowDatasetSource, "to_arrow", return_value=expected_data):
            result = store.get_historical_features(
                entity_df=entity_df,
                features=["artifact_features:score", "artifact_features:category"],
            )
            result_df = result.to_df()

        assert len(result_df) == 3
        assert "score" in result_df.columns
        assert "category" in result_df.columns
        assert set(result_df["record_id"]) == {"r1", "r2", "r3"}
        assert all(result_df["score"].notna())

    def test_get_historical_features_genai_mode(self, tmp_feast_repo):
        """Verify GenAI-mode source also works through get_historical_features."""
        tmp_path, feature_path, _, _, entity_df = tmp_feast_repo

        now = datetime.now(tz=timezone.utc)
        expected_data = pa.table(
            {
                "record_id": ["r1", "r2", "r3"],
                "score": [0.7, 0.85, 0.9],
                "category": ["B", "A", "A"],
                "event_timestamp": [
                    now - timedelta(hours=3),
                    now - timedelta(hours=2),
                    now - timedelta(hours=1),
                ],
            }
        )

        batch_source = FileSource(path=feature_path, timestamp_field="event_timestamp")
        mlflow_src = MlflowDatasetSource(
            name="genai_src",
            dataset_name="eval_dataset",
            batch_source=batch_source,
            timestamp_field="event_timestamp",
        )

        entity = Entity(
            name="record_id",
            join_keys=["record_id"],
            value_type=ValueType.STRING,
        )
        fv = FeatureView(
            name="genai_features",
            entities=[entity],
            schema=[
                Field(name="score", dtype=Float64),
                Field(name="category", dtype=String),
            ],
            source=mlflow_src,
            ttl=timedelta(days=1),
        )

        config = RepoConfig(
            project="test_project",
            registry=str(tmp_path / "registry_genai.db"),
            provider="local",
            online_store=SqliteOnlineStoreConfig(
                path=str(tmp_path / "online_genai.db")
            ),
            offline_store="duckdb",
            entity_key_serialization_version=3,
        )

        store = FeatureStore(config=config)
        store.apply([entity, fv])

        with patch.object(MlflowDatasetSource, "to_arrow", return_value=expected_data):
            result = store.get_historical_features(
                entity_df=entity_df,
                features=["genai_features:score", "genai_features:category"],
            )
            result_df = result.to_df()

        assert len(result_df) == 3
        assert "score" in result_df.columns
        assert "category" in result_df.columns


class TestProtoRoundTripIntegration:
    """Test that MlflowDatasetSource survives proto round-trip in a FeatureStore."""

    def test_apply_and_retrieve_feature_view(self, tmp_feast_repo):
        tmp_path, feature_path, _, _, _ = tmp_feast_repo

        batch_source = FileSource(path=feature_path, timestamp_field="event_timestamp")
        mlflow_src = MlflowDatasetSource(
            name="genai_src",
            dataset_name="eval_dataset",
            batch_source=batch_source,
            timestamp_field="event_timestamp",
        )

        entity = Entity(
            name="record_id",
            join_keys=["record_id"],
            value_type=ValueType.STRING,
        )
        fv = FeatureView(
            name="genai_features",
            entities=[entity],
            schema=[
                Field(name="score", dtype=Float64),
                Field(name="category", dtype=String),
            ],
            source=mlflow_src,
            ttl=timedelta(days=1),
        )

        config = RepoConfig(
            project="test_project",
            registry=str(tmp_path / "registry.db"),
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=str(tmp_path / "online.db")),
            offline_store="duckdb",
            entity_key_serialization_version=3,
        )

        store = FeatureStore(config=config)
        store.apply([entity, fv])

        retrieved_fv = store.get_feature_view("genai_features")
        assert isinstance(retrieved_fv.batch_source, MlflowDatasetSource)

        retrieved_src = retrieved_fv.batch_source
        assert retrieved_src.dataset_name == "eval_dataset"
        assert retrieved_src.is_genai_mode
        assert not retrieved_src.is_artifact_mode
        assert isinstance(retrieved_src.batch_source, FileSource)

    def test_apply_artifact_mode_feature_view(self, tmp_feast_repo):
        tmp_path, feature_path, _, _, _ = tmp_feast_repo

        batch_source = FileSource(path=feature_path, timestamp_field="event_timestamp")
        mlflow_src = MlflowDatasetSource(
            name="artifact_src",
            run_id="run-abc",
            artifact_path="outputs/features.parquet",
            artifact_format="parquet",
            batch_source=batch_source,
            timestamp_field="event_timestamp",
        )

        entity = Entity(
            name="record_id",
            join_keys=["record_id"],
            value_type=ValueType.STRING,
        )
        fv = FeatureView(
            name="artifact_features",
            entities=[entity],
            schema=[Field(name="score", dtype=Float64)],
            source=mlflow_src,
            ttl=timedelta(days=1),
        )

        config = RepoConfig(
            project="test_project",
            registry=str(tmp_path / "registry2.db"),
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=str(tmp_path / "online2.db")),
            offline_store="duckdb",
            entity_key_serialization_version=3,
        )

        store = FeatureStore(config=config)
        store.apply([entity, fv])

        retrieved_fv = store.get_feature_view("artifact_features")
        retrieved_src = retrieved_fv.batch_source
        assert isinstance(retrieved_src, MlflowDatasetSource)
        assert retrieved_src.run_id == "run-abc"
        assert retrieved_src.artifact_path == "outputs/features.parquet"
        assert retrieved_src.artifact_format == "parquet"
        assert retrieved_src.is_artifact_mode
