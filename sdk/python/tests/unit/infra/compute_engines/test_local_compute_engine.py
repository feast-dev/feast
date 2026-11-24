from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from feast import (
    Aggregation,
    BatchFeatureView,
    Entity,
    Field,
    FileSource,
    RepoConfig,
    ValueType,
)
from feast.feature_view import DUMMY_ENTITY
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.local.compute import (
    LocalComputeEngine,
    LocalComputeEngineConfig,
)
from feast.infra.compute_engines.utils import ENTITY_ROW_ID
from feast.infra.offline_stores.dask import DaskOfflineStore, DaskOfflineStoreConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.types import Float32, Int64


class TestLocalComputeEngineConfig:
    def test_default_type(self):
        config = LocalComputeEngineConfig()
        assert config.type == "local"

    def test_default_backend_none(self):
        config = LocalComputeEngineConfig()
        assert config.backend is None

    def test_custom_backend(self):
        config = LocalComputeEngineConfig(backend="polars")
        assert config.backend == "polars"


class TestLocalComputeEngine:
    def _make_engine(self):
        return LocalComputeEngine(
            repo_config=MagicMock(),
            offline_store=MagicMock(),
            online_store=MagicMock(),
        )

    def test_is_compute_engine(self):
        engine = self._make_engine()
        assert isinstance(engine, ComputeEngine)

    def test_update_is_noop(self):
        engine = self._make_engine()
        # Should not raise
        engine.update(
            project="test",
            views_to_delete=[],
            views_to_keep=[],
            entities_to_delete=[],
            entities_to_keep=[],
        )

    def test_teardown_is_noop(self):
        engine = self._make_engine()
        # Should not raise
        engine.teardown_infra(
            project="test",
            fvs=[],
            entities=[],
        )

    def test_stores_config(self):
        repo_config = MagicMock()
        offline = MagicMock()
        online = MagicMock()
        engine = LocalComputeEngine(
            repo_config=repo_config,
            offline_store=offline,
            online_store=online,
        )
        assert engine.repo_config is repo_config
        assert engine.offline_store is offline
        assert engine.online_store is online


class TestLocalComputeEngineHistoricalRetrieval:
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    schema = [
        Field(name="driver_id", dtype=Int64),
        Field(name="conv_rate", dtype=Float32),
    ]

    def _source_view(self, tmp_path):
        source_path = str(tmp_path / "driver_stats.parquet")
        pd.DataFrame(
            {
                "driver_id": [1, 1, 2],
                "conv_rate": [0.1, 0.9, 0.5],
                "event_timestamp": [self.t0, self.t0 + timedelta(hours=2), self.t0],
            }
        ).to_parquet(source_path)
        return BatchFeatureView(
            name="driver_stats",
            entities=[self.driver],
            mode="pandas",
            schema=self.schema,
            source=FileSource(path=source_path, timestamp_field="event_timestamp"),
            ttl=timedelta(days=1),
            online=False,
            offline=False,
        )

    def _get_historical_features(
        self, tmp_path, feature_view, entity_df=None, entity=None, backend="pandas"
    ):
        if backend == "polars":
            pytest.importorskip("polars")
        repo_config = RepoConfig(
            project="test_local_engine",
            registry=str(tmp_path / "registry.db"),
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=str(tmp_path / "online.db")),
            offline_store=DaskOfflineStoreConfig(),
            entity_key_serialization_version=3,
        )
        engine = LocalComputeEngine(
            repo_config=repo_config,
            offline_store=DaskOfflineStore(),
            online_store=MagicMock(),
            backend=backend,
        )

        # Driver 1 is requested twice: before and after its second feature
        # row. Driver 2 is not requested at all.
        if entity_df is None:
            entity_df = pd.DataFrame(
                {
                    "driver_id": [1, 1],
                    "event_timestamp": [
                        self.t0 + timedelta(hours=1),
                        self.t0 + timedelta(hours=3),
                    ],
                }
            )
        registry = MagicMock()
        registry.get_entity.return_value = entity or self.driver
        task = HistoricalRetrievalTask(
            project="test_local_engine",
            entity_df=entity_df,
            feature_view=feature_view,
            full_feature_name=False,
            registry=registry,
        )
        return engine.get_historical_features(registry, task).to_df()

    def test_get_historical_features_is_point_in_time_per_entity_row(self, tmp_path):
        df = self._get_historical_features(tmp_path, self._source_view(tmp_path))

        assert sorted(df["driver_id"].to_list()) == [1, 1]
        assert sorted(round(v, 6) for v in df["conv_rate"].to_list()) == [0.1, 0.9]
        assert ENTITY_ROW_ID not in df.columns

    def test_get_historical_features_for_derived_view(self, tmp_path):
        derived = BatchFeatureView(
            name="derived_driver_stats",
            entities=[self.driver],
            mode="pandas",
            schema=self.schema,
            source=self._source_view(tmp_path),
            sink_source=FileSource(
                path=str(tmp_path / "derived.parquet"),
                timestamp_field="event_timestamp",
            ),
            ttl=timedelta(days=1),
            online=False,
            offline=False,
        )

        df = self._get_historical_features(tmp_path, derived)

        assert sorted(df["driver_id"].to_list()) == [1, 1]
        assert sorted(round(v, 6) for v in df["conv_rate"].to_list()) == [0.1, 0.9]

    def test_get_historical_features_aggregates_per_entity_row(self, tmp_path):
        source_view = self._source_view(tmp_path)
        driver_id = Entity(
            name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
        )
        aggregated = BatchFeatureView(
            name="driver_stats_sum",
            entities=[driver_id],
            mode="pandas",
            schema=self.schema,
            source=source_view.batch_source,
            aggregations=[Aggregation(column="conv_rate", function="sum")],
            ttl=timedelta(days=1),
            online=False,
            offline=False,
        )

        df = self._get_historical_features(tmp_path, aggregated)

        assert sorted(df["driver_id"].to_list()) == [1, 1]
        assert sorted(round(v, 6) for v in df["sum_conv_rate"].to_list()) == [0.1, 1.0]

    def _same_request_twice(self):
        """Two entity rows with the same driver and timestamp, told apart by a label."""
        return pd.DataFrame(
            {
                "driver_id": [1, 1],
                "event_timestamp": [self.t0 + timedelta(hours=1)] * 2,
                "label": ["a", "b"],
            }
        )

    @pytest.mark.parametrize("backend", ["pandas", "polars"])
    def test_get_historical_features_keeps_entity_rows_with_the_same_key_and_time(
        self, tmp_path, backend
    ):
        df = self._get_historical_features(
            tmp_path,
            self._source_view(tmp_path),
            self._same_request_twice(),
            backend=backend,
        )

        assert sorted(df["label"].to_list()) == ["a", "b"]
        assert [round(v, 6) for v in df["conv_rate"].to_list()] == [0.1, 0.1]

    @pytest.mark.parametrize("backend", ["pandas", "polars"])
    def test_get_historical_features_aggregates_entity_rows_with_the_same_key_and_time(
        self, tmp_path, backend
    ):
        driver_id = Entity(
            name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
        )
        aggregated = BatchFeatureView(
            name="driver_stats_sum",
            entities=[driver_id],
            mode="pandas",
            schema=self.schema,
            source=self._source_view(tmp_path).batch_source,
            aggregations=[Aggregation(column="conv_rate", function="sum")],
            ttl=timedelta(days=1),
            online=False,
            offline=False,
        )

        df = self._get_historical_features(
            tmp_path, aggregated, self._same_request_twice(), backend=backend
        )

        assert [round(v, 6) for v in df["sum_conv_rate"].to_list()] == [0.1, 0.1]
        assert ENTITY_ROW_ID not in df.columns

    @pytest.mark.parametrize("backend", ["pandas", "polars"])
    def test_get_historical_features_for_view_without_entities(self, tmp_path, backend):
        source_path = str(tmp_path / "global_stats.parquet")
        pd.DataFrame(
            {
                "conv_rate": [0.1, 0.9],
                "event_timestamp": [self.t0, self.t0 + timedelta(hours=2)],
            }
        ).to_parquet(source_path)
        global_view = BatchFeatureView(
            name="global_stats",
            entities=[],
            mode="pandas",
            schema=[Field(name="conv_rate", dtype=Float32)],
            source=FileSource(path=source_path, timestamp_field="event_timestamp"),
            ttl=timedelta(days=1),
            online=False,
            offline=False,
        )
        entity_df = pd.DataFrame(
            {
                "event_timestamp": [
                    self.t0 + timedelta(hours=1),
                    self.t0 + timedelta(hours=3),
                ]
            }
        )

        df = self._get_historical_features(
            tmp_path, global_view, entity_df, entity=DUMMY_ENTITY, backend=backend
        )

        assert sorted(round(v, 6) for v in df["conv_rate"].to_list()) == [0.1, 0.9]
