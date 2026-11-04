from unittest.mock import MagicMock

from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.local.compute import (
    LocalComputeEngine,
    LocalComputeEngineConfig,
)


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
