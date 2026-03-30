"""Unit tests for versioned feature view support in FaissOnlineStore."""
import sys
from datetime import timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32

# ---------------------------------------------------------------------------
# Helpers to build lightweight FeatureView fixtures without faiss
# ---------------------------------------------------------------------------


def _make_feature_view(name: str, version_number=None, version_tag=None):
    entity = Entity(name="driver_id", join_keys=["driver_id"])
    fv = FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="feature_a", dtype=Float32)],
    )
    if version_number is not None:
        fv.current_version_number = version_number
    if version_tag is not None:
        fv.projection.version_tag = version_tag
    return fv


# ---------------------------------------------------------------------------
# _table_id tests (no faiss needed — we mock the import at module level)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _mock_faiss():
    """Inject a minimal faiss mock so faiss_online_store can be imported."""
    faiss_mock = MagicMock()
    with patch.dict(sys.modules, {"faiss": faiss_mock}):
        # Remove cached module so the patched version is used
        sys.modules.pop("feast.infra.online_stores.faiss_online_store", None)
        yield faiss_mock
    sys.modules.pop("feast.infra.online_stores.faiss_online_store", None)


class TestFaissTableId:
    def test_no_versioning(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view("driver_stats")
        assert _table_id("my_project", fv) == "my_project_driver_stats"

    def test_versioning_disabled_ignores_version_number(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view("driver_stats", version_number=3)
        assert _table_id("my_project", fv, enable_versioning=False) == "my_project_driver_stats"

    def test_versioning_enabled_v0_no_suffix(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view("driver_stats", version_number=0)
        # version 0 should NOT produce a suffix
        assert _table_id("my_project", fv, enable_versioning=True) == "my_project_driver_stats"

    def test_versioning_enabled_version_number(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view("driver_stats", version_number=2)
        assert (
            _table_id("my_project", fv, enable_versioning=True)
            == "my_project_driver_stats_v2"
        )

    def test_versioning_enabled_version_tag_takes_precedence(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        # version_tag on the projection takes precedence over current_version_number
        fv = _make_feature_view("driver_stats", version_number=1, version_tag=3)
        assert (
            _table_id("my_project", fv, enable_versioning=True)
            == "my_project_driver_stats_v3"
        )

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view("driver_stats")
        # No version information — falls back to bare name
        assert (
            _table_id("my_project", fv, enable_versioning=True)
            == "my_project_driver_stats"
        )


# ---------------------------------------------------------------------------
# FaissOnlineStore versioned write / read tests (faiss is mocked)
# ---------------------------------------------------------------------------


def _make_config(project: str = "test_project", versioning: bool = False):
    """Build a minimal RepoConfig-like mock."""
    config = MagicMock()
    config.project = project
    config.entity_key_serialization_version = 2
    config.online_store.dict.return_value = {
        "dimension": 1,
        "index_path": "/tmp/test.index",
        "index_type": "IVFFlat",
        "nlist": 10,
    }
    config.registry.enable_online_feature_view_versioning = versioning
    return config


def _make_entity_key(driver_id: int = 1):
    return EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int64_val=driver_id)],
    )


class TestFaissOnlineStoreVersionedWrite:
    def _make_store(self, faiss_mock, nlist: int = 10):
        """Create a FaissOnlineStore with a real-enough faiss mock."""
        # Make the index mock respond correctly
        index_mock = MagicMock()
        index_mock.ntotal = 0

        def add_side_effect(vectors):
            index_mock.ntotal += len(vectors)

        index_mock.add.side_effect = add_side_effect

        def reconstruct_side_effect(idx):
            return np.array([float(idx)], dtype=np.float32)

        index_mock.reconstruct.side_effect = reconstruct_side_effect

        faiss_mock.IndexFlatL2.return_value = MagicMock()
        faiss_mock.IndexIVFFlat.return_value = index_mock

        from feast.infra.online_stores.faiss_online_store import FaissOnlineStore

        store = FaissOnlineStore()
        # Reset class-level dicts to avoid test pollution
        store._indices = {}
        store._in_memory_stores = {}
        return store, index_mock

    def test_write_and_read_without_versioning(self, _mock_faiss):
        store, index_mock = self._make_store(_mock_faiss)
        config = _make_config(versioning=False)
        fv = _make_feature_view("driver_stats")

        store.update(config, [], [fv], [], [], partial=False)

        entity_key = _make_entity_key(driver_id=42)
        data = [
            (entity_key, {"feature_a": ValueProto(double_val=1.5)}, None, None)
        ]
        store.online_write_batch(config, fv, data, None)

        results = store.online_read(config, fv, [entity_key])
        assert len(results) == 1
        ts, feature_dict = results[0]
        assert feature_dict is not None
        assert "feature_a" in feature_dict

    def test_write_and_read_with_versioning(self, _mock_faiss):
        store, index_mock = self._make_store(_mock_faiss)
        config = _make_config(versioning=True)
        fv_v2 = _make_feature_view("driver_stats", version_number=2)

        store.update(config, [], [fv_v2], [], [], partial=False)

        entity_key = _make_entity_key(driver_id=7)
        data = [
            (entity_key, {"feature_a": ValueProto(double_val=2.0)}, None, None)
        ]
        store.online_write_batch(config, fv_v2, data, None)

        results = store.online_read(config, fv_v2, [entity_key])
        assert len(results) == 1
        _, feature_dict = results[0]
        assert feature_dict is not None

    def test_versioned_namespaces_are_isolated(self, _mock_faiss):
        """Data written under v1 must not be visible when reading under v2."""
        store, _ = self._make_store(_mock_faiss)
        config = _make_config(versioning=True)

        fv_v1 = _make_feature_view("driver_stats", version_number=1)
        fv_v2 = _make_feature_view("driver_stats", version_number=2)

        store.update(config, [], [fv_v1, fv_v2], [], [], partial=False)

        entity_key = _make_entity_key(driver_id=99)
        data = [
            (entity_key, {"feature_a": ValueProto(double_val=9.9)}, None, None)
        ]
        # Write only to v1
        store.online_write_batch(config, fv_v1, data, None)

        # Reading from v2 should return (None, None)
        results_v2 = store.online_read(config, fv_v2, [entity_key])
        assert results_v2 == [(None, None)]

        # Reading from v1 should return data
        results_v1 = store.online_read(config, fv_v1, [entity_key])
        assert results_v1[0][1] is not None

    def test_missing_index_returns_none(self, _mock_faiss):
        store, _ = self._make_store(_mock_faiss)
        config = _make_config(versioning=True)
        fv = _make_feature_view("driver_stats", version_number=5)
        # No update called — index not initialised
        entity_key = _make_entity_key(driver_id=1)
        results = store.online_read(config, fv, [entity_key])
        assert results == [(None, None)]

    def test_teardown_removes_versioned_index(self, _mock_faiss):
        store, _ = self._make_store(_mock_faiss)
        config = _make_config(versioning=True)
        fv = _make_feature_view("driver_stats", version_number=3)

        store.update(config, [], [fv], [], [], partial=False)

        entity_key = _make_entity_key(driver_id=1)
        data = [(entity_key, {"feature_a": ValueProto(double_val=3.0)}, None, None)]
        store.online_write_batch(config, fv, data, None)

        store.teardown(config, [fv], [])

        # After teardown, reads return None
        results = store.online_read(config, fv, [entity_key])
        assert results == [(None, None)]
