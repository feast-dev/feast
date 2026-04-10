"""Unit tests for FAISS online store feature view versioning."""

import sys
from datetime import timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from feast import Entity, FeatureView
from feast.field import Field
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32
from feast.value_type import ValueType


def _make_feature_view(name="driver_stats", version_number=None, version_tag=None):
    entity = Entity(
        name="driver_id",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )
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


@pytest.fixture(autouse=True)
def _mock_faiss():
    """Inject a minimal faiss mock so faiss_online_store can be imported."""
    faiss_mock = MagicMock()
    with patch.dict(sys.modules, {"faiss": faiss_mock}):
        sys.modules.pop("feast.infra.online_stores.faiss_online_store", None)
        yield faiss_mock
    sys.modules.pop("feast.infra.online_stores.faiss_online_store", None)


class TestFaissTableId:
    """Test _table_id generates correct versioned table names."""

    def test_default_no_versioning(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view()
        assert _table_id("proj", fv) == "proj_driver_stats"

    def test_versioning_explicitly_disabled(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view(version_number=3)
        assert _table_id("proj", fv, enable_versioning=False) == "proj_driver_stats"

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view()
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats"

    def test_versioning_enabled_with_current_version_number(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view(version_number=2)
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats_v2"

    def test_version_zero_no_suffix(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view(version_number=0)
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats"

    def test_projection_version_tag_takes_priority(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view(version_number=1, version_tag=3)
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats_v3"

    def test_projection_version_tag_zero_no_suffix(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view(version_tag=0, version_number=3)
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats"

    def test_different_project_names(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view(version_number=1)
        assert _table_id("prod", fv, enable_versioning=True) == "prod_driver_stats_v1"
        assert (
            _table_id("staging", fv, enable_versioning=True)
            == "staging_driver_stats_v1"
        )

    def test_different_feature_view_names(self):
        from feast.infra.online_stores.faiss_online_store import _table_id

        fv = _make_feature_view(name="user_stats", version_number=2)
        assert _table_id("proj", fv, enable_versioning=True) == "proj_user_stats_v2"


class TestFaissVersionedReadSupport:
    """Test that FaissOnlineStore passes _check_versioned_read_support."""

    def test_allowed_with_version_tag(self):
        from feast.infra.online_stores.faiss_online_store import FaissOnlineStore

        store = FaissOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        store._check_versioned_read_support([(fv, ["feature_a"])])

    def test_allowed_without_version_tag(self):
        from feast.infra.online_stores.faiss_online_store import FaissOnlineStore

        store = FaissOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["feature_a"])])


def _make_config(project="test_project", versioning=False):
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


def _make_entity_key(driver_id=1):
    return EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int64_val=driver_id)],
    )


class TestFaissOnlineStoreVersionedReadWrite:
    def _make_store(self, faiss_mock, nlist=10):
        """Create a FaissOnlineStore with a real-enough faiss mock."""
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
        return store, index_mock

    def test_write_and_read_without_versioning(self, _mock_faiss):
        store, _ = self._make_store(_mock_faiss)
        config = _make_config(versioning=False)
        fv = _make_feature_view()

        store.update(config, [], [fv], [], [], partial=False)

        entity_key = _make_entity_key(driver_id=42)
        data = [(entity_key, {"feature_a": ValueProto(double_val=1.5)}, None, None)]
        store.online_write_batch(config, fv, data, None)

        results = store.online_read(config, fv, [entity_key])
        assert len(results) == 1
        _, feature_dict = results[0]
        assert feature_dict is not None
        assert "feature_a" in feature_dict

    def test_write_and_read_with_versioning(self, _mock_faiss):
        store, _ = self._make_store(_mock_faiss)
        config = _make_config(versioning=True)
        fv_v2 = _make_feature_view(version_number=2)

        store.update(config, [], [fv_v2], [], [], partial=False)

        entity_key = _make_entity_key(driver_id=7)
        data = [(entity_key, {"feature_a": ValueProto(double_val=2.0)}, None, None)]
        store.online_write_batch(config, fv_v2, data, None)

        results = store.online_read(config, fv_v2, [entity_key])
        assert len(results) == 1
        _, feature_dict = results[0]
        assert feature_dict is not None

    def test_versioned_namespaces_are_isolated(self, _mock_faiss):
        """Data written under v1 must not be visible when reading under v2."""
        store, _ = self._make_store(_mock_faiss)
        config = _make_config(versioning=True)

        fv_v1 = _make_feature_view(version_number=1)
        fv_v2 = _make_feature_view(version_number=2)

        store.update(config, [], [fv_v1, fv_v2], [], [], partial=False)

        entity_key = _make_entity_key(driver_id=99)
        data = [(entity_key, {"feature_a": ValueProto(double_val=9.9)}, None, None)]
        store.online_write_batch(config, fv_v1, data, None)

        results_v2 = store.online_read(config, fv_v2, [entity_key])
        assert results_v2 == [(None, None)]

        results_v1 = store.online_read(config, fv_v1, [entity_key])
        assert results_v1[0][1] is not None

    def test_missing_index_returns_none(self, _mock_faiss):
        store, _ = self._make_store(_mock_faiss)
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=5)
        entity_key = _make_entity_key(driver_id=1)
        results = store.online_read(config, fv, [entity_key])
        assert results == [(None, None)]

    def test_teardown_removes_versioned_index(self, _mock_faiss):
        store, _ = self._make_store(_mock_faiss)
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=3)

        store.update(config, [], [fv], [], [], partial=False)

        entity_key = _make_entity_key(driver_id=1)
        data = [(entity_key, {"feature_a": ValueProto(double_val=3.0)}, None, None)]
        store.online_write_batch(config, fv, data, None)

        store.teardown(config, [fv], [])

        results = store.online_read(config, fv, [entity_key])
        assert results == [(None, None)]
