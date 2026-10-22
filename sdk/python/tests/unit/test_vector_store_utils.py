import unittest.mock
from datetime import datetime, timezone

import pytest

from feast.errors import FeatureViewNotFoundException
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Array, Float32, String
from feast.vector_store_utils import (
    VectorStoreRegistry,
    build_vector_store_object,
    feature_view_to_vs_id,
)

_DUMMY_SOURCE = FileSource(path="dummy.parquet", timestamp_field="ts")


def _make_fv(name: str, vector_index: bool = True) -> FeatureView:
    schema = [
        Field(name="embedding", dtype=Array(Float32), vector_index=vector_index),
        Field(name="text", dtype=String),
    ]
    fv = FeatureView(name=name, schema=schema, source=_DUMMY_SOURCE)
    fv.created_timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return fv


class TestFeatureViewToVsId:
    def test_deterministic(self):
        id1 = feature_view_to_vs_id("proj", "fv_name")
        id2 = feature_view_to_vs_id("proj", "fv_name")
        assert id1 == id2

    def test_format(self):
        vs_id = feature_view_to_vs_id("proj", "fv_name")
        assert vs_id.startswith("vs_")
        assert len(vs_id) == 27  # "vs_" + 24 hex chars

    def test_different_projects_produce_different_ids(self):
        id_a = feature_view_to_vs_id("project_a", "embeddings")
        id_b = feature_view_to_vs_id("project_b", "embeddings")
        assert id_a != id_b

    def test_different_fv_names_produce_different_ids(self):
        id_a = feature_view_to_vs_id("proj", "fv_one")
        id_b = feature_view_to_vs_id("proj", "fv_two")
        assert id_a != id_b


class TestBuildVectorStoreObject:
    def test_shape(self):
        fv = _make_fv("my_embeddings")
        obj = build_vector_store_object("proj", fv)
        assert obj["object"] == "vector_store"
        assert obj["name"] == "my_embeddings"
        assert obj["status"] == "completed"
        assert obj["id"] == feature_view_to_vs_id("proj", "my_embeddings")
        assert isinstance(obj["created_at"], int)
        assert obj["created_at"] > 0

    def test_no_created_timestamp(self):
        fv = _make_fv("fv")
        fv.created_timestamp = None
        obj = build_vector_store_object("proj", fv)
        assert obj["created_at"] == 0


class TestVectorStoreRegistry:
    def _mock_store(self, feature_views: list, project: str = "test_project"):
        store = unittest.mock.MagicMock()
        store.project = project
        store.list_feature_views.return_value = feature_views
        return store

    def test_resolve_found(self):
        fv = _make_fv("products")
        store = self._mock_store([fv])
        registry = VectorStoreRegistry(store)
        vs_id = feature_view_to_vs_id("test_project", "products")
        assert registry.resolve(vs_id).name == "products"

    def test_resolve_not_found(self):
        store = self._mock_store([_make_fv("products")])
        registry = VectorStoreRegistry(store)
        with pytest.raises(FeatureViewNotFoundException):
            registry.resolve("vs_does_not_exist_000000")

    def test_list_filters_non_vector_fvs(self):
        vec_fv = _make_fv("vec_fv", vector_index=True)
        plain_fv = _make_fv("plain_fv", vector_index=False)
        store = self._mock_store([vec_fv, plain_fv])
        registry = VectorStoreRegistry(store)
        stores = registry.list_vector_stores()
        names = [s["name"] for s in stores]
        assert "vec_fv" in names
        assert "plain_fv" not in names

    def test_get_vector_store_found(self):
        fv = _make_fv("embeddings")
        store = self._mock_store([fv])
        registry = VectorStoreRegistry(store)
        vs_id = feature_view_to_vs_id("test_project", "embeddings")
        obj = registry.get_vector_store(vs_id)
        assert obj is not None
        assert obj["name"] == "embeddings"

    def test_get_vector_store_not_found(self):
        store = self._mock_store([_make_fv("embeddings")])
        registry = VectorStoreRegistry(store)
        assert registry.get_vector_store("vs_bogus_id_000000000000") is None

    def test_refresh_picks_up_new_fvs(self):
        fv1 = _make_fv("fv1")
        store = self._mock_store([fv1])
        registry = VectorStoreRegistry(store)
        assert len(registry.list_vector_stores()) == 1

        fv2 = _make_fv("fv2")
        store.list_feature_views.return_value = [fv1, fv2]
        registry.refresh()
        assert len(registry.list_vector_stores()) == 2
