"""Unit tests for MongoDB online store feature view versioning."""

# ruff: noqa: E402

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pymongo")

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.infra.online_stores.mongodb_online_store.mongodb import (
    MongoDBOnlineStore,
    _versioned_fv_name,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Array, Float32, Int64
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
        schema=[Field(name="trips_today", dtype=Int64)],
        source=FileSource(path="fake.parquet", timestamp_field="event_timestamp"),
    )
    if version_number is not None:
        fv.current_version_number = version_number
    if version_tag is not None:
        fv.projection.version_tag = version_tag
    return fv


def _make_config(project="test_project", versioning=False, vector_enabled=False):
    """Build a real RepoConfig, so the registry flag is exercised rather than mocked."""
    return RepoConfig(
        project=project,
        provider="local",
        online_store={
            "type": "mongodb",
            "connection_string": "mongodb://localhost:27017",
            "vector_enabled": vector_enabled,
        },
        registry={
            "path": "memory://",
            "enable_online_feature_view_versioning": versioning,
        },
        entity_key_serialization_version=3,
    )


def _entity_key(driver_id=1):
    key = EntityKeyProto()
    key.join_keys.append("driver_id")
    key.entity_values.append(ValueProto(int64_val=driver_id))
    return key


class TestVersionedFvName:
    """_versioned_fv_name produces the document namespace the store writes under."""

    def test_no_versioning(self):
        fv = _make_feature_view()
        assert _versioned_fv_name(fv, _make_config(versioning=False)) == "driver_stats"

    def test_versioning_disabled_ignores_version(self):
        fv = _make_feature_view(version_number=3)
        assert _versioned_fv_name(fv, _make_config(versioning=False)) == "driver_stats"

    def test_versioning_enabled_no_version_set(self):
        fv = _make_feature_view()
        assert _versioned_fv_name(fv, _make_config(versioning=True)) == "driver_stats"

    def test_versioning_enabled_with_current_version_number(self):
        fv = _make_feature_view(version_number=2)
        assert (
            _versioned_fv_name(fv, _make_config(versioning=True)) == "driver_stats_v2"
        )

    def test_version_zero_no_suffix(self):
        fv = _make_feature_view(version_number=0)
        assert _versioned_fv_name(fv, _make_config(versioning=True)) == "driver_stats"

    def test_projection_version_tag_takes_priority(self):
        fv = _make_feature_view(version_number=1, version_tag=3)
        assert (
            _versioned_fv_name(fv, _make_config(versioning=True)) == "driver_stats_v3"
        )

    def test_projection_version_tag_zero_no_suffix(self):
        fv = _make_feature_view(version_tag=0, version_number=3)
        assert _versioned_fv_name(fv, _make_config(versioning=True)) == "driver_stats"


class TestWritePathVersioning:
    """Writes land under the versioned namespace inside the shared collection."""

    @staticmethod
    def _set_doc(fv, config):
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        data = [(_entity_key(), {"trips_today": ValueProto(int64_val=7)}, ts, ts)]
        ops = MongoDBOnlineStore._build_write_ops(config, fv, data)
        return ops[0]._doc["$set"]

    def test_unversioned_write_uses_plain_name(self):
        doc = self._set_doc(_make_feature_view(), _make_config(versioning=False))
        assert "features.driver_stats.trips_today" in doc
        assert "event_timestamps.driver_stats" in doc

    def test_versioned_write_uses_suffixed_name(self):
        doc = self._set_doc(
            _make_feature_view(version_number=2), _make_config(versioning=True)
        )
        assert "features.driver_stats_v2.trips_today" in doc
        assert "event_timestamps.driver_stats_v2" in doc
        assert "features.driver_stats.trips_today" not in doc

    def test_two_versions_write_to_different_namespaces(self):
        config = _make_config(versioning=True)
        doc_v1 = self._set_doc(_make_feature_view(version_number=1), config)
        doc_v2 = self._set_doc(_make_feature_view(version_number=2), config)
        assert set(doc_v1) != set(doc_v2)

    def test_versioning_disabled_ignores_version_on_write(self):
        doc = self._set_doc(
            _make_feature_view(version_number=2), _make_config(versioning=False)
        )
        assert "features.driver_stats.trips_today" in doc


class TestReadPathVersioning:
    """Reads project the versioned namespace, so a versioned read cannot see v1 data."""

    @staticmethod
    def _projection(fv, config, requested=None):
        store = MongoDBOnlineStore()
        collection = MagicMock()
        collection.find.return_value = []
        store._get_collection = MagicMock(return_value=collection)
        store.online_read(
            config=config,
            table=fv,
            entity_keys=[_entity_key()],
            requested_features=requested,
        )
        return collection.find.call_args.kwargs["projection"]

    def test_unversioned_read_projects_plain_name(self):
        projection = self._projection(_make_feature_view(), _make_config())
        assert "features.driver_stats" in projection
        assert "event_timestamps.driver_stats" in projection

    def test_versioned_read_projects_suffixed_name(self):
        projection = self._projection(
            _make_feature_view(version_number=2), _make_config(versioning=True)
        )
        assert "features.driver_stats_v2" in projection
        assert "event_timestamps.driver_stats_v2" in projection
        assert "features.driver_stats" not in projection

    def test_versioned_read_of_requested_features(self):
        projection = self._projection(
            _make_feature_view(version_number=2),
            _make_config(versioning=True),
            requested=["trips_today"],
        )
        assert "features.driver_stats_v2.trips_today" in projection


class TestConvertRawDocsVersioning:
    """The converter reads back out of the namespace it was told about."""

    def test_reads_versioned_namespace(self):
        fv = _make_feature_view(version_number=2)
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        docs = {
            b"e1": {
                "features": {
                    "driver_stats": {"trips_today": 1},
                    "driver_stats_v2": {"trips_today": 99},
                },
                "event_timestamps": {"driver_stats": ts, "driver_stats_v2": ts},
            }
        }

        results = MongoDBOnlineStore._convert_raw_docs_to_proto(
            [b"e1"], docs, fv, "driver_stats_v2"
        )

        assert results[0][1]["trips_today"].int64_val == 99

    def test_defaults_to_table_name(self):
        """Omitting fv_name keeps the pre-versioning behaviour for existing callers."""
        fv = _make_feature_view()
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        docs = {
            b"e1": {
                "features": {"driver_stats": {"trips_today": 5}},
                "event_timestamps": {"driver_stats": ts},
            }
        }

        results = MongoDBOnlineStore._convert_raw_docs_to_proto([b"e1"], docs, fv)

        assert results[0][1]["trips_today"].int64_val == 5

    def test_missing_version_namespace_reads_as_absent(self):
        """A versioned read of data written before that version finds nothing."""
        fv = _make_feature_view(version_number=2)
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        docs = {
            b"e1": {
                "features": {"driver_stats": {"trips_today": 1}},
                "event_timestamps": {"driver_stats": ts},
            }
        }

        results = MongoDBOnlineStore._convert_raw_docs_to_proto(
            [b"e1"], docs, fv, "driver_stats_v2"
        )

        assert results[0] == (None, None)


class TestUpdateVersioning:
    """Deleting a versioned feature view unsets only that version namespace."""

    @staticmethod
    def _unset(fv, config):
        store = MongoDBOnlineStore()
        collection = MagicMock()
        store._get_collection = MagicMock(return_value=collection)
        store.update(
            config=config,
            tables_to_delete=[fv],
            tables_to_keep=[],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )
        return collection.update_many.call_args.args[1]["$unset"]

    def test_versioned_delete_unsets_versioned_namespace(self):
        unset = self._unset(
            _make_feature_view(version_number=2), _make_config(versioning=True)
        )
        assert "features.driver_stats_v2" in unset
        assert "event_timestamps.driver_stats_v2" in unset
        assert "features.driver_stats" not in unset

    def test_unversioned_delete_unsets_plain_namespace(self):
        unset = self._unset(_make_feature_view(), _make_config(versioning=False))
        assert "features.driver_stats" in unset


class TestVectorIndexVersioning:
    """Each version gets its own Atlas vector search index and path."""

    @staticmethod
    def _vector_fv(version_number=None):
        entity = Entity(
            name="doc_id", join_keys=["doc_id"], value_type=ValueType.STRING
        )
        fv = FeatureView(
            name="docs",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=4,
                )
            ],
            source=FileSource(path="fake.parquet", timestamp_field="event_timestamp"),
        )
        if version_number is not None:
            fv.current_version_number = version_number
        return fv

    @staticmethod
    def _created_index(fv, enable_versioning):
        store = MongoDBOnlineStore()
        collection = MagicMock()
        collection.name = "test_project_latest"
        collection.database.list_collection_names.return_value = ["test_project_latest"]

        def _list_search_indexes(*args, **kwargs):
            # No index exists yet for the "already exists?" check, but the
            # readiness poll that follows creation must find it READY or it
            # would spin until vector_index_wait_timeout.
            if kwargs.get("name"):
                return [{"name": kwargs["name"], "status": "READY"}]
            return []

        collection.list_search_indexes.side_effect = _list_search_indexes
        online_config = _make_config(vector_enabled=True).online_store

        store._ensure_vector_indexes(collection, [fv], online_config, enable_versioning)

        return collection.create_search_index.call_args.kwargs["model"]

    def test_unversioned_index_name_and_path(self):
        model = self._created_index(self._vector_fv(), False)
        assert model.document["name"] == "docs__embedding__vs_index"
        assert (
            model.document["definition"]["fields"][0]["path"]
            == "features.docs.embedding"
        )

    def test_versioned_index_name_and_path(self):
        model = self._created_index(self._vector_fv(version_number=2), True)
        assert model.document["name"] == "docs_v2__embedding__vs_index"
        assert (
            model.document["definition"]["fields"][0]["path"]
            == "features.docs_v2.embedding"
        )

    def test_drop_targets_versioned_index(self):
        store = MongoDBOnlineStore()
        collection = MagicMock()
        collection.list_search_indexes.return_value = [
            {"name": "docs_v2__embedding__vs_index"}
        ]

        store._drop_vector_indexes_for_tables(
            collection, [self._vector_fv(version_number=2)], True
        )

        collection.drop_search_index.assert_called_once_with(
            "docs_v2__embedding__vs_index"
        )


class TestMongoDBVersionedReadSupport:
    """MongoDBOnlineStore is registered as supporting versioned reads."""

    def test_allowed_with_version_tag(self):
        store = MongoDBOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        # Should not raise
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        store = MongoDBOnlineStore()
        store._check_versioned_read_support([(_make_feature_view(), ["trips_today"])])
