"""Unit tests for Elasticsearch online store feature view versioning."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
    ElasticSearchOnlineStore,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32
from feast.value_type import ValueType

MODULE = "feast.infra.online_stores.elasticsearch_online_store.elasticsearch"


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
        schema=[Field(name="trips_today", dtype=Float32)],
    )
    if version_number is not None:
        fv.current_version_number = version_number
    if version_tag is not None:
        fv.projection.version_tag = version_tag
    return fv


def _make_config(project="test_project", versioning=False):
    config = MagicMock()
    config.project = project
    config.entity_key_serialization_version = 2
    config.registry.enable_online_feature_view_versioning = versioning
    config.online_store.write_batch_size = 100
    config.online_store.similarity = "cosine"
    config.online_store.enable_openai_compatible_store = False
    return config


def _entity_key():
    ek = EntityKeyProto()
    ek.join_keys.append("driver_id")
    value = ValueProto()
    value.int64_val = 1001
    ek.entity_values.append(value)
    return ek


def _write_one(store, config, fv):
    value = ValueProto()
    value.float_val = 1.0
    store.online_write_batch(
        config,
        fv,
        [(_entity_key(), {"trips_today": value}, datetime(2026, 1, 1), None)],
        None,
    )


class TestVersionedIndexName:
    """_versioned_index_name names the index the store should touch."""

    def test_no_versioning(self):
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            _versioned_index_name,
        )

        fv = _make_feature_view()
        config = _make_config(versioning=False)
        assert _versioned_index_name(fv, config) == "driver_stats"

    def test_versioning_disabled_ignores_version(self):
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            _versioned_index_name,
        )

        fv = _make_feature_view(version_number=3)
        config = _make_config(versioning=False)
        assert _versioned_index_name(fv, config) == "driver_stats"

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            _versioned_index_name,
        )

        fv = _make_feature_view()
        config = _make_config(versioning=True)
        assert _versioned_index_name(fv, config) == "driver_stats"

    def test_versioning_enabled_with_current_version_number(self):
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            _versioned_index_name,
        )

        fv = _make_feature_view(version_number=2)
        config = _make_config(versioning=True)
        assert _versioned_index_name(fv, config) == "driver_stats_v2"

    def test_version_zero_no_suffix(self):
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            _versioned_index_name,
        )

        fv = _make_feature_view(version_number=0)
        config = _make_config(versioning=True)
        assert _versioned_index_name(fv, config) == "driver_stats"

    def test_projection_version_tag_takes_priority(self):
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            _versioned_index_name,
        )

        fv = _make_feature_view(version_number=1, version_tag=3)
        config = _make_config(versioning=True)
        assert _versioned_index_name(fv, config) == "driver_stats_v3"

    def test_projection_version_tag_zero_no_suffix(self):
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            _versioned_index_name,
        )

        fv = _make_feature_view(version_tag=0, version_number=3)
        config = _make_config(versioning=True)
        assert _versioned_index_name(fv, config) == "driver_stats"

    def test_two_versions_do_not_share_an_index(self):
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            _versioned_index_name,
        )

        config = _make_config(versioning=True)
        v1 = _versioned_index_name(_make_feature_view(version_number=1), config)
        v2 = _versioned_index_name(_make_feature_view(version_number=2), config)
        assert v1 != v2


class TestStorePathsUseTheVersionedIndex:
    """Every path that names an index must name the versioned one."""

    def test_write_targets_the_versioned_index(self):
        store = ElasticSearchOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        with (
            patch.object(ElasticSearchOnlineStore, "_get_client", MagicMock()),
            patch.object(
                ElasticSearchOnlineStore, "_index_has_value_num", return_value=False
            ),
            patch(f"{MODULE}.helpers") as es_helpers,
        ):
            _write_one(store, config, fv)
        actions = list(es_helpers.bulk.call_args[0][1])
        assert actions
        assert {action["_index"] for action in actions} == {"driver_stats_v2"}

    def test_write_is_unchanged_when_versioning_is_off(self):
        store = ElasticSearchOnlineStore()
        config = _make_config(versioning=False)
        fv = _make_feature_view(version_number=2)
        with (
            patch.object(ElasticSearchOnlineStore, "_get_client", MagicMock()),
            patch.object(
                ElasticSearchOnlineStore, "_index_has_value_num", return_value=False
            ),
            patch(f"{MODULE}.helpers") as es_helpers,
        ):
            _write_one(store, config, fv)
        actions = list(es_helpers.bulk.call_args[0][1])
        assert {action["_index"] for action in actions} == {"driver_stats"}

    def test_write_checks_the_mapping_of_the_versioned_index(self):
        store = ElasticSearchOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        with (
            patch.object(ElasticSearchOnlineStore, "_get_client", MagicMock()),
            patch.object(
                ElasticSearchOnlineStore, "_index_has_value_num", return_value=False
            ) as has_value_num,
            patch(f"{MODULE}.helpers"),
        ):
            _write_one(store, config, fv)
        assert has_value_num.call_args[0][1] == "driver_stats_v2"

    def test_read_searches_the_versioned_index(self):
        store = ElasticSearchOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        client = MagicMock()
        client.search.return_value = {"hits": {"hits": []}}
        with patch.object(ElasticSearchOnlineStore, "_get_client", return_value=client):
            store.online_read(config, fv, [_entity_key()], ["trips_today"])
        assert client.search.call_args.kwargs["index"] == "driver_stats_v2"

    def test_create_index_creates_the_versioned_index(self):
        store = ElasticSearchOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        client = MagicMock()
        with patch.object(ElasticSearchOnlineStore, "_get_client", return_value=client):
            store.create_index(config, fv)
        assert client.indices.create.call_args.kwargs["index"] == "driver_stats_v2"

    def test_update_deletes_and_creates_versioned_indices(self):
        store = ElasticSearchOnlineStore()
        config = _make_config(versioning=True)
        keep = _make_feature_view(name="keep_me", version_number=2)
        drop = _make_feature_view(name="drop_me", version_number=3)
        client = MagicMock()
        with patch.object(ElasticSearchOnlineStore, "_get_client", return_value=client):
            store.update(config, [drop], [keep], [], [], partial=False)
        assert client.delete_by_query.call_args.kwargs["index"] == "drop_me_v3"
        assert client.indices.create.call_args.kwargs["index"] == "keep_me_v2"

    def test_teardown_deletes_the_versioned_index(self):
        store = ElasticSearchOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        client = MagicMock()
        with patch.object(ElasticSearchOnlineStore, "_get_client", return_value=client):
            store.teardown(config, [fv], [])
        assert client.indices.delete.call_args.kwargs["index"] == "driver_stats_v2"


class TestElasticsearchVersionedReadSupport:
    """The store must also be on the base class's supported list: without that,
    every versioned read is refused however the indices are named."""

    def test_store_declares_versioned_read_support(self):
        assert ElasticSearchOnlineStore()._is_versioned_read_supported() is True

    def test_versioned_ref_is_not_refused(self):
        store = ElasticSearchOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        store._check_versioned_read_support([(fv, ["trips_today"])])
