# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the Lance online store.

Everything runs against local Lance datasets under ``tmp_path``; no server is
involved.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.filter_models import ComparisonFilter, CompoundFilter
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList, Int64List
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Array, Bool, Float32, Float64, Int64, String
from feast.value_type import ValueType

lance = pytest.importorskip("lance")

from feast.infra.online_stores.lance_online_store.lance_online_store import (  # noqa: E402
    LanceFilterTranslator,
    LanceOnlineStore,
    LanceOnlineStoreConfig,
)

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
VECTOR_LENGTH = 4

# --------------------------------------------------------------------- helpers


def _config(tmp_path, **online_store) -> RepoConfig:
    return RepoConfig(
        project="lance_online",
        registry=str(tmp_path / "registry.db"),
        provider="local",
        offline_store={"type": "file"},
        online_store=LanceOnlineStoreConfig(
            path=str(tmp_path / "online"), **online_store
        ),
        entity_key_serialization_version=3,
    )


def _feature_view(schema=None, name="items") -> FeatureView:
    fv = FeatureView(
        name=name,
        entities=[Entity(name="item", join_keys=["item_id"])],
        schema=schema
        if schema is not None
        else [
            Field(name="rating", dtype=Float64),
            Field(name="category", dtype=String),
            Field(name="in_stock", dtype=Bool),
            Field(name="views", dtype=Int64),
            Field(name="tags", dtype=Array(Int64)),
            Field(
                name="embedding",
                dtype=Array(Float32),
                vector_index=True,
                vector_length=VECTOR_LENGTH,
            ),
        ],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
        ttl=timedelta(days=1),
    )
    return fv


def _key(item_id: int) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["item_id"], entity_values=[ValueProto(int64_val=item_id)]
    )


def _values(item_id: int, embedding=None, **overrides):
    values = {
        "rating": ValueProto(double_val=item_id / 10),
        "category": ValueProto(string_val="even" if item_id % 2 == 0 else "odd"),
        "in_stock": ValueProto(bool_val=item_id % 3 == 0),
        "views": ValueProto(int64_val=item_id * 100),
        "tags": ValueProto(int64_list_val=Int64List(val=[item_id, item_id + 1])),
        "embedding": ValueProto(
            float_list_val=FloatList(
                val=embedding
                if embedding is not None
                else [float(item_id)] + [0.0] * (VECTOR_LENGTH - 1)
            )
        ),
    }
    values.update(overrides)
    return values


def _write(store, config, fv, rows):
    store.online_write_batch(config, fv, rows, progress=None)


def _provisioned(tmp_path, fv=None, **online_store):
    config = _config(tmp_path, **online_store)
    fv = fv or _feature_view()
    store = LanceOnlineStore()
    store.update(config, [], [fv], [], [], partial=False)
    return store, config, fv


# ------------------------------------------------------------ reads and writes


def test_registered_as_lance():
    config = _config_from_dict()
    assert isinstance(config.online_store, LanceOnlineStoreConfig)


def _config_from_dict():
    return RepoConfig.model_validate(
        {
            "project": "p",
            "registry": "registry.db",
            "provider": "local",
            "online_store": {"type": "lance", "path": "online"},
            "entity_key_serialization_version": 3,
        }
    )


def test_relative_path_resolves_against_the_repo(tmp_path):
    config = _config_from_dict()
    config.repo_path = tmp_path
    uri = LanceOnlineStore()._dataset_uri(config, _feature_view())
    assert uri == f"{tmp_path}/online/p_items.lance"


def test_object_storage_path_is_used_as_given(tmp_path):
    config = _config_from_dict()
    config.repo_path = tmp_path
    config.online_store.path = "s3://bucket/online/"
    uri = LanceOnlineStore()._dataset_uri(config, _feature_view())
    assert uri == "s3://bucket/online/p_items.lance"


def test_read_before_any_write_is_missing(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    assert store.online_read(config, fv, [_key(1)]) == [(None, None)]


def test_read_without_provisioning_is_missing(tmp_path):
    config = _config(tmp_path)
    store = LanceOnlineStore()
    assert store.online_read(config, _feature_view(), [_key(1)]) == [(None, None)]


def test_write_then_read_round_trips_every_type(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])

    [(ts, values)] = store.online_read(config, fv, [_key(1)])
    assert ts == T0.replace(tzinfo=None)
    assert values == _values(1)


def test_a_write_creates_an_unprovisioned_dataset(tmp_path):
    config = _config(tmp_path)
    store, fv = LanceOnlineStore(), _feature_view()
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])
    [(_, values)] = store.online_read(config, fv, [_key(1)])
    assert values["views"].int64_val == 100


def test_reads_preserve_request_order_and_mark_missing_keys(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(i), _values(i), T0, None) for i in (1, 2, 3)])

    result = store.online_read(config, fv, [_key(3), _key(99), _key(1), _key(3)])
    assert [r[1]["views"].int64_val if r[1] else None for r in result] == [
        300,
        None,
        100,
        300,
    ]


def test_requested_features_limit_what_is_returned(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])
    [(_, values)] = store.online_read(
        config, fv, [_key(1)], requested_features=["category", "embedding"]
    )
    assert set(values) == {"category", "embedding"}
    assert list(values["embedding"].float_list_val.val) == [1.0, 0.0, 0.0, 0.0]


def test_an_unset_value_reads_back_unset(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1, category=ValueProto()), T0, None)])
    [(_, values)] = store.online_read(config, fv, [_key(1)])
    assert values["category"].WhichOneof("val") is None


def test_a_newer_write_replaces_the_row(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])
    _write(
        store,
        config,
        fv,
        [(_key(1), _values(1, views=ValueProto(int64_val=7)), T0 + timedelta(1), None)],
    )
    [(ts, values)] = store.online_read(config, fv, [_key(1)])
    assert values["views"].int64_val == 7
    assert ts == (T0 + timedelta(1)).replace(tzinfo=None)
    assert lance.dataset(store._dataset_uri(config, fv)).count_rows() == 1


def test_an_older_write_does_not_roll_the_row_back(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])
    _write(
        store,
        config,
        fv,
        [(_key(1), _values(1, views=ValueProto(int64_val=7)), T0 - timedelta(1), None)],
    )
    [(ts, values)] = store.online_read(config, fv, [_key(1)])
    assert values["views"].int64_val == 100
    assert ts == T0.replace(tzinfo=None)


def test_duplicate_keys_in_one_batch_keep_the_newest(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(
        store,
        config,
        fv,
        [
            (
                _key(1),
                _values(1, views=ValueProto(int64_val=2)),
                T0 + timedelta(2),
                None,
            ),
            (
                _key(1),
                _values(1, views=ValueProto(int64_val=1)),
                T0 + timedelta(1),
                None,
            ),
        ],
    )
    [(_, values)] = store.online_read(config, fv, [_key(1)])
    assert values["views"].int64_val == 2
    assert lance.dataset(store._dataset_uri(config, fv)).count_rows() == 1


def test_naive_timestamps_are_treated_as_utc(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), datetime(2026, 1, 1), None)])
    [(ts, _)] = store.online_read(config, fv, [_key(1)])
    assert ts == datetime(2026, 1, 1)


# ---------------------------------------------------------------- provisioning


def test_update_indexes_the_entity_key(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    indices = lance.dataset(store._dataset_uri(config, fv)).describe_indices()
    assert [i.field_names for i in indices] == [["__entity_key"]]


def test_an_added_feature_keeps_existing_values(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])

    wider = _feature_view(schema=[*fv.schema, Field(name="score", dtype=Float32)])
    store.update(config, [], [wider], [], [], partial=False)

    [(_, values)] = store.online_read(config, wider, [_key(1)])
    assert values["views"].int64_val == 100
    assert values["score"].WhichOneof("val") is None


def test_a_removed_feature_is_dropped(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])

    narrower = _feature_view(schema=[f for f in fv.schema if f.name != "category"])
    store.update(config, [], [narrower], [], [], partial=False)

    dataset = lance.dataset(store._dataset_uri(config, narrower))
    assert "category" not in dataset.schema.names
    [(_, values)] = store.online_read(config, narrower, [_key(1)])
    assert values["views"].int64_val == 100


def test_a_changed_type_recreates_the_dataset_empty(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])

    retyped = _feature_view(
        schema=[
            Field(name="views", dtype=String) if f.name == "views" else f
            for f in fv.schema
        ]
    )
    store.update(config, [], [retyped], [], [], partial=False)
    assert store.online_read(config, retyped, [_key(1)]) == [(None, None)]


def test_delete_and_teardown_drop_the_dataset(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    other = _feature_view(name="others")
    store.update(config, [], [other], [], [], partial=False)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])

    store.update(config, [fv], [other], [], [], partial=False)
    assert store._open(config, fv) is None
    assert store._open(config, other) is not None

    store.teardown(config, [other], [])
    assert store._open(config, other) is None
    # Dropping an absent dataset is not an error.
    store.teardown(config, [fv, other], [])


# --------------------------------------------------------------------- search


def _searchable(tmp_path, **online_store):
    store, config, fv = _provisioned(tmp_path, vector_enabled=True, **online_store)
    rows = [
        (_key(i), _values(i, embedding=[1.0, float(i), 0.0, 0.0]), T0, None)
        for i in range(6)
    ]
    _write(store, config, fv, rows)
    return store, config, fv


def _ids(results):
    return [entity_key.entity_values[0].int64_val for _, entity_key, _ in results]


def test_vector_search_returns_the_nearest_rows(tmp_path):
    store, config, fv = _searchable(tmp_path)
    results = store.retrieve_online_documents_v2(
        config,
        fv,
        ["views"],
        embedding=[1.0, 2.1, 0.0, 0.0],
        top_k=3,
        distance_metric="l2",
    )
    assert _ids(results) == [2, 3, 1]
    _, _, features = results[0]
    assert features["views"].int64_val == 200
    assert features["distance"].float_val == pytest.approx(0.01, abs=1e-4)


def test_vector_search_prefilters_on_metadata(tmp_path):
    store, config, fv = _searchable(tmp_path)
    results = store.retrieve_online_documents_v2(
        config,
        fv,
        ["category"],
        embedding=[1.0, 2.1, 0.0, 0.0],
        top_k=2,
        distance_metric="l2",
        filters=CompoundFilter(
            type="and",
            filters=[
                ComparisonFilter(type="eq", key="category", value="odd"),
                ComparisonFilter(type="gte", key="views", value=300),
            ],
        ),
    )
    # A postfilter would keep only 3 of the unfiltered top 2 (2 and 3).
    assert _ids(results) == [3, 5]


def test_vector_search_needs_vector_enabled(tmp_path):
    store, config, fv = _provisioned(tmp_path)
    _write(store, config, fv, [(_key(1), _values(1), T0, None)])
    with pytest.raises(ValueError, match="vector_enabled"):
        store.retrieve_online_documents_v2(
            config, fv, ["views"], embedding=[0.0] * VECTOR_LENGTH, top_k=1
        )


def test_vector_search_rejects_an_unknown_metric(tmp_path):
    store, config, fv = _searchable(tmp_path)
    with pytest.raises(ValueError, match="Unsupported distance metric"):
        store.retrieve_online_documents_v2(
            config,
            fv,
            [],
            embedding=[0.0] * VECTOR_LENGTH,
            top_k=1,
            distance_metric="manhattan",
        )


def test_hybrid_search_is_refused(tmp_path):
    store, config, fv = _searchable(tmp_path, text_search_enabled=True)
    with pytest.raises(ValueError, match="hybrid"):
        store.retrieve_online_documents_v2(
            config,
            fv,
            [],
            embedding=[0.0] * VECTOR_LENGTH,
            top_k=1,
            query_string="even",
        )


def test_text_search_ranks_matching_rows(tmp_path):
    schema = [
        Field(name="title", dtype=String),
        Field(name="body", dtype=String),
        Field(name="views", dtype=Int64),
    ]
    fv = _feature_view(schema=schema, name="docs")
    store, config, fv = _provisioned(tmp_path, fv=fv, text_search_enabled=True)
    docs = {
        1: ("feast feature store", "serving features online"),
        2: ("lance format", "columnar vectors and a feature store"),
        3: ("unrelated", "nothing here"),
    }
    _write(
        store,
        config,
        fv,
        [
            (
                _key(i),
                {
                    "title": ValueProto(string_val=title),
                    "body": ValueProto(string_val=body),
                    "views": ValueProto(int64_val=i),
                },
                T0,
                None,
            )
            for i, (title, body) in docs.items()
        ],
    )
    results = store.retrieve_online_documents_v2(
        config, fv, ["title"], embedding=None, top_k=5, query_string="feature"
    )
    assert sorted(_ids(results)) == [1, 2]
    _, _, features = results[0]
    assert features["text_rank"].float_val > 0

    filtered = store.retrieve_online_documents_v2(
        config,
        fv,
        ["title"],
        embedding=None,
        top_k=5,
        query_string="feature",
        filters=ComparisonFilter(type="eq", key="views", value=2),
    )
    assert _ids(filtered) == [2]


def test_text_search_needs_text_search_enabled(tmp_path):
    store, config, fv = _searchable(tmp_path)
    with pytest.raises(ValueError, match="text_search_enabled"):
        store.retrieve_online_documents_v2(
            config, fv, [], embedding=None, top_k=1, query_string="even"
        )


def test_search_before_any_write_is_empty(tmp_path):
    config = _config(tmp_path, vector_enabled=True)
    assert (
        LanceOnlineStore().retrieve_online_documents_v2(
            config, _feature_view(), [], embedding=[0.0] * VECTOR_LENGTH, top_k=1
        )
        == []
    )


# ---------------------------------------------------------------- filters


@pytest.mark.parametrize(
    "filters, expected",
    [
        (
            ComparisonFilter(type="eq", key="category", value="it's"),
            "`category` = 'it''s'",
        ),
        (ComparisonFilter(type="ne", key="views", value=3), "`views` != 3"),
        (ComparisonFilter(type="lte", key="rating", value=0.5), "`rating` <= 0.5"),
        (ComparisonFilter(type="eq", key="in_stock", value=True), "`in_stock` = TRUE"),
        (ComparisonFilter(type="eq", key="category", value=None), "`category` IS NULL"),
        (ComparisonFilter(type="in", key="views", value=[1, 2]), "`views` IN (1, 2)"),
        (
            ComparisonFilter(type="nin", key="category", value=["a"]),
            "`category` NOT IN ('a')",
        ),
        (ComparisonFilter(type="in", key="views", value=[]), "FALSE"),
        (
            CompoundFilter(
                type="or",
                filters=[
                    ComparisonFilter(type="gt", key="views", value=1),
                    ComparisonFilter(type="lt", key="views", value=0),
                ],
            ),
            "(`views` > 1 OR `views` < 0)",
        ),
        (None, None),
    ],
)
def test_filter_translation(filters, expected):
    translator = LanceFilterTranslator(["category", "views", "rating", "in_stock"])
    assert translator.translate(filters) == expected


def test_filtering_on_a_non_native_feature_is_an_error():
    translator = LanceFilterTranslator(["views"])
    with pytest.raises(ValueError, match="Cannot filter on 'tags'"):
        translator.translate(ComparisonFilter(type="eq", key="tags", value=1))


# ---------------------------------------------------------- feature store e2e


def test_feature_store_end_to_end(tmp_path):
    config = _config(tmp_path, vector_enabled=True)
    fs = FeatureStore(config=config)
    item = Entity(name="item", join_keys=["item_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="items",
        entities=[item],
        schema=[
            Field(name="item_id", dtype=Int64),
            Field(name="category", dtype=String),
            Field(
                name="embedding",
                dtype=Array(Float32),
                vector_index=True,
                vector_length=VECTOR_LENGTH,
                vector_search_metric="L2",
            ),
        ],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
        ttl=timedelta(days=1),
    )
    fs.apply([item, fv])

    now = datetime.now(timezone.utc)
    fs.write_to_online_store(
        "items",
        pd.DataFrame(
            {
                "item_id": [1, 2, 3],
                "category": ["a", "b", "a"],
                "embedding": [[float(i), 0.0, 0.0, 0.0] for i in (1, 2, 3)],
                "event_timestamp": [now] * 3,
            }
        ),
    )

    online = fs.get_online_features(
        features=["items:category"], entity_rows=[{"item_id": 2}, {"item_id": 9}]
    ).to_dict()
    assert online["category"] == ["b", None]

    documents = fs.retrieve_online_documents_v2(
        features=["items:category", "items:embedding"],
        query=[2.9, 0.0, 0.0, 0.0],
        top_k=2,
        filters=ComparisonFilter(type="eq", key="category", value="a"),
    ).to_dict()
    assert documents["item_id"] == [3, 1]
    assert documents["category"] == ["a", "a"]
    assert documents["distance"][0] == pytest.approx(0.01, abs=1e-4)
