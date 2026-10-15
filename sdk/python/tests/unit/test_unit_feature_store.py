from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from feast import utils
from feast.data_source import RequestSource
from feast.feature_store import FeatureStore
from feast.field import Field
from feast.infra.registry.registry import Registry
from feast.infra.registry.sql import SqlRegistry
from feast.protos.feast.types.Value_pb2 import Value
from feast.repo_config import RepoConfig
from feast.types import Int64


@dataclass
class MockFeatureViewProjection:
    join_key_map: Dict[str, str]


@dataclass
class MockFeatureView:
    name: str
    entities: List[str]
    projection: MockFeatureViewProjection


@pytest.mark.parametrize("registry_type", ["file", "sql"])
@pytest.mark.parametrize(
    "initial_fields,updated_fields",
    [
        (["amount"], ["amount", "multiplier"]),
        (["amount", "multiplier"], ["amount"]),
        ([], ["amount"]),
        (["amount"], []),
    ],
    ids=["add-field", "remove-field", "add-to-empty", "remove-all"],
)
def test_apply_request_source_schema_changes(
    tmp_path: Path,
    registry_type: str,
    initial_fields: List[str],
    updated_fields: List[str],
) -> None:
    """Request schema changes must survive applying and reopening the registry."""
    registry_path = tmp_path / "registry.db"
    config = RepoConfig(
        project="test_project",
        provider="local",
        registry={
            "registry_type": registry_type,
            "path": (
                str(registry_path)
                if registry_type == "file"
                else f"sqlite:///{registry_path}"
            ),
        },
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
        offline_store={"type": "file"},
    )
    store = FeatureStore(config=config)
    reloaded_store = None
    try:
        store.apply(
            RequestSource(
                name="request",
                schema=[Field(name=name, dtype=Int64) for name in initial_fields],
            )
        )
        updated_schema = [Field(name=name, dtype=Int64) for name in updated_fields]
        store.apply(RequestSource(name="request", schema=updated_schema))

        reloaded_store = FeatureStore(config=config)
        persisted_source = reloaded_store.get_data_source("request")
        assert isinstance(persisted_source, RequestSource)
        assert persisted_source.schema == updated_schema
    finally:
        for current_store in (reloaded_store, store):
            if current_store is not None:
                registry = current_store.registry
                assert isinstance(registry, (Registry, SqlRegistry))
                registry.teardown()


def test_get_unique_entities_success():
    entity_values = {
        "entity_1": [Value(int64_val=1), Value(int64_val=2), Value(int64_val=1)],
        "entity_2": [
            Value(string_val="1"),
            Value(string_val="2"),
            Value(string_val="1"),
        ],
        "entity_3": [Value(int64_val=8), Value(int64_val=9), Value(int64_val=10)],
    }

    entity_name_to_join_key_map = {"entity_1": "entity_1", "entity_2": "entity_2"}

    fv = MockFeatureView(
        name="fv_1",
        entities=["entity_1", "entity_2"],
        projection=MockFeatureViewProjection(join_key_map={}),
    )

    unique_entities, indexes, output_len = utils._get_unique_entities(
        table=fv,
        join_key_values=entity_values,
        entity_name_to_join_key_map=entity_name_to_join_key_map,
    )
    expected_entities = (
        {"entity_1": Value(int64_val=1), "entity_2": Value(string_val="1")},
        {"entity_1": Value(int64_val=2), "entity_2": Value(string_val="2")},
    )
    expected_indexes = ([0, 2], [1])

    assert unique_entities == expected_entities
    assert indexes == expected_indexes
    assert output_len == 3


def test_get_unique_entities_missing_join_key_success():
    """
    Tests that _get_unique_entities raises a KeyError when a required join key is missing.
    """
    # Here, we omit the required key for "entity_1"
    entity_values = {
        "entity_2": [
            Value(string_val="1"),
            Value(string_val="2"),
            Value(string_val="1"),
        ],
    }

    entity_name_to_join_key_map = {"entity_1": "entity_1", "entity_2": "entity_2"}

    fv = MockFeatureView(
        name="fv_1",
        entities=["entity_1", "entity_2"],
        projection=MockFeatureViewProjection(join_key_map={}),
    )

    unique_entities, indexes, output_len = utils._get_unique_entities(
        table=fv,
        join_key_values=entity_values,
        entity_name_to_join_key_map=entity_name_to_join_key_map,
    )
    expected_entities = (
        {"entity_2": Value(string_val="1")},
        {"entity_2": Value(string_val="2")},
    )
    expected_indexes = ([0, 2], [1])

    assert unique_entities == expected_entities
    assert indexes == expected_indexes
    assert output_len == 3
    # We're not say anything about the entity_1 missing from the unique_entities list
    assert "entity_1" not in [entity.keys() for entity in unique_entities]


def test_get_unique_entities_missing_all_join_keys_error():
    """
    Tests that _get_unique_entities raises a KeyError when all required join keys are missing.
    """
    entity_values_not_in_feature_view = {
        "entity_3": [Value(string_val="3")],
    }
    entity_name_to_join_key_map = {
        "entity_1": "entity_1",
        "entity_2": "entity_2",
        "entity_3": "entity_3",
    }

    fv = MockFeatureView(
        name="fv_1",
        entities=["entity_1", "entity_2"],
        projection=MockFeatureViewProjection(join_key_map={}),
    )

    with pytest.raises(KeyError) as excinfo:
        utils._get_unique_entities(
            table=fv,
            join_key_values=entity_values_not_in_feature_view,
            entity_name_to_join_key_map=entity_name_to_join_key_map,
        )

    error_message = str(excinfo.value)
    assert (
        "Missing join key values for keys: ['entity_1', 'entity_2', 'entity_3']"
        in error_message
    )
    assert (
        "No values provided for keys: ['entity_1', 'entity_2', 'entity_3']"
        in error_message
    )
    assert "Provided join_key_values: ['entity_3']" in error_message


def test_write_to_offline_store_resolves_feature_view_with_single_lookup():
    """``write_to_offline_store`` must resolve the feature view with a single
    registry lookup via ``get_any_feature_view`` rather than the legacy
    per-type try/except chain.

    The old chain called ``get_stream_feature_view`` first, so the common plain
    ``FeatureView`` always paid a guaranteed-miss lookup before the one that
    could succeed. On a ``RemoteRegistry`` each miss is a wasted gRPC round-trip
    on this per-batch write path (#6671).
    """
    store = FeatureStore.__new__(FeatureStore)

    feature_view = MagicMock()
    feature_view.name = "driver_hourly_stats"
    feature_view.batch_source = MagicMock()

    registry = MagicMock()
    registry.get_any_feature_view.return_value = feature_view
    store._registry = registry

    current_project = MagicMock()
    current_project.get.return_value = "test_project"
    store._current_project = current_project
    store.config = MagicMock()

    provider = MagicMock()
    provider.get_table_column_names_and_types_from_data_source.return_value = [
        ("driver_id", "INT64"),
    ]

    df = pd.DataFrame({"driver_id": [1, 2, 3]})

    with patch.object(FeatureStore, "_get_provider", return_value=provider):
        store.write_to_offline_store("driver_hourly_stats", df, reorder_columns=False)

    # A single unified lookup, with the default allow_registry_cache=True
    # forwarded as allow_cache.
    registry.get_any_feature_view.assert_called_once_with(
        "driver_hourly_stats", "test_project", allow_cache=True
    )
    # None of the legacy per-type getters are used, so there is no
    # guaranteed-miss lookup before the real one.
    registry.get_stream_feature_view.assert_not_called()
    registry.get_feature_view.assert_not_called()
    registry.get_label_view.assert_not_called()

    provider.ingest_df_to_offline_store.assert_called_once()
