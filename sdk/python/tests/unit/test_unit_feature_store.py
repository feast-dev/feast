from dataclasses import dataclass
from typing import Dict, List

import pytest

from feast import utils
from feast.protos.feast.types.Value_pb2 import Value


@dataclass
class MockFeatureViewProjection:
    join_key_map: Dict[str, str]


@dataclass
class MockFeatureView:
    name: str
    entities: List[str]
    projection: MockFeatureViewProjection


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

    unique_entities, indexes = utils._get_unique_entities(
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

    unique_entities, indexes = utils._get_unique_entities(
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
