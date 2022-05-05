from dataclasses import dataclass
from typing import Dict, List

from feast import FeatureStore
from feast.protos.feast.types.Value_pb2 import Value


@dataclass
class MockFeatureViewProjection:
    join_key_map: Dict[str, str]


@dataclass
class MockFeatureView:
    name: str
    entities: List[str]
    projection: MockFeatureViewProjection


def test__get_unique_entities():
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

    unique_entities, indexes = FeatureStore._get_unique_entities(
        FeatureStore,
        table=fv,
        join_key_values=entity_values,
        entity_name_to_join_key_map=entity_name_to_join_key_map,
    )

    assert unique_entities == (
        {"entity_1": Value(int64_val=1), "entity_2": Value(string_val="1")},
        {"entity_1": Value(int64_val=2), "entity_2": Value(string_val="2")},
    )
    assert indexes == ([0, 2], [1])
