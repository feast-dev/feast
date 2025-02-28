import copy
from datetime import timedelta

import pytest

from feast import FileSource
from feast.entity import Entity
from feast.field import Field
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.sort_key import SortKey
from feast.sorted_feature_view import SortedFeatureView
from feast.types import Float32, Int64, String
from feast.utils import _utc_now, make_tzaware
from feast.value_type import ValueType


def test_sorted_feature_view_to_proto_and_from_proto():
    """
    Test round-trip conversion:
      - Create a SortedFeatureView with a sort key.
      - Convert it to its proto representation.
      - Convert back from proto.
      - Verify that key attributes (name, description, tags, owner, sort keys, etc.) are preserved.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [Field(name="sort_key1", dtype=Int64), Field(name="feature1", dtype=Int64)]

    sort_key = SortKey(
        name="sort_key1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )

    sfv = SortedFeatureView(
        name="sorted_feature_view_test",
        source=source,
        entities=[entity],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
        description="test sorted feature view",
        tags={"test": "true"},
        owner="test_owner",
    )

    proto = sfv.to_proto()
    sfv_from_proto = SortedFeatureView.from_proto(proto)

    assert sfv.name == sfv_from_proto.name
    assert sfv.description == sfv_from_proto.description
    assert sfv.tags == sfv_from_proto.tags
    assert sfv.owner == sfv_from_proto.owner

    assert len(sfv.sort_keys) == len(sfv_from_proto.sort_keys)
    for original_sk, proto_sk in zip(sfv.sort_keys, sfv_from_proto.sort_keys):
        assert original_sk.name == proto_sk.name
        assert original_sk.default_sort_order == proto_sk.default_sort_order
        assert original_sk.value_type == proto_sk.value_type


def test_sorted_feature_view_ensure_valid():
    """
    Test that a SortedFeatureView without any sort keys fails validation.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])

    with pytest.raises(ValueError) as excinfo:
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            sort_keys=[],
        )

    assert "must have at least one sort key defined" in str(excinfo.value)


def test_sorted_feature_view_ensure_valid_sort_key_in_entity_columns():
    """
    Test that a SortedFeatureView fails validation if any sort key's name is part of the entity columns.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    entity_field = Field(name="entity1", dtype=Float32)

    sort_key = SortKey(
        name="entity1",
        value_type=ValueType.STRING,
        default_sort_order=SortOrder.ASC,  # Assuming ASC is valid.
    )

    # Create a SortedFeatureView with a sort key that conflicts.
    with pytest.raises(ValueError) as excinfo:
        sfv = SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            sort_keys=[sort_key],
        )

        sfv.entity_columns = [entity_field]

    assert (
        "Sort key 'entity1' does not match any feature name or the event timestamp. Valid options are: []"
        in str(excinfo.value)
    )


def test_sorted_feature_view_copy():
    """
    Test that __copy__ produces a valid and independent copy of a SortedFeatureView.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [Field(name="dummy_sort_key", dtype=String)]

    sort_key = SortKey(
        name="dummy_sort_key",
        value_type=ValueType.STRING,
        default_sort_order=SortOrder.ASC,
    )

    sfv = SortedFeatureView(
        name="sorted_feature_view_test",
        source=source,
        entities=[entity],
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
        schema=schema,
        description="Test sorted feature view",
        tags={"test": "true"},
        owner="test_owner",
    )

    sfv_copy = copy.copy(sfv)
    # Check that the copied object's attributes match.
    assert sfv.name == sfv_copy.name
    assert sfv.sort_keys == sfv_copy.sort_keys
    assert sfv.features == sfv_copy.features
    # Check that modifying the copy does not affect the original.
    sfv_copy.tags["new_key"] = "new_value"
    assert "new_key" not in sfv.tags


def test_sorted_feature_view_materialization_intervals_update():
    """
    Test that the update_materialization_intervals method correctly updates intervals.
    """
    source = FileSource(path="dummy/path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [Field(name="dummy", dtype=String)]

    sfv = SortedFeatureView(
        name="sorted_feature_view_test",
        source=source,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=schema,
        sort_keys=[
            SortKey(
                name="dummy",
                value_type=ValueType.STRING,
                default_sort_order=SortOrder.ASC,
            )
        ],
    )

    assert len(sfv.materialization_intervals) == 0

    # Add one interval.
    current_time = _utc_now()
    start_date = make_tzaware(current_time - timedelta(days=1))
    end_date = make_tzaware(current_time)
    sfv.materialization_intervals.append((start_date, end_date))

    new_sfv = SortedFeatureView(
        name="sorted_feature_view_updated",
        source=source,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=schema,
        sort_keys=[
            SortKey(
                name="dummy",
                value_type=ValueType.STRING,
                default_sort_order=SortOrder.ASC,
            )
        ],
    )
    new_sfv.update_materialization_intervals(sfv.materialization_intervals)
    assert len(new_sfv.materialization_intervals) == 1
    assert new_sfv.materialization_intervals[0] == (start_date, end_date)


def test_sorted_feature_view_multiple_sort_keys():
    """
    Test that a SortedFeatureView with multiple sort keys is valid and correctly serialized/deserialized.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [
        Field(name="sort_key1", dtype=Int64),
        Field(name="sort_key2", dtype=String),
        Field(name="feature1", dtype=Int64),
    ]

    sort_key1 = SortKey(
        name="sort_key1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )
    sort_key2 = SortKey(
        name="sort_key2", value_type=ValueType.STRING, default_sort_order=SortOrder.DESC
    )

    sfv = SortedFeatureView(
        name="sorted_feature_view_test",
        source=source,
        entities=[entity],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key1, sort_key2],
        description="test sorted feature view with multiple sort keys",
        tags={"test": "true"},
        owner="test_owner",
    )

    proto = sfv.to_proto()
    sfv_from_proto = SortedFeatureView.from_proto(proto)

    assert sfv.name == sfv_from_proto.name
    assert sfv.description == sfv_from_proto.description
    assert sfv.tags == sfv_from_proto.tags
    assert sfv.owner == sfv_from_proto.owner

    assert len(sfv.sort_keys) == len(sfv_from_proto.sort_keys)
    for original_sk, proto_sk in zip(sfv.sort_keys, sfv_from_proto.sort_keys):
        assert original_sk.name == proto_sk.name
        assert original_sk.default_sort_order == proto_sk.default_sort_order
        assert original_sk.value_type == proto_sk.value_type


def test_sorted_feature_view_sort_key_not_in_schema():
    """
    Test that a SortedFeatureView fails validation if the sort key is not defined in the schema.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [Field(name="feature1", dtype=Int64)]

    sort_key = SortKey(
        name="sort_key1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )

    with pytest.raises(ValueError) as excinfo:
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
        )

    assert (
        "Sort key 'sort_key1' does not match any feature name or the event timestamp. Valid options are: ['feature1']"
        in str(excinfo.value)
    )


def test_sorted_feature_view_sort_key_invalid_data_type():
    """
    Test that a SortedFeatureView fails validation if the sort key's data type does not match the feature's data type.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [Field(name="sort_key1", dtype=Int64), Field(name="feature1", dtype=Int64)]

    # Define a sort key with a different data type than the feature
    sort_key = SortKey(
        name="sort_key1", value_type=ValueType.STRING, default_sort_order=SortOrder.ASC
    )

    with pytest.raises(ValueError):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
        )


def test_sorted_feature_view_same_sort_key_repeated_in_schema():
    """
    Test that a SortedFeatureView fails validation if the sort key's data type does not match the feature's data type.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [
        Field(name="sort_key1", dtype=Int64),
        Field(name="sort_key1", dtype=String),
    ]

    # Define a sort key with a different data type than the feature
    sort_key = SortKey(
        name="sort_key1", value_type=ValueType.STRING, default_sort_order=SortOrder.ASC
    )

    with pytest.raises(ValueError):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
        )


def test_sorted_feature_view_sort_key_matches_event_timestamp():
    """
    Test that a SortedFeatureView is valid if the sort key matches the event timestamp field.
    """
    source = FileSource(path="some path", timestamp_field="event_timestamp")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [
        Field(name="feature1", dtype=Int64),
    ]

    sort_key = SortKey(
        name="event_timestamp",
        value_type=ValueType.UNIX_TIMESTAMP,
        default_sort_order=SortOrder.ASC,
    )

    sfv = SortedFeatureView(
        name="valid_sorted_feature_view",
        source=source,
        entities=[entity],
        schema=schema,
        sort_keys=[sort_key],
    )

    assert sfv.name == "valid_sorted_feature_view"
    assert sfv.sort_keys[0].name == "event_timestamp"
