import copy
from datetime import timedelta

import pytest

from feast import FileSource
from feast.entity import Entity
from feast.field import Field
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.sort_key import SortKey
from feast.sorted_feature_view import SortedFeatureView
from feast.types import Int64, String
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

    assert (
        "For SortedFeatureView: invalid_sorted_feature_view, must have at least one sort key defined"
        in str(excinfo.value)
    )


def test_sorted_feature_view_ensure_valid_sort_key_in_entity_columns():
    """
    Test that a SortedFeatureView fails validation if any sort key's name is part of the entity columns.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])

    sort_key = SortKey(
        name="entity1",
        value_type=ValueType.STRING,
        default_sort_order=SortOrder.ASC,
    )

    # Create a SortedFeatureView with a sort key that conflicts.
    with pytest.raises(ValueError) as excinfo:
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            sort_keys=[sort_key],
        )

    assert (
        "For SortedFeatureView: invalid_sorted_feature_view, Sort key 'entity1' refers to an entity column and cannot be used as a sort key."
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

    with pytest.raises(ValueError):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
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


def test_sorted_feature_view_reserved_feature_name_event_ts():
    """
    Test that a SortedFeatureView fails validation if a feature is named 'event_ts'
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [Field(name="event_ts", dtype=Int64), Field(name="feature1", dtype=Int64)]
    sort_key = SortKey(
        name="feature1",
        value_type=ValueType.INT64,
        default_sort_order=SortOrder.ASC,
    )
    with pytest.raises(ValueError):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
        )


def test_sorted_feature_view_reserved_feature_name_created_ts():
    """
    Test that a SortedFeatureView fails validation if a feature is named 'created_ts'
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [
        Field(name="created_ts", dtype=Int64),
        Field(name="feature1", dtype=Int64),
    ]
    sort_key = SortKey(
        name="feature1",
        value_type=ValueType.INT64,
        default_sort_order=SortOrder.ASC,
    )
    with pytest.raises(ValueError):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
            ttl=timedelta(days=1),
        )


def test_sorted_feature_view_reserved_feature_name_entity_key():
    """
    Test that a SortedFeatureView fails validation if a feature is named 'entity_key'
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    schema = [
        Field(name="entity_key", dtype=Int64),
        Field(name="feature1", dtype=Int64),
    ]
    sort_key = SortKey(
        name="feature1",
        value_type=ValueType.INT64,
        default_sort_order=SortOrder.ASC,
    )
    with pytest.raises(ValueError):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
            ttl=timedelta(days=1),
        )


def test_sorted_feature_view_entity_conflict_feature_name():
    """
    Test that a SortedFeatureView fails validation if a feature name conflicts with an entity name.
    """
    source = FileSource(path="some path")
    entity = Entity(name="customer", join_keys=["customer_id"])
    # Schema with a field that conflicts with the entity name "customer".
    schema = [Field(name="customer", dtype=Int64), Field(name="feature1", dtype=Int64)]
    sort_key = SortKey(
        name="feature1",
        value_type=ValueType.INT64,
        default_sort_order=SortOrder.ASC,
    )
    with pytest.raises(ValueError):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
            ttl=timedelta(days=1),
        )


def test_sorted_feature_view_duplicate_features():
    """
    Test that a SortedFeatureView fails validation if duplicate feature names are present.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    # Schema with duplicate feature names.
    schema = [
        Field(name="dup_field", dtype=Int64),
        Field(name="dup_field", dtype=Int64),
    ]
    sort_key = SortKey(
        name="dup_field",
        value_type=ValueType.INT64,
        default_sort_order=SortOrder.ASC,
    )
    with pytest.raises(ValueError, match="Duplicate feature name found: 'dup_field'"):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key],
        )


def test_sorted_feature_view_duplicate_sort_keys():
    """
    Test that a SortedFeatureView fails validation if duplicate sort key names are present.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    # Schema with duplicate feature names.
    schema = [
        Field(name="dup_field", dtype=Int64),
    ]
    sort_key_1 = SortKey(
        name="dup_field",
        value_type=ValueType.INT64,
        default_sort_order=SortOrder.ASC,
    )
    sort_key_2 = SortKey(
        name="dup_field",
        value_type=ValueType.INT64,
        default_sort_order=SortOrder.ASC,
    )
    with pytest.raises(ValueError, match="Duplicate sort key found: 'dup_field'."):
        SortedFeatureView(
            name="invalid_sorted_feature_view",
            source=source,
            entities=[entity],
            schema=schema,
            sort_keys=[sort_key_1, sort_key_2],
        )


def test_sorted_feature_view_invalid_sort_key_order_str():
    """
    Test that a SortedFeatureView fails validation if default_sort_order is incorrect.
    """

    with pytest.raises(ValueError, match="default_sort_order must be 'ASC' or 'DESC'"):
        SortKey(
            name="dup_field",
            value_type=ValueType.INT64,
            default_sort_order="99",
        )


def test_sorted_feature_view_invalid_sort_key_order_int():
    """
    Test that a SortedFeatureView fails validation if default_sort_order is incorrect.
    """

    with pytest.raises(
        ValueError, match="default_sort_order must be SortOrder.ASC or SortOrder.DESC"
    ):
        SortKey(
            name="dup_field",
            value_type=ValueType.INT64,
            default_sort_order=99,
        )


def test_sfv_compatibility_same():
    """
    Two identical SortedFeatureViews should be compatible with no reasons.
    """
    source = FileSource(path="dummy", event_timestamp_column="ts")
    entity = Entity(name="e1", join_keys=["e1_id"])
    schema = [Field(name="f1", dtype=Int64), Field(name="f2", dtype=String)]
    sort_key = SortKey(
        name="f1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )
    sfv1 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
    )
    sfv2 = copy.copy(sfv1)

    ok, reasons = sfv1.is_update_compatible_with(sfv2)
    assert ok
    assert reasons == []


def test_sfv_compatibility_remove_non_sort_feature():
    """
    Removing a feature not in sort_keys should still be compatible.
    """
    source = FileSource(path="dummy", event_timestamp_column="ts")
    entity = Entity(name="e1", join_keys=["e1_id"])
    schema1 = [Field(name="f1", dtype=Int64), Field(name="f2", dtype=String)]
    schema2 = [Field(name="f1", dtype=Int64)]
    sort_key = SortKey(
        name="f1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )
    sfv1 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity],
        schema=schema1,
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
    )
    sfv2 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity],
        schema=schema2,
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
    )

    ok, reasons = sfv1.is_update_compatible_with(sfv2)
    assert ok
    assert reasons == []


def test_sfv_compatibility_change_sort_keys():
    """
    Changing sort_keys should produce incompatibility reasons.
    """
    source = FileSource(path="dummy", event_timestamp_column="ts")
    entity = Entity(name="e1", join_keys=["e1_id"])
    schema = [Field(name="k1", dtype=Int64), Field(name="k2", dtype=Int64)]
    sort_key1 = SortKey(
        name="k1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )
    sort_key2 = SortKey(
        name="k2", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )
    sfv1 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key1],
    )
    sfv2 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key2],
    )

    ok, reasons = sfv1.is_update_compatible_with(sfv2)
    assert not ok
    assert any("sort keys cannot change" in r for r in reasons)


def test_sfv_compatibility_change_entity():
    """
    Changing the entity list should produce incompatibility reasons.
    """
    source = FileSource(path="dummy", event_timestamp_column="ts")
    entity1 = Entity(name="e1", join_keys=["e1_id"])
    entity2 = Entity(name="e2", join_keys=["e2_id"])
    schema = [Field(name="f1", dtype=Int64)]
    sort_key = SortKey(
        name="f1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )
    sfv1 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity1],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
    )
    sfv2 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity2],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
    )

    current_time = _utc_now()
    start_date = make_tzaware(current_time - timedelta(days=1))
    end_date = make_tzaware(current_time)
    sfv1.materialization_intervals.append((start_date, end_date))

    ok, reasons = sfv1.is_update_compatible_with(sfv2)
    assert not ok
    assert any("entity definitions cannot change" in r for r in reasons)


def test_sfv_compatibility_change_sort_key_dtype():
    """
    Changing the sort key's dtype should produce incompatibility reasons.
    """
    source = FileSource(path="dummy", event_timestamp_column="ts")
    entity = Entity(name="e1", join_keys=["e1_id"])
    schema = [Field(name="f1", dtype=Int64)]
    sort_key1 = SortKey(
        name="f1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )
    sort_key2 = SortKey(
        name="f1", value_type=ValueType.INT64, default_sort_order=SortOrder.DESC
    )
    sfv1 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key1],
    )
    sfv2 = SortedFeatureView(
        name="sfv",
        source=source,
        entities=[entity],
        schema=schema,
        ttl=timedelta(days=1),
        sort_keys=[sort_key2],
    )

    ok, reasons = sfv1.is_update_compatible_with(sfv2)
    assert not ok
    assert any("sort key 'f1' sort order changed" in r for r in reasons)
