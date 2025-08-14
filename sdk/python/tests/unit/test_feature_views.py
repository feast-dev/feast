import copy
from datetime import timedelta

import pytest
from typeguard import TypeCheckError

from feast.batch_feature_view import BatchFeatureView
from feast.data_format import AvroFormat
from feast.data_source import KafkaSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.protos.feast.types.Value_pb2 import ValueType
from feast.types import Float32, Int64, String
from feast.utils import _utc_now, make_tzaware


def make_fv(name, fields, entities, ttl):
    """
    Helper to create a simple FeatureView with given field names and dtypes.
    """
    source = FileSource(path="dummy_path")
    schema = [Field(name=fname, dtype=ftype) for fname, ftype in fields]
    fv = FeatureView(
        name=name,
        source=source,
        entities=entities,
        schema=schema,
        ttl=ttl,
    )
    current_time = _utc_now()
    start_date = make_tzaware(current_time - timedelta(days=1))
    end_date = make_tzaware(current_time)
    fv.materialization_intervals.append((start_date, end_date))

    return fv


def make_fv_no_materialization_interval(name, fields, entities, ttl):
    """
    Helper to create a simple FeatureView with given field names and dtypes.
    """
    source = FileSource(path="dummy_path")
    schema = [Field(name=fname, dtype=ftype) for fname, ftype in fields]
    return FeatureView(
        name=name,
        source=source,
        entities=entities,
        schema=schema,
        ttl=ttl,
    )


def test_create_feature_view_with_conflicting_entities():
    user1 = Entity(name="user1", join_keys=["user_id"])
    user2 = Entity(name="user2", join_keys=["user_id"])
    batch_source = FileSource(path="some path")

    with pytest.raises(ValueError):
        _ = FeatureView(
            name="test",
            entities=[user1, user2],
            ttl=timedelta(days=30),
            source=batch_source,
        )


def test_create_batch_feature_view():
    batch_source = FileSource(path="some path")
    BatchFeatureView(
        name="test batch feature view",
        entities=[],
        ttl=timedelta(days=30),
        source=batch_source,
    )

    with pytest.raises(TypeError):
        BatchFeatureView(
            name="test batch feature view", entities=[], ttl=timedelta(days=30)
        )

    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
    )
    with pytest.raises(ValueError):
        BatchFeatureView(
            name="test batch feature view",
            entities=[],
            ttl=timedelta(days=30),
            source=stream_source,
        )


def simple_udf(x: int):
    return x + 3


def test_hash():
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view_1 = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    feature_view_2 = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    feature_view_3 = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
    )
    feature_view_4 = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
        description="test",
    )

    s1 = {feature_view_1, feature_view_2}
    assert len(s1) == 1

    s2 = {feature_view_1, feature_view_3}
    assert len(s2) == 2

    s3 = {feature_view_3, feature_view_4}
    assert len(s3) == 2

    s4 = {feature_view_1, feature_view_2, feature_view_3, feature_view_4}
    assert len(s4) == 3


# TODO(felixwang9817): Add tests for proto conversion.
# TODO(felixwang9817): Add tests for field mapping logic.


def test_field_types():
    with pytest.raises(TypeCheckError):
        Field(name="name", dtype=ValueType.INT32)


def test_update_materialization_intervals():
    batch_source = FileSource(path="some path")
    entity = Entity(name="entity_1", description="Some entity")
    # Create a feature view that is already present in the SQL registry
    stored_feature_view = FeatureView(
        name="my-feature-view",
        entities=[entity],
        ttl=timedelta(days=1),
        source=batch_source,
    )

    # Update the Feature View without modifying anything
    updated_feature_view = FeatureView(
        name="my-feature-view",
        entities=[entity],
        ttl=timedelta(days=1),
        source=batch_source,
    )
    updated_feature_view.update_materialization_intervals(
        stored_feature_view.materialization_intervals
    )
    assert len(updated_feature_view.materialization_intervals) == 0

    current_time = _utc_now()
    start_date = make_tzaware(current_time - timedelta(days=1))
    end_date = make_tzaware(current_time)
    updated_feature_view.materialization_intervals.append((start_date, end_date))

    # Update the Feature View, i.e. simply update the name
    second_updated_feature_view = FeatureView(
        name="my-feature-view-1",
        entities=[entity],
        ttl=timedelta(days=1),
        source=batch_source,
    )

    second_updated_feature_view.update_materialization_intervals(
        updated_feature_view.materialization_intervals
    )
    assert len(second_updated_feature_view.materialization_intervals) == 1
    assert (
        second_updated_feature_view.materialization_intervals[0][0]
        == updated_feature_view.materialization_intervals[0][0]
    )
    assert (
        second_updated_feature_view.materialization_intervals[0][1]
        == updated_feature_view.materialization_intervals[0][1]
    )


def test_get_online_store_tags_retrieval():
    # Test when TTL is set as a valid integer in tags
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view = FeatureView(
        name="feature_view_with_ttl",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
        tags={"online_store_key_ttl_seconds": "3600", "online_store_batch_size": "100"},
    )
    expected = {"key_ttl_seconds": "3600", "batch_size": "100"}
    assert feature_view.get_online_store_tags == expected


def test_get_online_store_tags_none_when_not_set():
    # Test when TTL is not set in tags, expecting None
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view = FeatureView(
        name="feature_view_without_ttl",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
        tags={},
    )
    expected = {}
    assert feature_view.get_online_store_tags == expected


def test_fv_compatibility_same():
    """
    Two identical FeatureViews should be compatible with no reasons.
    """
    entity = Entity(name="e1", join_keys=["e1_id"])
    fv1 = make_fv(
        "fv", [("feat1", Int64), ("feat2", String)], [entity], timedelta(days=1)
    )
    fv2 = copy.copy(fv1)

    ok, reasons = fv1.is_update_compatible_with(fv2)
    assert ok
    assert reasons == []


def test_fv_compatibility_remove_non_entity_feature(caplog):
    """
    Removing a non-entity feature should be compatible, warning logged.
    """
    entity = Entity(name="e1", join_keys=["e1_id"])
    fv1 = make_fv(
        "fv", [("feat1", Int64), ("feat2", String)], [entity], timedelta(days=1)
    )
    fv2 = make_fv("fv", [("feat1", Int64)], [entity], timedelta(days=1))
    caplog.set_level("INFO")
    ok, reasons = fv1.is_update_compatible_with(fv2)
    assert ok
    assert reasons == []
    assert "Feature 'feat2' removed from FeatureView 'fv'." in caplog.text


def test_fv_compatibility_change_entities():
    """
    Changing the entity list should be incompatible and list reason.
    """
    ent1 = Entity(name="e1", join_keys=["e1_id"])
    ent2 = Entity(name="e2", join_keys=["e2_id"])
    fv1 = make_fv("fv", [("feat1", Int64)], [ent1], timedelta(days=1))
    fv2 = make_fv("fv", [("feat1", Int64)], [ent2], timedelta(days=1))

    ok, reasons = fv1.is_update_compatible_with(fv2)
    assert not ok
    assert "entity definitions cannot change for FeatureView: fv" in reasons


def test_fv_compatibility_change_entities_with_no_materialization_interval():
    """
    Changing the entity list should be ok.
    """
    ent1 = Entity(name="e1", join_keys=["e1_id"])
    ent2 = Entity(name="e2", join_keys=["e2_id"])
    fv1 = make_fv_no_materialization_interval(
        "fv", [("feat1", Int64)], [ent1], timedelta(days=1)
    )
    fv2 = make_fv_no_materialization_interval(
        "fv", [("feat1", Int64)], [ent2], timedelta(days=1)
    )

    ok, reasons = fv1.is_update_compatible_with(fv2)
    assert ok
    assert reasons == []
