from google.protobuf import wrappers_pb2 as wrappers

from feast.diff.infra_diff import (
    diff_between,
    diff_infra_protos,
    tag_infra_proto_objects_for_keep_delete_add,
)
from feast.diff.property_diff import TransitionType
from feast.infra.online_stores.datastore import DatastoreTable
from feast.infra.online_stores.dynamodb import DynamoDBTable
from feast.protos.feast.core.InfraObject_pb2 import Infra as InfraProto


def test_tag_infra_proto_objects_for_keep_delete_add():
    to_delete = DynamoDBTable(name="to_delete", region="us-west-2").to_proto()
    to_add = DynamoDBTable(name="to_add", region="us-west-2").to_proto()
    unchanged_table = DynamoDBTable(name="unchanged", region="us-west-2").to_proto()
    pre_changed = DynamoDBTable(name="table", region="us-west-2").to_proto()
    post_changed = DynamoDBTable(name="table", region="us-east-2").to_proto()

    keep, delete, add = tag_infra_proto_objects_for_keep_delete_add(
        [to_delete, unchanged_table, pre_changed],
        [to_add, unchanged_table, post_changed],
    )

    assert len(list(keep)) == 2
    assert unchanged_table in keep
    assert post_changed in keep
    assert to_add not in keep
    assert len(list(delete)) == 1
    assert to_delete in delete
    assert unchanged_table not in delete
    assert pre_changed not in delete
    assert len(list(add)) == 1
    assert to_add in add
    assert unchanged_table not in add
    assert post_changed not in add


def test_diff_between_datastore_tables():
    pre_changed = DatastoreTable(
        project="test", name="table", project_id="pre", namespace="pre"
    ).to_proto()
    post_changed = DatastoreTable(
        project="test", name="table", project_id="post", namespace="post"
    ).to_proto()

    infra_object_diff = diff_between(pre_changed, pre_changed, "datastore table")
    infra_object_property_diffs = infra_object_diff.infra_object_property_diffs
    assert len(infra_object_property_diffs) == 0

    infra_object_diff = diff_between(pre_changed, post_changed, "datastore table")
    infra_object_property_diffs = infra_object_diff.infra_object_property_diffs
    assert len(infra_object_property_diffs) == 2

    assert infra_object_property_diffs[0].property_name == "project_id"
    assert infra_object_property_diffs[0].val_existing == wrappers.StringValue(
        value="pre"
    )
    assert infra_object_property_diffs[0].val_declared == wrappers.StringValue(
        value="post"
    )
    assert infra_object_property_diffs[1].property_name == "namespace"
    assert infra_object_property_diffs[1].val_existing == wrappers.StringValue(
        value="pre"
    )
    assert infra_object_property_diffs[1].val_declared == wrappers.StringValue(
        value="post"
    )


def test_diff_infra_protos():
    to_delete = DynamoDBTable(name="to_delete", region="us-west-2")
    to_add = DynamoDBTable(name="to_add", region="us-west-2")
    unchanged_table = DynamoDBTable(name="unchanged", region="us-west-2")
    pre_changed = DatastoreTable(
        project="test", name="table", project_id="pre", namespace="pre"
    )
    post_changed = DatastoreTable(
        project="test", name="table", project_id="post", namespace="post"
    )

    infra_objects_before = [to_delete, unchanged_table, pre_changed]
    infra_objects_after = [to_add, unchanged_table, post_changed]

    infra_proto_before = InfraProto()
    infra_proto_before.infra_objects.extend(
        [obj.to_infra_object_proto() for obj in infra_objects_before]
    )

    infra_proto_after = InfraProto()
    infra_proto_after.infra_objects.extend(
        [obj.to_infra_object_proto() for obj in infra_objects_after]
    )

    infra_diff = diff_infra_protos(infra_proto_before, infra_proto_after)
    infra_object_diffs = infra_diff.infra_object_diffs

    # There should be one addition, one deletion, one unchanged, and one changed.
    assert len(infra_object_diffs) == 4

    additions = [
        infra_object_diff
        for infra_object_diff in infra_object_diffs
        if infra_object_diff.transition_type == TransitionType.CREATE
    ]
    assert len(additions) == 1
    assert not additions[0].current_infra_object
    assert additions[0].new_infra_object == to_add.to_proto()
    assert len(additions[0].infra_object_property_diffs) == 0

    deletions = [
        infra_object_diff
        for infra_object_diff in infra_object_diffs
        if infra_object_diff.transition_type == TransitionType.DELETE
    ]
    assert len(deletions) == 1
    assert deletions[0].current_infra_object == to_delete.to_proto()
    assert not deletions[0].new_infra_object
    assert len(deletions[0].infra_object_property_diffs) == 0

    unchanged = [
        infra_object_diff
        for infra_object_diff in infra_object_diffs
        if infra_object_diff.transition_type == TransitionType.UNCHANGED
    ]
    assert len(unchanged) == 1
    assert unchanged[0].current_infra_object == unchanged_table.to_proto()
    assert unchanged[0].new_infra_object == unchanged_table.to_proto()
    assert len(unchanged[0].infra_object_property_diffs) == 0

    updates = [
        infra_object_diff
        for infra_object_diff in infra_object_diffs
        if infra_object_diff.transition_type == TransitionType.UPDATE
    ]
    assert len(updates) == 1
    assert updates[0].current_infra_object == pre_changed.to_proto()
    assert updates[0].new_infra_object == post_changed.to_proto()
    assert len(updates[0].infra_object_property_diffs) == 2
    assert updates[0].infra_object_property_diffs[0].property_name == "project_id"
    assert updates[0].infra_object_property_diffs[
        0
    ].val_existing == wrappers.StringValue(value="pre")
    assert updates[0].infra_object_property_diffs[
        0
    ].val_declared == wrappers.StringValue(value="post")
    assert updates[0].infra_object_property_diffs[1].property_name == "namespace"
    assert updates[0].infra_object_property_diffs[
        1
    ].val_existing == wrappers.StringValue(value="pre")
    assert updates[0].infra_object_property_diffs[
        1
    ].val_declared == wrappers.StringValue(value="post")
