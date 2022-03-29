from feast.diff.registry_diff import (
    diff_registry_objects,
    tag_objects_for_keep_delete_update_add,
)
from feast.feature_view import FeatureView
from tests.utils.data_source_utils import prep_file_source


def test_tag_objects_for_keep_delete_update_add(simple_dataset_1):
    with prep_file_source(
        df=simple_dataset_1, event_timestamp_column="ts_1"
    ) as file_source:
        to_delete = FeatureView(
            name="to_delete", entities=["id"], batch_source=file_source, ttl=None,
        )
        unchanged_fv = FeatureView(
            name="fv1", entities=["id"], batch_source=file_source, ttl=None,
        )
        pre_changed = FeatureView(
            name="fv2",
            entities=["id"],
            batch_source=file_source,
            ttl=None,
            tags={"when": "before"},
        )
        post_changed = FeatureView(
            name="fv2",
            entities=["id"],
            batch_source=file_source,
            ttl=None,
            tags={"when": "after"},
        )
        to_add = FeatureView(
            name="to_add", entities=["id"], batch_source=file_source, ttl=None,
        )

        keep, delete, update, add = tag_objects_for_keep_delete_update_add(
            [unchanged_fv, pre_changed, to_delete], [unchanged_fv, post_changed, to_add]
        )

        assert len(list(keep)) == 2
        assert unchanged_fv in keep
        assert pre_changed in keep
        assert post_changed not in keep
        assert len(list(delete)) == 1
        assert to_delete in delete
        assert len(list(update)) == 2
        assert unchanged_fv in update
        assert post_changed in update
        assert pre_changed not in update
        assert len(list(add)) == 1
        assert to_add in add


def test_diff_registry_objects_feature_views(simple_dataset_1):
    with prep_file_source(
        df=simple_dataset_1, event_timestamp_column="ts_1"
    ) as file_source:
        pre_changed = FeatureView(
            name="fv2",
            entities=["id"],
            batch_source=file_source,
            ttl=None,
            tags={"when": "before"},
        )
        post_changed = FeatureView(
            name="fv2",
            entities=["id"],
            batch_source=file_source,
            ttl=None,
            tags={"when": "after"},
        )

        feast_object_diffs = diff_registry_objects(
            pre_changed, pre_changed, "feature view"
        )
        assert len(feast_object_diffs.feast_object_property_diffs) == 0

        feast_object_diffs = diff_registry_objects(
            pre_changed, post_changed, "feature view"
        )
        assert len(feast_object_diffs.feast_object_property_diffs) == 1

        assert feast_object_diffs.feast_object_property_diffs[0].property_name == "tags"
        assert feast_object_diffs.feast_object_property_diffs[0].val_existing == {
            "when": "before"
        }
        assert feast_object_diffs.feast_object_property_diffs[0].val_declared == {
            "when": "after"
        }
