from feast.diff.FcoDiff import diff_between, tag_proto_objects_for_keep_delete_add
from feast.feature_view import FeatureView
from tests.utils.data_source_utils import prep_file_source


def test_tag_proto_objects_for_keep_delete_add(simple_dataset_1):
    with prep_file_source(
        df=simple_dataset_1, event_timestamp_column="ts_1"
    ) as file_source:
        to_delete = FeatureView(
            name="to_delete", entities=["id"], batch_source=file_source, ttl=None,
        ).to_proto()
        unchanged_fv = FeatureView(
            name="fv1", entities=["id"], batch_source=file_source, ttl=None,
        ).to_proto()
        pre_changed = FeatureView(
            name="fv2",
            entities=["id"],
            batch_source=file_source,
            ttl=None,
            tags={"when": "before"},
        ).to_proto()
        post_changed = FeatureView(
            name="fv2",
            entities=["id"],
            batch_source=file_source,
            ttl=None,
            tags={"when": "after"},
        ).to_proto()
        to_add = FeatureView(
            name="to_add", entities=["id"], batch_source=file_source, ttl=None,
        ).to_proto()

        keep, delete, add = tag_proto_objects_for_keep_delete_add(
            [unchanged_fv, pre_changed, to_delete], [unchanged_fv, post_changed, to_add]
        )

        assert len(list(keep)) == 2
        assert unchanged_fv in keep
        assert post_changed in keep
        assert pre_changed not in keep
        assert len(list(delete)) == 1
        assert to_delete in delete
        assert len(list(add)) == 1
        assert to_add in add


def test_diff_between_feature_views(simple_dataset_1):
    with prep_file_source(
        df=simple_dataset_1, event_timestamp_column="ts_1"
    ) as file_source:
        pre_changed = FeatureView(
            name="fv2",
            entities=["id"],
            batch_source=file_source,
            ttl=None,
            tags={"when": "before"},
        ).to_proto()
        post_changed = FeatureView(
            name="fv2",
            entities=["id"],
            batch_source=file_source,
            ttl=None,
            tags={"when": "after"},
        ).to_proto()

        fco_diffs = diff_between(pre_changed, pre_changed, "feature view")
        assert len(fco_diffs.fco_property_diffs) == 0

        fco_diffs = diff_between(pre_changed, post_changed, "feature view")
        assert len(fco_diffs.fco_property_diffs) == 1

        assert fco_diffs.fco_property_diffs[0].property_name == "tags"
        assert fco_diffs.fco_property_diffs[0].val_existing == {"when": "before"}
        assert fco_diffs.fco_property_diffs[0].val_declared == {"when": "after"}
