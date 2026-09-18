from datetime import timedelta

from feast import Entity, FeatureView, OnlineConfig, ValueType
from feast.diff.registry_diff import diff_registry_objects
from feast.infra.offline_stores.file_source import FileSource


def test_diff_registry_objects_feature_view_online_config() -> None:
    source = FileSource(path="events.parquet", timestamp_field="event_timestamp")
    entity = Entity(name="event", join_keys=["event_id"], value_type=ValueType.INT64)
    current = FeatureView(name="events", entities=[entity], source=source)
    updated = FeatureView(
        name="events",
        entities=[entity],
        source=source,
        online_config=OnlineConfig(
            mode="sequence",
            max_length=50,
            max_age=timedelta(days=90),
            write_mode="append",
        ),
    )

    diff = diff_registry_objects(current, updated, "feature view")

    assert [
        property_diff.property_name
        for property_diff in diff.feast_object_property_diffs
    ] == ["online_config"]
