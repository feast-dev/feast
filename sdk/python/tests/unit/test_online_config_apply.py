from datetime import timedelta
from pathlib import Path
from typing import Optional

from feast import Entity, FeatureStore, FeatureView, Field, OnlineConfig, ValueType
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64


def _feature_store(tmp_path: Path) -> FeatureStore:
    return FeatureStore(
        config=RepoConfig(
            registry=str(tmp_path / "registry.db"),
            project="online_config_test",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=str(tmp_path / "online.db")),
            entity_key_serialization_version=3,
        )
    )


def test_apply_persists_updates_and_clears_online_config(tmp_path: Path) -> None:
    store = _feature_store(tmp_path)
    source = FileSource(
        path="events.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )
    entity = Entity(name="event", join_keys=["event_id"], value_type=ValueType.INT64)
    schema = [
        Field(name="event_id", dtype=Int64),
        Field(name="event_value", dtype=Float32),
    ]

    def feature_view(online_config: Optional[OnlineConfig]) -> FeatureView:
        return FeatureView(
            name="events",
            entities=[entity],
            schema=schema,
            source=source,
            online_config=online_config,
        )

    try:
        initial_config = OnlineConfig(
            mode="sequence", max_length=10, write_mode="append"
        )
        store.apply([entity, feature_view(initial_config)])
        assert store.get_feature_view("events").online_config == initial_config
        versions_before_config_update = store.list_feature_view_versions("events")

        updated_config = OnlineConfig(
            mode="sequence",
            max_length=50,
            max_age=timedelta(days=90),
            write_mode="append",
        )
        store.apply([entity, feature_view(updated_config)])
        assert store.get_feature_view("events").online_config == updated_config
        assert (
            store.list_feature_view_versions("events") == versions_before_config_update
        )

        store.apply([entity, feature_view(None)])
        stored = store.get_feature_view("events")
        assert stored.online_config is None
        assert not stored.to_proto().spec.HasField("online_config")
        assert (
            store.list_feature_view_versions("events") == versions_before_config_update
        )
    finally:
        store.teardown()
