from datetime import timedelta

from feast.data_format import AvroFormat
from feast.data_source import KafkaSource
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStore, SqliteOnlineStoreConfig
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import String


def _repo_config() -> RepoConfig:
    return RepoConfig(
        registry="/tmp/unused_registry.db",
        project="test_project",
        provider="local",
        online_store=SqliteOnlineStoreConfig(),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )


def _feature_view(name: str) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[],
        schema=[Field(name="value", dtype=String)],
        ttl=timedelta(days=1),
        online=True,
        source=FileSource(path="dummy.parquet", timestamp_field="event_timestamp"),
    )


def _stream_feature_view(name: str) -> StreamFeatureView:
    return StreamFeatureView(
        name=name,
        entities=[],
        schema=[Field(name="value", dtype=String)],
        source=KafkaSource(
            name="dummy_kafka",
            timestamp_field="event_timestamp",
            message_format=AvroFormat(""),
            kafka_bootstrap_servers="localhost:9092",
            topic="dummy_topic",
            batch_source=FileSource(
                path="dummy.parquet", timestamp_field="event_timestamp"
            ),
        ),
    )


class TestSqliteOnlineStorePlanWithStreamFeatureViews:
    """Regression test for a typeguard.TypeCheckError previously raised by
    plan() when the registry contains a StreamFeatureView: FeatureView.from_proto()
    was applied uniformly to both feature_views and stream_feature_views, but
    FeatureView is @typechecked and a StreamFeatureView proto is not a
    FeatureView proto."""

    def test_plan_succeeds_with_only_stream_feature_views(self):
        config = _repo_config()
        registry_proto = RegistryProto()
        registry_proto.stream_feature_views.append(
            _stream_feature_view("driver_dropoffs_stream").to_proto()
        )

        infra_objects = SqliteOnlineStore().plan(config, registry_proto)

        assert len(infra_objects) == 1
        assert infra_objects[0].name == "test_project_driver_dropoffs_stream"

    def test_plan_succeeds_with_batch_and_stream_feature_views_together(self):
        config = _repo_config()
        registry_proto = RegistryProto()
        registry_proto.feature_views.append(_feature_view("batch_view").to_proto())
        registry_proto.stream_feature_views.append(
            _stream_feature_view("driver_dropoffs_stream").to_proto()
        )

        infra_objects = SqliteOnlineStore().plan(config, registry_proto)

        assert sorted(o.name for o in infra_objects) == [
            "test_project_batch_view",
            "test_project_driver_dropoffs_stream",
        ]
