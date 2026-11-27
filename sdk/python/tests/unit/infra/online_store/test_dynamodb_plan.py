from datetime import timedelta

from feast.data_format import AvroFormat
from feast.data_source import KafkaSource
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.infra_object import Infra
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.dynamodb import (
    DynamoDBOnlineStore,
    DynamoDBOnlineStoreConfig,
    DynamoDBTable,
)
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import String

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "test_aws"
PROVIDER = "aws"
REGION = "us-west-2"


def _repo_config() -> RepoConfig:
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(region=REGION),
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


class TestDynamoDBOnlineStorePlan:
    """DynamoDBOnlineStore previously had no plan() override at all, so
    `feast plan` reported no DynamoDB infrastructure changes. This adds one,
    following the InfraObject.CustomInfra extension point (see
    protos/feast/core/InfraObject.proto) used by no other in-tree store yet."""

    def test_plan_returns_one_table_per_feature_view(self):
        config = _repo_config()
        registry_proto = RegistryProto()
        registry_proto.feature_views.append(_feature_view("view_a").to_proto())
        registry_proto.feature_views.append(_feature_view("view_b").to_proto())

        infra_objects = DynamoDBOnlineStore().plan(config, registry_proto)

        assert sorted(o.name for o in infra_objects) == [
            f"{PROJECT}.view_a",
            f"{PROJECT}.view_b",
        ]
        assert all(isinstance(o, DynamoDBTable) for o in infra_objects)
        assert all(o.region == REGION for o in infra_objects)

    def test_plan_includes_stream_feature_views(self):
        """Regression guard: FeatureView.from_proto() is @typechecked and
        only accepts a FeatureViewProto, so it can't be applied to
        stream_feature_views (StreamFeatureViewProto) too -- each list needs
        its matching class (see the related fix in sqlite.py's plan())."""
        config = _repo_config()
        registry_proto = RegistryProto()
        registry_proto.stream_feature_views.append(
            _stream_feature_view("driver_dropoffs_stream").to_proto()
        )

        infra_objects = DynamoDBOnlineStore().plan(config, registry_proto)

        assert [o.name for o in infra_objects] == [f"{PROJECT}.driver_dropoffs_stream"]

    def test_plan_includes_batch_and_stream_feature_views_together(self):
        config = _repo_config()
        registry_proto = RegistryProto()
        registry_proto.feature_views.append(_feature_view("batch_view").to_proto())
        registry_proto.stream_feature_views.append(
            _stream_feature_view("driver_dropoffs_stream").to_proto()
        )

        infra_objects = DynamoDBOnlineStore().plan(config, registry_proto)

        assert sorted(o.name for o in infra_objects) == [
            f"{PROJECT}.batch_view",
            f"{PROJECT}.driver_dropoffs_stream",
        ]

    def test_plan_empty_registry_produces_empty_plan(self):
        config = _repo_config()
        registry_proto = RegistryProto()

        assert DynamoDBOnlineStore().plan(config, registry_proto) == []


class TestDynamoDBTableProtoRoundTrip:
    """This is the mechanism `feast plan` relies on: Infra gets serialized to
    proto and deserialized back via the dotted class_type path, not a
    hardcoded per-store branch."""

    def test_infra_object_survives_proto_round_trip(self):
        original = DynamoDBTable(
            name=f"{PROJECT}.view_a", region=REGION, endpoint_url=None
        )
        infra = Infra(infra_objects=[original])

        restored = Infra.from_proto(infra.to_proto())

        assert len(restored.infra_objects) == 1
        restored_table = restored.infra_objects[0]
        assert isinstance(restored_table, DynamoDBTable)
        assert restored_table.name == f"{PROJECT}.view_a"
        assert restored_table.region == REGION

    def test_round_trip_preserves_endpoint_url(self):
        original = DynamoDBTable(
            name=f"{PROJECT}.view_a",
            region=REGION,
            endpoint_url="http://localhost:8000",
        )
        infra = Infra(infra_objects=[original])

        restored = Infra.from_proto(infra.to_proto())

        assert restored.infra_objects[0].endpoint_url == "http://localhost:8000"
