from datetime import timedelta

import pandas as pd

from feast.data_format import AvroFormat
from feast.data_source import KafkaSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStore, SqliteOnlineStoreConfig
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.transformation.pandas_transformation import PandasTransformation
from feast.types import Int64, String


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


def _on_demand_feature_view(
    name: str, source: FeatureView, write_to_online_store: bool
) -> OnDemandFeatureView:
    def transform(inputs: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"derived": inputs["value"]})

    return OnDemandFeatureView(
        name=name,
        entities=[Entity(name="driver", join_keys=["driver_id"])],
        sources=[source],
        schema=[Field(name="derived", dtype=Int64)],
        feature_transformation=PandasTransformation(
            udf=transform, udf_string="transform"
        ),
        write_to_online_store=write_to_online_store,
    )


class TestSqliteOnlineStorePlanWithOnDemandFeatureViews:
    """feast apply on the local provider goes through plan(), so on demand
    feature views with write_to_online_store=True must get a table here too,
    otherwise materialize / write_to_online_store fails with 'no such table'."""

    def test_plan_includes_on_demand_feature_views_with_writes(self):
        config = _repo_config()
        source = _feature_view("source_view")
        registry_proto = RegistryProto()
        registry_proto.feature_views.append(source.to_proto())
        registry_proto.on_demand_feature_views.append(
            _on_demand_feature_view(
                "odfv_on_write", source, write_to_online_store=True
            ).to_proto()
        )

        infra_objects = SqliteOnlineStore().plan(config, registry_proto)

        assert sorted(o.name for o in infra_objects) == [
            "test_project_odfv_on_write",
            "test_project_source_view",
        ]

    def test_plan_skips_on_demand_feature_views_without_writes(self):
        config = _repo_config()
        source = _feature_view("source_view")
        registry_proto = RegistryProto()
        registry_proto.feature_views.append(source.to_proto())
        registry_proto.on_demand_feature_views.append(
            _on_demand_feature_view(
                "odfv_on_read", source, write_to_online_store=False
            ).to_proto()
        )

        infra_objects = SqliteOnlineStore().plan(config, registry_proto)

        assert [o.name for o in infra_objects] == ["test_project_source_view"]
