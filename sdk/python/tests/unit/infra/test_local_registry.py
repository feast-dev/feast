# Copyright 2022 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from datetime import timedelta
from tempfile import mkstemp

import pandas as pd
import pytest
from pytest_lazyfixture import lazy_fixture

from feast import FileSource
from feast.aggregation import Aggregation
from feast.data_format import AvroFormat, ParquetFormat
from feast.data_source import KafkaSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.registry.registry import Registry
from feast.on_demand_feature_view import RequestSource, on_demand_feature_view
from feast.repo_config import RegistryConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import Array, Bytes, Float32, Int32, Int64, String
from feast.value_type import ValueType
from tests.integration.feature_repos.universal.entities import driver
from tests.utils.e2e_test_validation import validate_registry_data_source_apply


@pytest.fixture
def local_registry() -> Registry:
    fd, registry_path = mkstemp()
    registry_config = RegistryConfig(path=registry_path, cache_ttl_seconds=600)
    return Registry("project", registry_config, None)


@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("local_registry")],
)
def test_apply_entity_success(test_registry):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        tags={"team": "matchmaking"},
    )

    project = "project"

    # Register Entity
    test_registry.apply_entity(entity, project)

    entities = test_registry.list_entities(project)

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    entity = test_registry.get_entity("driver_car_id", project)
    assert (
        entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    test_registry.delete_entity("driver_car_id", project)
    entities = test_registry.list_entities(project)
    assert len(entities) == 0

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)


@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("local_registry")],
)
def test_apply_feature_view_success(test_registry):
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
    )

    entity = Entity(name="fs1_my_entity_1", join_keys=["test"])

    fv1 = FeatureView(
        name="my_feature_view_1",
        schema=[
            Field(name="fs1_my_feature_1", dtype=Int64),
            Field(name="fs1_my_feature_2", dtype=String),
            Field(name="fs1_my_feature_3", dtype=Array(String)),
            Field(name="fs1_my_feature_4", dtype=Array(Bytes)),
        ],
        entities=[entity],
        tags={"team": "matchmaking"},
        source=batch_source,
        ttl=timedelta(minutes=5),
    )

    project = "project"

    # Register Feature View
    test_registry.apply_feature_view(fv1, project)

    feature_views = test_registry.list_feature_views(project)

    # List Feature Views
    assert (
        len(feature_views) == 1
        and feature_views[0].name == "my_feature_view_1"
        and feature_views[0].features[0].name == "fs1_my_feature_1"
        and feature_views[0].features[0].dtype == Int64
        and feature_views[0].features[1].name == "fs1_my_feature_2"
        and feature_views[0].features[1].dtype == String
        and feature_views[0].features[2].name == "fs1_my_feature_3"
        and feature_views[0].features[2].dtype == Array(String)
        and feature_views[0].features[3].name == "fs1_my_feature_4"
        and feature_views[0].features[3].dtype == Array(Bytes)
        and feature_views[0].entities[0] == "fs1_my_entity_1"
    )

    feature_view = test_registry.get_feature_view("my_feature_view_1", project)
    assert (
        feature_view.name == "my_feature_view_1"
        and feature_view.features[0].name == "fs1_my_feature_1"
        and feature_view.features[0].dtype == Int64
        and feature_view.features[1].name == "fs1_my_feature_2"
        and feature_view.features[1].dtype == String
        and feature_view.features[2].name == "fs1_my_feature_3"
        and feature_view.features[2].dtype == Array(String)
        and feature_view.features[3].name == "fs1_my_feature_4"
        and feature_view.features[3].dtype == Array(Bytes)
        and feature_view.entities[0] == "fs1_my_entity_1"
    )

    test_registry.delete_feature_view("my_feature_view_1", project)
    feature_views = test_registry.list_feature_views(project)
    assert len(feature_views) == 0

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)


@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("local_registry")],
)
def test_apply_on_demand_feature_view_success(test_registry):
    # Create Feature Views
    driver_stats = FileSource(
        name="driver_stats_source",
        path="data/driver_stats_lat_lon.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
        description="A table describing the stats of a driver based on hourly logs",
        owner="test2@gmail.com",
    )

    driver_daily_features_view = FeatureView(
        name="driver_daily_features",
        entities=[driver()],
        ttl=timedelta(seconds=8640000000),
        schema=[
            Field(name="daily_miles_driven", dtype=Float32),
            Field(name="lat", dtype=Float32),
            Field(name="lon", dtype=Float32),
            Field(name="string_feature", dtype=String),
        ],
        online=True,
        source=driver_stats,
        tags={"production": "True"},
        owner="test2@gmail.com",
    )

    @on_demand_feature_view(
        sources=[driver_daily_features_view],
        schema=[Field(name="first_char", dtype=String)],
    )
    def location_features_from_push(inputs: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        df["first_char"] = inputs["string_feature"].str[:1].astype("string")
        return df

    project = "project"

    # Register Feature View
    test_registry.apply_feature_view(location_features_from_push, project)

    feature_views = test_registry.list_on_demand_feature_views(project)

    # List Feature Views
    assert (
        len(feature_views) == 1
        and feature_views[0].name == "location_features_from_push"
        and feature_views[0].features[0].name == "first_char"
        and feature_views[0].features[0].dtype == String
    )

    feature_view = test_registry.get_on_demand_feature_view(
        "location_features_from_push", project
    )
    assert (
        feature_view.name == "location_features_from_push"
        and feature_view.features[0].name == "first_char"
        and feature_view.features[0].dtype == String
    )

    test_registry.delete_feature_view("location_features_from_push", project)
    feature_views = test_registry.list_on_demand_feature_views(project)
    assert len(feature_views) == 0

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)


@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("local_registry")],
)
def test_apply_stream_feature_view_success(test_registry):
    # Create Feature Views
    def simple_udf(x: int):
        return x + 3

    entity = Entity(name="driver_entity", join_keys=["test_key"])

    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
        watermark_delay_threshold=timedelta(days=1),
    )

    sfv = StreamFeatureView(
        name="test kafka stream feature view",
        entities=[entity],
        ttl=timedelta(days=30),
        owner="test@example.com",
        online=True,
        schema=[Field(name="dummy_field", dtype=Float32)],
        description="desc",
        aggregations=[
            Aggregation(
                column="dummy_field",
                function="max",
                time_window=timedelta(days=1),
            ),
            Aggregation(
                column="dummy_field2",
                function="count",
                time_window=timedelta(days=24),
            ),
        ],
        timestamp_field="event_timestamp",
        mode="spark",
        source=stream_source,
        udf=simple_udf,
        tags={},
    )

    project = "project"

    # Register Feature View
    test_registry.apply_feature_view(sfv, project)

    stream_feature_views = test_registry.list_stream_feature_views(project)

    # List Feature Views
    assert len(stream_feature_views) == 1
    assert stream_feature_views[0] == sfv

    test_registry.delete_feature_view("test kafka stream feature view", project)
    stream_feature_views = test_registry.list_stream_feature_views(project)
    assert len(stream_feature_views) == 0

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)


@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("local_registry")],
)
def test_modify_feature_views_success(test_registry):
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
    )

    request_source = RequestSource(
        name="request_source",
        schema=[Field(name="my_input_1", dtype=Int32)],
    )

    entity = Entity(name="fs1_my_entity_1", join_keys=["test"])

    fv1 = FeatureView(
        name="my_feature_view_1",
        schema=[Field(name="fs1_my_feature_1", dtype=Int64)],
        entities=[entity],
        tags={"team": "matchmaking"},
        source=batch_source,
        ttl=timedelta(minutes=5),
    )

    @on_demand_feature_view(
        schema=[
            Field(name="odfv1_my_feature_1", dtype=String),
            Field(name="odfv1_my_feature_2", dtype=Int32),
        ],
        sources=[request_source],
    )
    def odfv1(feature_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["odfv1_my_feature_1"] = feature_df["my_input_1"].astype("category")
        data["odfv1_my_feature_2"] = feature_df["my_input_1"].astype("int32")
        return data

    project = "project"

    # Register Feature Views
    test_registry.apply_feature_view(odfv1, project)
    test_registry.apply_feature_view(fv1, project)

    # Modify odfv by changing a single feature dtype
    @on_demand_feature_view(
        schema=[
            Field(name="odfv1_my_feature_1", dtype=Float32),
            Field(name="odfv1_my_feature_2", dtype=Int32),
        ],
        sources=[request_source],
    )
    def odfv1(feature_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["odfv1_my_feature_1"] = feature_df["my_input_1"].astype("float")
        data["odfv1_my_feature_2"] = feature_df["my_input_1"].astype("int32")
        return data

    # Apply the modified odfv
    test_registry.apply_feature_view(odfv1, project)

    # Check odfv
    on_demand_feature_views = test_registry.list_on_demand_feature_views(project)

    assert (
        len(on_demand_feature_views) == 1
        and on_demand_feature_views[0].name == "odfv1"
        and on_demand_feature_views[0].features[0].name == "odfv1_my_feature_1"
        and on_demand_feature_views[0].features[0].dtype == Float32
        and on_demand_feature_views[0].features[1].name == "odfv1_my_feature_2"
        and on_demand_feature_views[0].features[1].dtype == Int32
    )
    request_schema = on_demand_feature_views[0].get_request_data_schema()
    assert (
        list(request_schema.keys())[0] == "my_input_1"
        and list(request_schema.values())[0] == ValueType.INT32
    )

    feature_view = test_registry.get_on_demand_feature_view("odfv1", project)
    assert (
        feature_view.name == "odfv1"
        and feature_view.features[0].name == "odfv1_my_feature_1"
        and feature_view.features[0].dtype == Float32
        and feature_view.features[1].name == "odfv1_my_feature_2"
        and feature_view.features[1].dtype == Int32
    )
    request_schema = feature_view.get_request_data_schema()
    assert (
        list(request_schema.keys())[0] == "my_input_1"
        and list(request_schema.values())[0] == ValueType.INT32
    )

    # Make sure fv1 is untouched
    feature_views = test_registry.list_feature_views(project)

    # List Feature Views
    assert (
        len(feature_views) == 1
        and feature_views[0].name == "my_feature_view_1"
        and feature_views[0].features[0].name == "fs1_my_feature_1"
        and feature_views[0].features[0].dtype == Int64
        and feature_views[0].entities[0] == "fs1_my_entity_1"
    )

    feature_view = test_registry.get_feature_view("my_feature_view_1", project)
    assert (
        feature_view.name == "my_feature_view_1"
        and feature_view.features[0].name == "fs1_my_feature_1"
        and feature_view.features[0].dtype == Int64
        and feature_view.entities[0] == "fs1_my_entity_1"
    )

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)


@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("local_registry")],
)
def test_apply_data_source(test_registry: Registry):
    validate_registry_data_source_apply(test_registry)


def test_commit():
    fd, registry_path = mkstemp()
    registry_config = RegistryConfig(path=registry_path, cache_ttl_seconds=600)
    test_registry = Registry("project", registry_config, None)

    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        tags={"team": "matchmaking"},
    )

    project = "project"

    # Register Entity without commiting
    test_registry.apply_entity(entity, project, commit=False)
    assert test_registry.cached_registry_proto
    assert len(test_registry.cached_registry_proto.project_metadata) == 1
    project_metadata = test_registry.cached_registry_proto.project_metadata[0]
    project_uuid = project_metadata.project_uuid
    assert len(project_uuid) == 36
    validate_project_uuid(project_uuid, test_registry)

    # Retrieving the entity should still succeed
    entities = test_registry.list_entities(project, allow_cache=True)
    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )
    validate_project_uuid(project_uuid, test_registry)

    entity = test_registry.get_entity("driver_car_id", project, allow_cache=True)
    assert (
        entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )
    validate_project_uuid(project_uuid, test_registry)

    # Create new registry that points to the same store
    registry_with_same_store = Registry("project", registry_config, None)

    # Retrieving the entity should fail since the store is empty
    entities = registry_with_same_store.list_entities(project)
    assert len(entities) == 0
    validate_project_uuid(project_uuid, registry_with_same_store)

    # commit from the original registry
    test_registry.commit()

    # Reconstruct the new registry in order to read the newly written store
    registry_with_same_store = Registry("project", registry_config, None)

    # Retrieving the entity should now succeed
    entities = registry_with_same_store.list_entities(project)
    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )
    validate_project_uuid(project_uuid, registry_with_same_store)

    entity = test_registry.get_entity("driver_car_id", project)
    assert (
        entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)


def validate_project_uuid(project_uuid, test_registry):
    assert len(test_registry.cached_registry_proto.project_metadata) == 1
    project_metadata = test_registry.cached_registry_proto.project_metadata[0]
    assert project_metadata.project_uuid == project_uuid
