# Copyright 2021 The Feast Authors
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
import logging
import os
import sys
from datetime import timedelta

import pandas as pd
import pytest
from pytest_lazyfixture import lazy_fixture
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from feast import FileSource, RequestSource
from feast.data_format import ParquetFormat
from feast.entity import Entity
from feast.errors import FeatureViewNotFoundException
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.infra_object import Infra
from feast.infra.online_stores.sqlite import SqliteTable
from feast.infra.registry.sql import SqlRegistry
from feast.on_demand_feature_view import on_demand_feature_view
from feast.repo_config import RegistryConfig
from feast.types import Array, Bytes, Float32, Int32, Int64, String
from feast.value_type import ValueType
from tests.integration.feature_repos.universal.entities import driver

POSTGRES_USER = "test"
POSTGRES_PASSWORD = "test"
POSTGRES_DB = "test"


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def pg_registry():
    container = (
        DockerContainer("postgres:latest")
        .with_exposed_ports(5432)
        .with_env("POSTGRES_USER", POSTGRES_USER)
        .with_env("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env("POSTGRES_DB", POSTGRES_DB)
    )

    container.start()

    log_string_to_wait_for = "database system is ready to accept connections"
    waited = wait_for_logs(
        container=container,
        predicate=log_string_to_wait_for,
        timeout=30,
        interval=10,
    )
    logger.info("Waited for %s seconds until postgres container was up", waited)
    container_port = container.get_exposed_port(5432)

    registry_config = RegistryConfig(
        registry_type="sql",
        path=f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{container_port}/{POSTGRES_DB}",
    )

    yield SqlRegistry(registry_config, "project", None)

    container.stop()


@pytest.fixture(scope="session")
def mysql_registry():
    container = (
        DockerContainer("mysql:latest")
        .with_exposed_ports(3306)
        .with_env("MYSQL_RANDOM_ROOT_PASSWORD", "true")
        .with_env("MYSQL_USER", POSTGRES_USER)
        .with_env("MYSQL_PASSWORD", POSTGRES_PASSWORD)
        .with_env("MYSQL_DATABASE", POSTGRES_DB)
    )

    container.start()

    # The log string uses '8.0.*' since the version might be changed as new Docker images are pushed.
    log_string_to_wait_for = "/usr/sbin/mysqld: ready for connections. Version: '(\d+(\.\d+){1,2})'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306"  # noqa: W605
    waited = wait_for_logs(
        container=container,
        predicate=log_string_to_wait_for,
        timeout=60,
        interval=10,
    )
    logger.info("Waited for %s seconds until mysql container was up", waited)
    container_port = container.get_exposed_port(3306)

    registry_config = RegistryConfig(
        registry_type="sql",
        path=f"mysql+mysqldb://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{container_port}/{POSTGRES_DB}",
    )

    yield SqlRegistry(registry_config, "project", None)

    container.stop()


@pytest.fixture(scope="session")
def sqlite_registry():
    registry_config = RegistryConfig(
        registry_type="sql",
        path="sqlite://",
    )

    yield SqlRegistry(registry_config, "project", None)


@pytest.mark.skipif(
    sys.platform == "darwin" and "GITHUB_REF" in os.environ,
    reason="does not run on mac github actions",
)
@pytest.mark.parametrize(
    "sql_registry",
    [
        lazy_fixture("mysql_registry"),
        lazy_fixture("pg_registry"),
        lazy_fixture("sqlite_registry"),
    ],
)
def test_apply_entity_success(sql_registry):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        tags={"team": "matchmaking"},
    )

    project = "project"

    # Register Entity
    sql_registry.apply_entity(entity, project)
    project_metadata = sql_registry.list_project_metadata(project=project)
    assert len(project_metadata) == 1
    project_uuid = project_metadata[0].project_uuid
    assert len(project_metadata[0].project_uuid) == 36
    assert_project_uuid(project, project_uuid, sql_registry)

    entities = sql_registry.list_entities(project)
    assert_project_uuid(project, project_uuid, sql_registry)

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    entity = sql_registry.get_entity("driver_car_id", project)
    assert (
        entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    # After the first apply, the created_timestamp should be the same as the last_update_timestamp.
    assert entity.created_timestamp == entity.last_updated_timestamp

    sql_registry.delete_entity("driver_car_id", project)
    assert_project_uuid(project, project_uuid, sql_registry)
    entities = sql_registry.list_entities(project)
    assert_project_uuid(project, project_uuid, sql_registry)
    assert len(entities) == 0

    sql_registry.teardown()


def assert_project_uuid(project, project_uuid, sql_registry):
    project_metadata = sql_registry.list_project_metadata(project=project)
    assert len(project_metadata) == 1
    assert project_metadata[0].project_uuid == project_uuid


@pytest.mark.skipif(
    sys.platform == "darwin" and "GITHUB_REF" in os.environ,
    reason="does not run on mac github actions",
)
@pytest.mark.parametrize(
    "sql_registry",
    [
        lazy_fixture("mysql_registry"),
        lazy_fixture("pg_registry"),
        lazy_fixture("sqlite_registry"),
    ],
)
def test_apply_feature_view_success(sql_registry):
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
    sql_registry.apply_feature_view(fv1, project)

    feature_views = sql_registry.list_feature_views(project)

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

    feature_view = sql_registry.get_feature_view("my_feature_view_1", project)
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
    assert feature_view.ttl == timedelta(minutes=5)

    # After the first apply, the created_timestamp should be the same as the last_update_timestamp.
    assert feature_view.created_timestamp == feature_view.last_updated_timestamp

    # Modify the feature view and apply again to test if diffing the online store table works
    fv1.ttl = timedelta(minutes=6)
    sql_registry.apply_feature_view(fv1, project)
    feature_views = sql_registry.list_feature_views(project)
    assert len(feature_views) == 1
    feature_view = sql_registry.get_feature_view("my_feature_view_1", project)
    assert feature_view.ttl == timedelta(minutes=6)

    # Delete feature view
    sql_registry.delete_feature_view("my_feature_view_1", project)
    feature_views = sql_registry.list_feature_views(project)
    assert len(feature_views) == 0

    sql_registry.teardown()


@pytest.mark.skipif(
    sys.platform == "darwin" and "GITHUB_REF" in os.environ,
    reason="does not run on mac github actions",
)
@pytest.mark.parametrize(
    "sql_registry",
    [
        lazy_fixture("mysql_registry"),
        lazy_fixture("pg_registry"),
        lazy_fixture("sqlite_registry"),
    ],
)
def test_apply_on_demand_feature_view_success(sql_registry):
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

    with pytest.raises(FeatureViewNotFoundException):
        sql_registry.get_user_metadata(project, location_features_from_push)

    # Register Feature View
    sql_registry.apply_feature_view(location_features_from_push, project)

    assert not sql_registry.get_user_metadata(project, location_features_from_push)

    b = "metadata".encode("utf-8")
    sql_registry.apply_user_metadata(project, location_features_from_push, b)
    assert sql_registry.get_user_metadata(project, location_features_from_push) == b

    feature_views = sql_registry.list_on_demand_feature_views(project)

    # List Feature Views
    assert (
        len(feature_views) == 1
        and feature_views[0].name == "location_features_from_push"
        and feature_views[0].features[0].name == "first_char"
        and feature_views[0].features[0].dtype == String
    )

    feature_view = sql_registry.get_on_demand_feature_view(
        "location_features_from_push", project
    )
    assert (
        feature_view.name == "location_features_from_push"
        and feature_view.features[0].name == "first_char"
        and feature_view.features[0].dtype == String
    )

    sql_registry.delete_feature_view("location_features_from_push", project)
    feature_views = sql_registry.list_on_demand_feature_views(project)
    assert len(feature_views) == 0

    sql_registry.teardown()


@pytest.mark.skipif(
    sys.platform == "darwin" and "GITHUB_REF" in os.environ,
    reason="does not run on mac github actions",
)
@pytest.mark.parametrize(
    "sql_registry",
    [
        lazy_fixture("mysql_registry"),
        lazy_fixture("pg_registry"),
        lazy_fixture("sqlite_registry"),
    ],
)
def test_modify_feature_views_success(sql_registry):
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
    sql_registry.apply_feature_view(odfv1, project)
    sql_registry.apply_feature_view(fv1, project)

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
    sql_registry.apply_feature_view(odfv1, project)

    # Check odfv
    on_demand_feature_views = sql_registry.list_on_demand_feature_views(project)

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

    feature_view = sql_registry.get_on_demand_feature_view("odfv1", project)
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
    feature_views = sql_registry.list_feature_views(project)

    # List Feature Views
    assert (
        len(feature_views) == 1
        and feature_views[0].name == "my_feature_view_1"
        and feature_views[0].features[0].name == "fs1_my_feature_1"
        and feature_views[0].features[0].dtype == Int64
        and feature_views[0].entities[0] == "fs1_my_entity_1"
    )

    feature_view = sql_registry.get_feature_view("my_feature_view_1", project)
    assert (
        feature_view.name == "my_feature_view_1"
        and feature_view.features[0].name == "fs1_my_feature_1"
        and feature_view.features[0].dtype == Int64
        and feature_view.entities[0] == "fs1_my_entity_1"
    )

    sql_registry.teardown()


@pytest.mark.skipif(
    sys.platform == "darwin" and "GITHUB_REF" in os.environ,
    reason="does not run on mac github actions",
)
@pytest.mark.parametrize(
    "sql_registry",
    [
        lazy_fixture("mysql_registry"),
        lazy_fixture("pg_registry"),
        lazy_fixture("sqlite_registry"),
    ],
)
def test_apply_data_source(sql_registry):
    # Create Feature Views
    batch_source = FileSource(
        name="test_source",
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

    # Register data source and feature view
    sql_registry.apply_data_source(batch_source, project, commit=False)
    sql_registry.apply_feature_view(fv1, project, commit=True)

    registry_feature_views = sql_registry.list_feature_views(project)
    registry_data_sources = sql_registry.list_data_sources(project)
    assert len(registry_feature_views) == 1
    assert len(registry_data_sources) == 1
    registry_feature_view = registry_feature_views[0]
    assert registry_feature_view.batch_source == batch_source
    registry_data_source = registry_data_sources[0]
    assert registry_data_source == batch_source

    # Check that change to batch source propagates
    batch_source.timestamp_field = "new_ts_col"
    sql_registry.apply_data_source(batch_source, project, commit=False)
    sql_registry.apply_feature_view(fv1, project, commit=True)
    registry_feature_views = sql_registry.list_feature_views(project)
    registry_data_sources = sql_registry.list_data_sources(project)
    assert len(registry_feature_views) == 1
    assert len(registry_data_sources) == 1
    registry_feature_view = registry_feature_views[0]
    assert registry_feature_view.batch_source == batch_source
    registry_batch_source = sql_registry.list_data_sources(project)[0]
    assert registry_batch_source == batch_source

    sql_registry.teardown()


@pytest.mark.skipif(
    sys.platform == "darwin" and "GITHUB_REF" in os.environ,
    reason="does not run on mac github actions",
)
@pytest.mark.parametrize(
    "sql_registry",
    [
        lazy_fixture("mysql_registry"),
        lazy_fixture("pg_registry"),
        lazy_fixture("sqlite_registry"),
    ],
)
def test_registry_cache(sql_registry):
    # Create Feature Views
    batch_source = FileSource(
        name="test_source",
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

    # Register data source and feature view
    sql_registry.apply_data_source(batch_source, project)
    sql_registry.apply_feature_view(fv1, project)
    registry_feature_views_cached = sql_registry.list_feature_views(
        project, allow_cache=True
    )
    registry_data_sources_cached = sql_registry.list_data_sources(
        project, allow_cache=True
    )
    # Not refreshed cache, so cache miss
    assert len(registry_feature_views_cached) == 0
    assert len(registry_data_sources_cached) == 0
    sql_registry.refresh(project)
    # Now objects exist
    registry_feature_views_cached = sql_registry.list_feature_views(
        project, allow_cache=True
    )
    registry_data_sources_cached = sql_registry.list_data_sources(
        project, allow_cache=True
    )
    assert len(registry_feature_views_cached) == 1
    assert len(registry_data_sources_cached) == 1
    registry_feature_view = registry_feature_views_cached[0]
    assert registry_feature_view.batch_source == batch_source
    registry_data_source = registry_data_sources_cached[0]
    assert registry_data_source == batch_source

    sql_registry.teardown()


@pytest.mark.skipif(
    sys.platform == "darwin" and "GITHUB_REF" in os.environ,
    reason="does not run on mac github actions",
)
@pytest.mark.parametrize(
    "sql_registry",
    [
        lazy_fixture("mysql_registry"),
        lazy_fixture("pg_registry"),
        lazy_fixture("sqlite_registry"),
    ],
)
def test_update_infra(sql_registry):
    # Create infra object
    project = "project"
    infra = sql_registry.get_infra(project=project)

    assert len(infra.infra_objects) == 0

    # Should run update infra successfully
    sql_registry.update_infra(infra, project)

    # Should run update infra successfully when adding
    new_infra = Infra()
    new_infra.infra_objects.append(
        SqliteTable(
            path="/tmp/my_path.db",
            name="my_table",
        )
    )
    sql_registry.update_infra(new_infra, project)
    infra = sql_registry.get_infra(project=project)
    assert len(infra.infra_objects) == 1

    # Try again since second time, infra should be not-empty
    sql_registry.teardown()
