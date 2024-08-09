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
import time
from datetime import timedelta
from tempfile import mkstemp
from unittest import mock

import grpc_testing
import pandas as pd
import pytest
from pytest_lazyfixture import lazy_fixture
from pytz import utc
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.minio import MinioContainer
from testcontainers.mysql import MySqlContainer

from feast import FeatureService, FileSource, RequestSource
from feast.data_format import AvroFormat, ParquetFormat
from feast.data_source import KafkaSource
from feast.entity import Entity
from feast.errors import FeatureViewNotFoundException
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.infra_object import Infra
from feast.infra.online_stores.sqlite import SqliteTable
from feast.infra.registry.registry import Registry
from feast.infra.registry.remote import RemoteRegistry, RemoteRegistryConfig
from feast.infra.registry.sql import SqlRegistry
from feast.on_demand_feature_view import on_demand_feature_view
from feast.permissions.action import AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.protos.feast.registry import RegistryServer_pb2, RegistryServer_pb2_grpc
from feast.registry_server import RegistryServer
from feast.repo_config import RegistryConfig
from feast.stream_feature_view import Aggregation, StreamFeatureView
from feast.types import Array, Bytes, Float32, Int32, Int64, String
from feast.utils import _utc_now
from feast.value_type import ValueType
from tests.integration.feature_repos.universal.entities import driver


@pytest.fixture
def local_registry() -> Registry:
    fd, registry_path = mkstemp()
    registry_config = RegistryConfig(path=registry_path, cache_ttl_seconds=600)
    return Registry("project", registry_config, None)


@pytest.fixture
def gcs_registry() -> Registry:
    from google.cloud import storage

    storage_client = storage.Client()
    bucket_name = f"feast-registry-test-{int(time.time() * 1000)}"
    bucket = storage_client.bucket(bucket_name)
    bucket = storage_client.create_bucket(bucket)
    bucket.add_lifecycle_delete_rule(
        age=14
    )  # delete buckets automatically after 14 days
    bucket.patch()
    bucket.blob("registry.db")
    registry_config = RegistryConfig(
        path=f"gs://{bucket_name}/registry.db", cache_ttl_seconds=600
    )
    return Registry("project", registry_config, None)


@pytest.fixture
def s3_registry() -> Registry:
    aws_registry_path = os.getenv(
        "AWS_REGISTRY_PATH", "s3://feast-int-bucket/registries"
    )
    registry_config = RegistryConfig(
        path=f"{aws_registry_path}/{int(time.time() * 1000)}/registry.db",
        cache_ttl_seconds=600,
    )
    return Registry("project", registry_config, None)


@pytest.fixture(scope="session")
def minio_registry() -> Registry:
    bucket_name = "test-bucket"

    container = MinioContainer()
    container.start()
    client = container.get_client()
    client.make_bucket(bucket_name)

    container_host = container.get_container_host_ip()
    exposed_port = container.get_exposed_port(container.port)

    registry_config = RegistryConfig(
        path=f"s3://{bucket_name}/registry.db", cache_ttl_seconds=600
    )

    mock_environ = {
        "FEAST_S3_ENDPOINT_URL": f"http://{container_host}:{exposed_port}",
        "AWS_ACCESS_KEY_ID": container.access_key,
        "AWS_SECRET_ACCESS_KEY": container.secret_key,
        "AWS_SESSION_TOKEN": "",
    }

    with mock.patch.dict(os.environ, mock_environ):
        yield Registry("project", registry_config, None)

    container.stop()


POSTGRES_USER = "test"
POSTGRES_PASSWORD = "test"
POSTGRES_DB = "test"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def pg_registry():
    container = (
        DockerContainer("postgres:latest")
        .with_exposed_ports(5432)
        .with_env("POSTGRES_USER", POSTGRES_USER)
        .with_env("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env("POSTGRES_DB", POSTGRES_DB)
    )

    container.start()

    registry_config = _given_registry_config_for_pg_sql(container)

    yield SqlRegistry(registry_config, "project", None)

    container.stop()


@pytest.fixture(scope="function")
def pg_registry_async():
    container = (
        DockerContainer("postgres:latest")
        .with_exposed_ports(5432)
        .with_env("POSTGRES_USER", POSTGRES_USER)
        .with_env("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env("POSTGRES_DB", POSTGRES_DB)
    )

    container.start()

    registry_config = _given_registry_config_for_pg_sql(container, 2, "thread")

    yield SqlRegistry(registry_config, "project", None)

    container.stop()


def _given_registry_config_for_pg_sql(
    container, cache_ttl_seconds=2, cache_mode="sync"
):
    log_string_to_wait_for = "database system is ready to accept connections"
    waited = wait_for_logs(
        container=container,
        predicate=log_string_to_wait_for,
        timeout=30,
        interval=10,
    )
    logger.info("Waited for %s seconds until postgres container was up", waited)
    container_port = container.get_exposed_port(5432)
    container_host = container.get_container_host_ip()

    return RegistryConfig(
        registry_type="sql",
        cache_ttl_seconds=cache_ttl_seconds,
        cache_mode=cache_mode,
        # The `path` must include `+psycopg` in order for `sqlalchemy.create_engine()`
        # to understand that we are using psycopg3.
        path=f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{container_host}:{container_port}/{POSTGRES_DB}",
        sqlalchemy_config_kwargs={"echo": False, "pool_pre_ping": True},
    )


@pytest.fixture(scope="function")
def mysql_registry():
    container = MySqlContainer("mysql:latest")
    container.start()

    registry_config = _given_registry_config_for_mysql(container)

    yield SqlRegistry(registry_config, "project", None)

    container.stop()


@pytest.fixture(scope="function")
def mysql_registry_async():
    container = MySqlContainer("mysql:latest")
    container.start()

    registry_config = _given_registry_config_for_mysql(container, 2, "thread")

    yield SqlRegistry(registry_config, "project", None)

    container.stop()


def _given_registry_config_for_mysql(container, cache_ttl_seconds=2, cache_mode="sync"):
    import sqlalchemy

    engine = sqlalchemy.create_engine(
        container.get_connection_url(), pool_pre_ping=True
    )
    engine.connect()

    return RegistryConfig(
        registry_type="sql",
        path=container.get_connection_url(),
        cache_ttl_seconds=cache_ttl_seconds,
        cache_mode=cache_mode,
        sqlalchemy_config_kwargs={"echo": False, "pool_pre_ping": True},
    )


@pytest.fixture(scope="session")
def sqlite_registry():
    registry_config = RegistryConfig(
        registry_type="sql",
        path="sqlite://",
    )

    yield SqlRegistry(registry_config, "project", None)


class GrpcMockChannel:
    def __init__(self, service, servicer):
        self.service = service
        self.test_server = grpc_testing.server_from_dictionary(
            {service: servicer},
            grpc_testing.strict_real_time(),
        )

    def unary_unary(
        self, method: str, request_serializer=None, response_deserializer=None
    ):
        method_name = method.split("/")[-1]
        method_descriptor = self.service.methods_by_name[method_name]

        def handler(request):
            rpc = self.test_server.invoke_unary_unary(
                method_descriptor, (), request, None
            )

            response, trailing_metadata, code, details = rpc.termination()
            return response

        return handler


@pytest.fixture
def mock_remote_registry():
    fd, registry_path = mkstemp()
    registry_config = RegistryConfig(path=registry_path, cache_ttl_seconds=600)
    proxied_registry = Registry("project", registry_config, None)

    registry = RemoteRegistry(
        registry_config=RemoteRegistryConfig(path=""),
        project=None,
        repo_path=None,
    )
    mock_channel = GrpcMockChannel(
        RegistryServer_pb2.DESCRIPTOR.services_by_name["RegistryServer"],
        RegistryServer(registry=proxied_registry),
    )
    registry.stub = RegistryServer_pb2_grpc.RegistryServerStub(mock_channel)
    yield registry


if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "False":
    all_fixtures = [lazy_fixture("s3_registry"), lazy_fixture("gcs_registry")]
else:
    all_fixtures = [
        lazy_fixture("local_registry"),
        pytest.param(
            lazy_fixture("minio_registry"),
            marks=pytest.mark.xdist_group(name="minio_registry"),
        ),
        pytest.param(
            lazy_fixture("pg_registry"),
            marks=pytest.mark.xdist_group(name="pg_registry"),
        ),
        pytest.param(
            lazy_fixture("mysql_registry"),
            marks=pytest.mark.xdist_group(name="mysql_registry"),
        ),
        lazy_fixture("sqlite_registry"),
        lazy_fixture("mock_remote_registry"),
    ]

sql_fixtures = [
    pytest.param(
        lazy_fixture("pg_registry"), marks=pytest.mark.xdist_group(name="pg_registry")
    ),
    pytest.param(
        lazy_fixture("mysql_registry"),
        marks=pytest.mark.xdist_group(name="mysql_registry"),
    ),
    lazy_fixture("sqlite_registry"),
]

async_sql_fixtures = [
    pytest.param(
        lazy_fixture("pg_registry_async"),
        marks=pytest.mark.xdist_group(name="pg_registry_async"),
    ),
    pytest.param(
        lazy_fixture("mysql_registry_async"),
        marks=pytest.mark.xdist_group(name="mysql_registry_async"),
    ),
]


@pytest.mark.integration
@pytest.mark.parametrize("test_registry", all_fixtures)
def test_apply_entity_success(test_registry):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        tags={"team": "matchmaking"},
    )

    project = "project"

    # Register Entity
    test_registry.apply_entity(entity, project)
    project_metadata = test_registry.list_project_metadata(project=project)
    assert len(project_metadata) == 1
    project_uuid = project_metadata[0].project_uuid
    assert len(project_metadata[0].project_uuid) == 36
    assert_project_uuid(project, project_uuid, test_registry)

    entities = test_registry.list_entities(project, tags=entity.tags)
    assert_project_uuid(project, project_uuid, test_registry)

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

    # After the first apply, the created_timestamp should be the same as the last_update_timestamp.
    assert entity.created_timestamp == entity.last_updated_timestamp

    # Update entity
    updated_entity = Entity(
        name="driver_car_id",
        description="Car driver Id",
        tags={"team": "matchmaking"},
    )
    test_registry.apply_entity(updated_entity, project)

    updated_entity = test_registry.get_entity("driver_car_id", project)

    # The created_timestamp for the entity should be set to the created_timestamp value stored from the previous apply
    assert (
        updated_entity.created_timestamp is not None
        and updated_entity.created_timestamp == entity.created_timestamp
    )

    test_registry.delete_entity("driver_car_id", project)
    assert_project_uuid(project, project_uuid, test_registry)
    entities = test_registry.list_entities(project)
    assert_project_uuid(project, project_uuid, test_registry)
    assert len(entities) == 0

    test_registry.teardown()


def assert_project_uuid(project, project_uuid, test_registry):
    project_metadata = test_registry.list_project_metadata(project=project)
    assert len(project_metadata) == 1
    assert project_metadata[0].project_uuid == project_uuid


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    all_fixtures,
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
            Field(name="test", dtype=Int64),
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

    feature_views = test_registry.list_feature_views(project, tags=fv1.tags)

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
    assert feature_view.ttl == timedelta(minutes=5)

    # After the first apply, the created_timestamp should be the same as the last_update_timestamp.
    assert feature_view.created_timestamp == feature_view.last_updated_timestamp

    # Modify the feature view and apply again to test if diffing the online store table works
    fv1.ttl = timedelta(minutes=6)
    test_registry.apply_feature_view(fv1, project)
    feature_views = test_registry.list_feature_views(project)
    assert len(feature_views) == 1
    feature_view = test_registry.get_feature_view("my_feature_view_1", project)
    assert feature_view.ttl == timedelta(minutes=6)

    # Delete feature view
    test_registry.delete_feature_view("my_feature_view_1", project)
    feature_views = test_registry.list_feature_views(project)
    assert len(feature_views) == 0

    test_registry.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    sql_fixtures,
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
            Field(name="driver_id", dtype=Int64),
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
        test_registry.get_user_metadata(project, location_features_from_push)

    # Register Feature View
    test_registry.apply_feature_view(location_features_from_push, project)

    assert not test_registry.get_user_metadata(project, location_features_from_push)

    b = "metadata".encode("utf-8")
    test_registry.apply_user_metadata(project, location_features_from_push, b)
    assert test_registry.get_user_metadata(project, location_features_from_push) == b

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


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    all_fixtures,
)
def test_apply_data_source(test_registry):
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
            Field(name="test", dtype=Int64),
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
    test_registry.apply_data_source(batch_source, project, commit=False)
    test_registry.apply_feature_view(fv1, project, commit=True)

    registry_feature_views = test_registry.list_feature_views(project, tags=fv1.tags)
    registry_data_sources = test_registry.list_data_sources(project)
    assert len(registry_feature_views) == 1
    assert len(registry_data_sources) == 1
    registry_feature_view = registry_feature_views[0]
    assert registry_feature_view.batch_source == batch_source
    registry_data_source = registry_data_sources[0]
    assert registry_data_source == batch_source

    # Check that change to batch source propagates
    batch_source.timestamp_field = "new_ts_col"
    test_registry.apply_data_source(batch_source, project, commit=False)
    test_registry.apply_feature_view(fv1, project, commit=True)
    registry_feature_views = test_registry.list_feature_views(project, tags=fv1.tags)
    registry_data_sources = test_registry.list_data_sources(project)
    assert len(registry_feature_views) == 1
    assert len(registry_data_sources) == 1
    registry_feature_view = registry_feature_views[0]
    assert registry_feature_view.batch_source == batch_source
    registry_batch_source = test_registry.list_data_sources(project)[0]
    assert registry_batch_source == batch_source

    test_registry.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    all_fixtures,
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
        schema=[
            Field(name="test", dtype=Int64),
            Field(name="fs1_my_feature_1", dtype=Int64),
        ],
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

    def simple_udf(x: int):
        return x + 3

    entity_sfv = Entity(name="sfv_my_entity_1", join_keys=["test_key"])

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
        entities=[entity_sfv],
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

    # Register Feature Views
    test_registry.apply_feature_view(odfv1, project)
    test_registry.apply_feature_view(fv1, project)
    test_registry.apply_feature_view(sfv, project)

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

    existing_odfv = test_registry.get_on_demand_feature_view("odfv1", project)

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

    assert (
        feature_view.created_timestamp is not None
        and feature_view.created_timestamp == existing_odfv.created_timestamp
    )

    # Make sure fv1 is untouched
    feature_views = test_registry.list_feature_views(project, tags=fv1.tags)

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

    # Simulate materialization
    current_date = _utc_now()
    end_date = current_date.replace(tzinfo=utc)
    start_date = (current_date - timedelta(days=1)).replace(tzinfo=utc)
    test_registry.apply_materialization(feature_view, project, start_date, end_date)
    materialized_feature_view = test_registry.get_feature_view(
        "my_feature_view_1", project
    )

    # Check if created_timestamp, along with materialized_intervals are updated
    assert (
        materialized_feature_view.created_timestamp is not None
        and materialized_feature_view.created_timestamp
        == feature_view.created_timestamp
        and len(materialized_feature_view.materialization_intervals) > 0
        and materialized_feature_view.materialization_intervals[0][0] == start_date
        and materialized_feature_view.materialization_intervals[0][1] == end_date
    )

    # Modify fv1 by changing a single dtype
    updated_fv1 = FeatureView(
        name="my_feature_view_1",
        schema=[
            Field(name="test", dtype=Int64),
            Field(name="fs1_my_feature_1", dtype=String),
        ],
        entities=[entity],
        tags={"team": "matchmaking"},
        source=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Check that these fields are empty before apply
    assert updated_fv1.created_timestamp is None
    assert len(updated_fv1.materialization_intervals) == 0

    # Apply the modified fv1
    test_registry.apply_feature_view(updated_fv1, project)

    # Verify feature view after modification
    updated_feature_views = test_registry.list_feature_views(project)

    # List Feature Views
    assert (
        len(updated_feature_views) == 1
        and updated_feature_views[0].name == "my_feature_view_1"
        and updated_feature_views[0].features[0].name == "fs1_my_feature_1"
        and updated_feature_views[0].features[0].dtype == String
        and updated_feature_views[0].entities[0] == "fs1_my_entity_1"
    )

    updated_feature_view = test_registry.get_feature_view("my_feature_view_1", project)
    assert (
        updated_feature_view.name == "my_feature_view_1"
        and updated_feature_view.features[0].name == "fs1_my_feature_1"
        and updated_feature_view.features[0].dtype == String
        and updated_feature_view.entities[0] == "fs1_my_entity_1"
    )

    # Check if materialization_intervals and created_timestamp values propagates on each apply
    # materialization_intervals will populate only when it's empty
    assert (
        updated_feature_view.created_timestamp is not None
        and updated_feature_view.created_timestamp == feature_view.created_timestamp
        and len(updated_feature_view.materialization_intervals) == 1
        and updated_feature_view.materialization_intervals[0][0] == start_date
        and updated_feature_view.materialization_intervals[0][1] == end_date
    )

    # Simulate materialization a second time
    current_date = _utc_now()
    end_date_1 = current_date.replace(tzinfo=utc)
    start_date_1 = (current_date - timedelta(days=1)).replace(tzinfo=utc)
    test_registry.apply_materialization(
        updated_feature_view, project, start_date_1, end_date_1
    )
    materialized_feature_view_1 = test_registry.get_feature_view(
        "my_feature_view_1", project
    )

    assert (
        materialized_feature_view_1.created_timestamp is not None
        and materialized_feature_view_1.created_timestamp
        == feature_view.created_timestamp
        and len(materialized_feature_view_1.materialization_intervals) == 2
        and materialized_feature_view_1.materialization_intervals[0][0] == start_date
        and materialized_feature_view_1.materialization_intervals[0][1] == end_date
        and materialized_feature_view_1.materialization_intervals[1][0] == start_date_1
        and materialized_feature_view_1.materialization_intervals[1][1] == end_date_1
    )

    # Modify sfv by changing the dtype

    sfv = StreamFeatureView(
        name="test kafka stream feature view",
        entities=[entity_sfv],
        ttl=timedelta(days=30),
        owner="test@example.com",
        online=True,
        schema=[Field(name="dummy_field", dtype=String)],
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

    existing_sfv = test_registry.get_stream_feature_view(
        "test kafka stream feature view", project
    )
    # Apply the modified sfv
    test_registry.apply_feature_view(sfv, project)

    # Verify feature view after modification
    updated_stream_feature_views = test_registry.list_stream_feature_views(project)

    # List Feature Views
    assert (
        len(updated_stream_feature_views) == 1
        and updated_stream_feature_views[0].name == "test kafka stream feature view"
        and updated_stream_feature_views[0].features[0].name == "dummy_field"
        and updated_stream_feature_views[0].features[0].dtype == String
        and updated_stream_feature_views[0].entities[0] == "sfv_my_entity_1"
    )

    updated_sfv = test_registry.get_stream_feature_view(
        "test kafka stream feature view", project
    )
    assert (
        updated_sfv.name == "test kafka stream feature view"
        and updated_sfv.features[0].name == "dummy_field"
        and updated_sfv.features[0].dtype == String
        and updated_sfv.entities[0] == "sfv_my_entity_1"
    )

    # The created_timestamp for the stream feature view should be set to the created_timestamp value stored from the
    # previous apply
    # Materialization_intervals is not set
    assert (
        updated_sfv.created_timestamp is not None
        and updated_sfv.created_timestamp == existing_sfv.created_timestamp
        and len(updated_sfv.materialization_intervals) == 0
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    sql_fixtures,
)
def test_update_infra(test_registry):
    # Create infra object
    project = "project"
    infra = test_registry.get_infra(project=project)

    assert len(infra.infra_objects) == 0

    # Should run update infra successfully
    test_registry.update_infra(infra, project)

    # Should run update infra successfully when adding
    new_infra = Infra()
    new_infra.infra_objects.append(
        SqliteTable(
            path="/tmp/my_path.db",
            name="my_table",
        )
    )
    test_registry.update_infra(new_infra, project)
    infra = test_registry.get_infra(project=project)
    assert len(infra.infra_objects) == 1

    # Try again since second time, infra should be not-empty
    test_registry.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    sql_fixtures,
)
def test_registry_cache(test_registry):
    # Create Feature Views
    batch_source = FileSource(
        name="test_source",
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
        tags={"team": "matchmaking"},
    )

    entity = Entity(name="fs1_my_entity_1", join_keys=["test"])

    fv1 = FeatureView(
        name="my_feature_view_1",
        schema=[
            Field(name="test", dtype=Int64),
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
    test_registry.apply_data_source(batch_source, project)
    test_registry.apply_feature_view(fv1, project)
    registry_feature_views_cached = test_registry.list_feature_views(
        project, allow_cache=True
    )
    registry_data_sources_cached = test_registry.list_data_sources(
        project, allow_cache=True
    )
    # Not refreshed cache, so cache miss
    assert len(registry_feature_views_cached) == 0
    assert len(registry_data_sources_cached) == 0
    test_registry.refresh(project)
    # Now objects exist
    registry_feature_views_cached = test_registry.list_feature_views(
        project, allow_cache=True, tags=fv1.tags
    )
    registry_data_sources_cached = test_registry.list_data_sources(
        project, allow_cache=True, tags=batch_source.tags
    )
    assert len(registry_feature_views_cached) == 1
    assert len(registry_data_sources_cached) == 1
    registry_feature_view = registry_feature_views_cached[0]
    assert registry_feature_view.batch_source == batch_source
    registry_data_source = registry_data_sources_cached[0]
    assert registry_data_source == batch_source

    test_registry.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    async_sql_fixtures,
)
def test_registry_cache_thread_async(test_registry):
    # Create Feature View
    batch_source = FileSource(
        name="test_source",
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
    )

    project = "project"

    # Register data source
    test_registry.apply_data_source(batch_source, project)
    registry_data_sources_cached = test_registry.list_data_sources(
        project, allow_cache=True
    )
    # async ttl yet to expire, so there will be a cache miss
    assert len(registry_data_sources_cached) == 0

    # Wait for cache to be refreshed
    time.sleep(4)
    # Now objects exist
    registry_data_sources_cached = test_registry.list_data_sources(
        project, allow_cache=True
    )
    assert len(registry_data_sources_cached) == 1
    registry_data_source = registry_data_sources_cached[0]
    assert registry_data_source == batch_source

    test_registry.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    all_fixtures,
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
        tags={"team": "matchmaking"},
    )

    project = "project"

    # Register Stream Feature View
    test_registry.apply_feature_view(sfv, project)

    stream_feature_views = test_registry.list_stream_feature_views(
        project, tags=sfv.tags
    )

    # List Feature Views
    assert len(stream_feature_views) == 1
    assert stream_feature_views[0] == sfv

    test_registry.delete_feature_view("test kafka stream feature view", project)
    stream_feature_views = test_registry.list_stream_feature_views(
        project, tags=sfv.tags
    )
    assert len(stream_feature_views) == 0

    test_registry.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    all_fixtures,
)
def test_apply_feature_service_success(test_registry):
    # Create Feature Service
    file_source = FileSource(name="my_file_source", path="test.parquet")
    feature_view = FeatureView(
        name="my_feature_view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    fs = FeatureService(
        name="my_feature_service_1", features=[feature_view[["feature1", "feature2"]]]
    )
    project = "project"

    # Register Feature Service
    test_registry.apply_feature_service(fs, project)

    feature_services = test_registry.list_feature_services(project)

    # List Feature Services
    assert len(feature_services) == 1
    assert feature_services[0] == fs

    # Delete Feature Service
    test_registry.delete_feature_service("my_feature_service_1", project)
    feature_services = test_registry.list_feature_services(project)
    assert len(feature_services) == 0

    test_registry.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    all_fixtures,
)
def test_modify_feature_service_success(test_registry):
    # Create Feature Service
    file_source = FileSource(name="my_file_source", path="test.parquet")
    feature_view = FeatureView(
        name="my_feature_view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    fs = FeatureService(
        name="my_feature_service_1", features=[feature_view[["feature1", "feature2"]]]
    )
    project = "project"

    # Register Feature service
    test_registry.apply_feature_service(fs, project)

    feature_services = test_registry.list_feature_services(project)

    # List Feature Services
    assert len(feature_services) == 1
    assert feature_services[0] == fs

    # Modify Feature Service by removing a feature
    fs = FeatureService(
        name="my_feature_service_1", features=[feature_view[["feature1"]]]
    )

    # Apply modified Feature Service
    test_registry.apply_feature_service(fs, project)

    updated_feature_services = test_registry.list_feature_services(project)

    # Verify Feature Services
    assert len(updated_feature_services) == 1
    assert updated_feature_services[0] == fs
    # The created_timestamp for the feature service should be set to the created_timestamp value stored from the
    # previous apply
    assert (
        updated_feature_services[0].created_timestamp is not None
        and updated_feature_services[0].created_timestamp
        == feature_services[0].created_timestamp
    )

    test_registry.teardown()


@pytest.mark.integration
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
    entities = test_registry.list_entities(project, allow_cache=True, tags=entity.tags)
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
    entities = registry_with_same_store.list_entities(project, tags=entity.tags)
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


@pytest.mark.integration
@pytest.mark.parametrize("test_registry", all_fixtures)
def test_apply_permission_success(test_registry):
    permission = Permission(
        name="read_permission",
        actions=AuthzedAction.READ,
        policy=RoleBasedPolicy(roles=["reader"]),
        types=FeatureView,
    )

    project = "project"

    # Register Permission
    test_registry.apply_permission(permission, project)
    project_metadata = test_registry.list_project_metadata(project=project)
    assert len(project_metadata) == 1
    project_uuid = project_metadata[0].project_uuid
    assert len(project_metadata[0].project_uuid) == 36
    assert_project_uuid(project, project_uuid, test_registry)

    permissions = test_registry.list_permissions(project)
    assert_project_uuid(project, project_uuid, test_registry)

    permission = permissions[0]
    assert (
        len(permissions) == 1
        and permission.name == "read_permission"
        and len(permission.types) == 1
        and permission.types[0] == FeatureView
        and len(permission.actions) == 1
        and permission.actions[0] == AuthzedAction.READ
        and isinstance(permission.policy, RoleBasedPolicy)
        and len(permission.policy.roles) == 1
        and permission.policy.roles[0] == "reader"
        and permission.with_subclasses
        and permission.name_pattern is None
        and permission.tags is None
    )

    permission = test_registry.get_permission("read_permission", project)
    assert (
        permission.name == "read_permission"
        and len(permission.types) == 1
        and permission.types[0] == FeatureView
        and len(permission.actions) == 1
        and permission.actions[0] == AuthzedAction.READ
        and isinstance(permission.policy, RoleBasedPolicy)
        and len(permission.policy.roles) == 1
        and permission.policy.roles[0] == "reader"
        and permission.with_subclasses
        and permission.name_pattern is None
        and permission.tags is None
    )

    # After the first apply, the created_timestamp should be the same as the last_update_timestamp.
    # TODO: no such fields
    # assert permission.created_timestamp == permission.last_updated_timestamp

    # Update permission
    updated_permission = Permission(
        name="read_permission",
        actions=[AuthzedAction.READ, AuthzedAction.WRITE_ONLINE],
        policy=RoleBasedPolicy(roles=["reader", "writer"]),
        types=FeatureView,
    )
    test_registry.apply_permission(updated_permission, project)

    permissions = test_registry.list_permissions(project)
    assert_project_uuid(project, project_uuid, test_registry)
    assert len(permissions) == 1

    updated_permission = test_registry.get_permission("read_permission", project)
    assert (
        updated_permission.name == "read_permission"
        and len(updated_permission.types) == 1
        and updated_permission.types[0] == FeatureView
        and len(updated_permission.actions) == 2
        and AuthzedAction.READ in updated_permission.actions
        and AuthzedAction.WRITE_ONLINE in updated_permission.actions
        and isinstance(updated_permission.policy, RoleBasedPolicy)
        and len(updated_permission.policy.roles) == 2
        and "reader" in updated_permission.policy.roles
        and "writer" in updated_permission.policy.roles
        and updated_permission.with_subclasses
        and updated_permission.name_pattern is None
        and updated_permission.tags is None
    )

    # # The created_timestamp for the entity should be set to the created_timestamp value stored from the previous apply
    # TODO: no such fields
    # assert (
    #     updated_entity.created_timestamp is not None
    #     and updated_entity.created_timestamp == entity.created_timestamp
    # )

    updated_permission = Permission(
        name="read_permission",
        actions=[AuthzedAction.READ, AuthzedAction.WRITE_ONLINE],
        policy=RoleBasedPolicy(roles=["reader", "writer"]),
        types=FeatureView,
        with_subclasses=False,
        name_pattern="aaa",
        tags={"team": "matchmaking"},
    )
    test_registry.apply_permission(updated_permission, project)

    permissions = test_registry.list_permissions(project)
    assert_project_uuid(project, project_uuid, test_registry)
    assert len(permissions) == 1

    updated_permission = test_registry.get_permission("read_permission", project)
    assert (
        updated_permission.name == "read_permission"
        and len(updated_permission.types) == 1
        and updated_permission.types[0] == FeatureView
        and len(updated_permission.actions) == 2
        and AuthzedAction.READ in updated_permission.actions
        and AuthzedAction.WRITE_ONLINE in updated_permission.actions
        and isinstance(updated_permission.policy, RoleBasedPolicy)
        and len(updated_permission.policy.roles) == 2
        and "reader" in updated_permission.policy.roles
        and "writer" in updated_permission.policy.roles
        and not updated_permission.with_subclasses
        and updated_permission.name_pattern == "aaa"
        and "team" in updated_permission.tags
        and updated_permission.tags["team"] == "matchmaking"
    )

    test_registry.delete_permission("read_permission", project)
    assert_project_uuid(project, project_uuid, test_registry)
    permissions = test_registry.list_permissions(project)
    assert_project_uuid(project, project_uuid, test_registry)
    assert len(permissions) == 0

    test_registry.teardown()
