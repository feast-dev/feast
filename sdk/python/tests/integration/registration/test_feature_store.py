# Copyright 2019 The Feast Authors
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
import os
import time
from datetime import timedelta
from tempfile import mkstemp

import pytest
from pytest_lazyfixture import lazy_fixture

from feast import FileSource
from feast.data_format import ParquetFormat
from feast.entity import Entity
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Array, Bytes, Float64, Int64, String
from tests.utils.data_source_test_creator import prep_file_source


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_feature_store",
    [
        lazy_fixture("feature_store_with_gcs_registry"),
        lazy_fixture("feature_store_with_s3_registry"),
    ],
)
def test_apply_entity_integration(test_feature_store):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        tags={"team": "matchmaking"},
    )

    # Register Entity
    test_feature_store.apply([entity])

    entities = test_feature_store.list_entities()

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    entity = test_feature_store.get_entity("driver_car_id")
    assert (
        entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    test_feature_store.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
@pytest.mark.parametrize("dataframe_source", [lazy_fixture("simple_dataset_1")])
def test_feature_view_inference_success(test_feature_store, dataframe_source):
    with prep_file_source(df=dataframe_source, timestamp_field="ts_1") as file_source:
        entity = Entity(name="id", join_keys=["id_join_key"])

        fv1 = FeatureView(
            name="fv1",
            entities=[entity],
            ttl=timedelta(minutes=5),
            online=True,
            source=file_source,
            tags={},
        )

        test_feature_store.apply([entity, fv1])  # Register Feature Views
        feature_view_1 = test_feature_store.list_feature_views()[0]

        actual_file_source = {
            (feature.name, feature.dtype) for feature in feature_view_1.features
        }
        expected = {
            ("float_col", Float64),
            ("int64_col", Int64),
            ("string_col", String),
        }

        assert expected == actual_file_source

        test_feature_store.teardown()


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_feature_store",
    [
        lazy_fixture("feature_store_with_gcs_registry"),
        lazy_fixture("feature_store_with_s3_registry"),
    ],
)
def test_apply_feature_view_integration(test_feature_store):
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
            Field(name="test", dtype=Int64),
        ],
        entities=[entity],
        tags={"team": "matchmaking"},
        source=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Register Feature View
    test_feature_store.apply([fv1, entity])

    feature_views = test_feature_store.list_feature_views()

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

    feature_view = test_feature_store.get_feature_view("my_feature_view_1")
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

    test_feature_store.delete_feature_view("my_feature_view_1")
    feature_views = test_feature_store.list_feature_views()
    assert len(feature_views) == 0

    test_feature_store.teardown()


@pytest.fixture
def feature_store_with_local_registry():
    fd, registry_path = mkstemp()
    fd, online_store_path = mkstemp()
    return FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
            entity_key_serialization_version=2,
        )
    )


@pytest.fixture
def feature_store_with_gcs_registry():
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

    return FeatureStore(
        config=RepoConfig(
            registry=f"gs://{bucket_name}/registry.db",
            project="default",
            provider="gcp",
            entity_key_serialization_version=2,
        )
    )


@pytest.fixture
def feature_store_with_s3_registry():
    aws_registry_path = os.getenv(
        "AWS_REGISTRY_PATH", "s3://feast-integration-tests/registries"
    )
    return FeatureStore(
        config=RepoConfig(
            registry=f"{aws_registry_path}/{int(time.time() * 1000)}/registry.db",
            project="default",
            provider="aws",
            online_store=DynamoDBOnlineStoreConfig(
                region=os.getenv("AWS_REGION", "us-west-2")
            ),
            offline_store=FileOfflineStoreConfig(),
            entity_key_serialization_version=2,
        )
    )
