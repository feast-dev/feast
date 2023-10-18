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
import os
import time
from datetime import timedelta

import pytest
from pytest_lazyfixture import lazy_fixture

from feast import FileSource
from feast.data_format import ParquetFormat
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig
from feast.types import Array, Bytes, Int64, String
from tests.utils.e2e_test_validation import validate_registry_data_source_apply


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
        "AWS_REGISTRY_PATH", "s3://feast-integration-tests/registries"
    )
    registry_config = RegistryConfig(
        path=f"{aws_registry_path}/{int(time.time() * 1000)}/registry.db",
        cache_ttl_seconds=600,
    )
    return Registry("project", registry_config, None)


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("gcs_registry"), lazy_fixture("s3_registry")],
)
def test_apply_entity_integration(test_registry):
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

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("gcs_registry"), lazy_fixture("s3_registry")],
)
def test_apply_feature_view_integration(test_registry):
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_registry",
    [lazy_fixture("gcs_registry"), lazy_fixture("s3_registry")],
)
def test_apply_data_source_integration(test_registry: Registry):
    validate_registry_data_source_apply(test_registry)
