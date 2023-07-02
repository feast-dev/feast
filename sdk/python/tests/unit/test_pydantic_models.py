# Copyright 2020 The Feast Authors
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
import assertpy
import pytest

from feast.entity import Entity, EntityModel
from feast.field import Field
from feast.data_source import DataSource, DataSourceModel, RequestSource, RequestSourceModel
from feast.feature_view import FeatureView, FeatureViewModel
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import \
    SparkSource, SparkSourceModel
from feast.types import Bool, Float32, Int64


def test_idempotent_entity_conversion():
    entity = Entity(
        name="my-entity",
        description="My entity",
        tags={"key1": "val1", "key2": "val2"},
    )
    entity_model = entity.to_pydantic_model()
    entity_2 = Entity.entity_from_pydantic_model(entity_model)
    assert entity == entity_2


def test_idempotent_requestsource_conversion():
    schema = [
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Bool),
    ]
    request_source = RequestSource(
        name="source",
        schema=schema,
        description="desc",
        tags={},
        owner="feast",
    )
    request_source_model = request_source.to_pydantic_model()
    request_source_2 = RequestSource.datasource_from_pydantic_model(request_source_model)
    assert request_source == request_source_2


def test_idempotent_sparksource_conversion():
    spark_source = SparkSource(
        name="source",
        table="thingy",
        description="desc",
        tags={},
        owner="feast",
    )
    spark_source_model = spark_source.to_pydantic_model()
    spark_source_2 = SparkSource.datasource_from_pydantic_model(spark_source_model)
    assert spark_source == spark_source_2


def test_type_safety_when_converting_multiple_datasources():
    pass


def test_idempotent_featureview_conversion():
    schema = [
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Bool),
    ]
    user_entity = Entity(name="user1", join_keys=["user_id"])
    request_source = RequestSource(
        name="source",
        schema=schema,
        description="desc",
        tags={},
        owner="feast",
    )
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=request_source,
    )
    feature_view_model = feature_view.to_pydantic_model()
    feature_view_2 = FeatureView.featureview_from_pydantic_model(feature_view_model)
    print(feature_view.original_schema)
    print(feature_view_2.original_schema)
    assert feature_view == feature_view_2
