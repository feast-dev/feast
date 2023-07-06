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
from typing import List

from pydantic import BaseModel

from feast.data_source import RequestSource
from feast.entity import Entity
from feast.expediagroup.pydantic_models.data_source_model import (
    AnyDataSource,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.expediagroup.pydantic_models.feature_view_model import FeatureViewModel
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.types import Bool, Float32


def test_datasource_child_deserialization():
    class DataSourcesByWire(BaseModel):
        source_models: List[AnyDataSource] = []

        class Config:
            arbitrary_types_allowed = True
            extra = "allow"

    spark_source_model_json = {
        "name": "string",
        "model_type": "SparkSourceModel",
        "table": "table1",
        "query": "",
        "path": "",
        "file_format": "",
        "timestamp_field": "",
        "created_timestamp_column": "",
        "description": "",
        "owner": "",
        "date_partition_column": "",
    }

    spark_source_model = SparkSourceModel(**spark_source_model_json)

    request_source_model_json = {
        "name": "source",
        "model_type": "RequestSourceModel",
        "schema": [{"name": "string", "dtype": "Int32", "description": "", "tags": {}}],
        "description": "desc",
        "tags": {},
        "owner": "feast",
    }

    request_source_model = RequestSourceModel(**request_source_model_json)

    data_dict = {"source_models": [spark_source_model, request_source_model]}

    sources = DataSourcesByWire(**data_dict)

    assert type(sources.source_models[0]).__name__ == "SparkSourceModel"
    assert sources.source_models[0] == spark_source_model
    assert type(sources.source_models[1]).__name__ == "RequestSourceModel"
    assert sources.source_models[1] == request_source_model


def test_idempotent_entity_conversion():
    entity = Entity(
        name="my-entity",
        description="My entity",
        tags={"key1": "val1", "key2": "val2"},
    )
    entity_model = EntityModel.from_entity(entity)
    entity_b = entity_model.to_entity()
    assert entity == entity_b


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
    request_source_model = RequestSourceModel.from_data_source(request_source)
    request_source_b = request_source_model.to_data_source()
    assert request_source == request_source_b


def test_idempotent_sparksource_conversion():
    spark_source = SparkSource(
        name="source",
        table="thingy",
        description="desc",
        tags={},
        owner="feast",
    )
    spark_source_model = SparkSourceModel.from_data_source(spark_source)
    spark_source_b = spark_source_model.to_data_source()
    assert spark_source == spark_source_b


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
    feature_view_model = FeatureViewModel.from_feature_view(feature_view)
    feature_view_b = feature_view_model.to_feature_view()
    assert feature_view == feature_view_b

    spark_source = SparkSource(
        name="sparky_sparky_boom_man",
        path="/data/driver_hourly_stats",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=spark_source,
    )
    feature_view_model = FeatureViewModel.from_feature_view(feature_view)
    feature_view_b = feature_view_model.to_feature_view()
    assert feature_view == feature_view_b
