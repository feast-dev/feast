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
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict

from feast.data_format import AvroFormat, ConfluentAvroFormat
from feast.data_source import KafkaSource, PushSource, RequestSource
from feast.entity import Entity
from feast.expediagroup.pydantic_models.data_source_model import (
    AnyDataSource,
    KafkaSourceModel,
    PushSourceModel,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.expediagroup.pydantic_models.feature_service import FeatureServiceModel
from feast.expediagroup.pydantic_models.feature_view_model import (
    FeatureViewModel,
    FeatureViewProjectionModel,
    OnDemandFeatureViewModel,
)
from feast.expediagroup.pydantic_models.field_model import FieldModel
from feast.expediagroup.pydantic_models.project_metadata_model import (
    ProjectMetadataModel,
)
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view
from feast.project_metadata import ProjectMetadata
from feast.types import Array, Bool, Float32, Float64, Int64, PrimitiveFeastType, String
from feast.value_type import ValueType


def test_idempotent_field_primitive_type_conversion():
    python_obj = Field(name="val_to_add", dtype=Int64)
    pydantic_obj = FieldModel.from_field(python_obj)
    converted_python_obj = pydantic_obj.to_field()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = Field.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == FieldModel.model_validate_json(pydantic_json)

    pydantic_dict = pydantic_obj.model_dump()
    assert pydantic_obj == FieldModel.model_validate(pydantic_dict)


def test_idempotent_field_complex_array_type_conversion():
    python_obj = Field(name="val_to_add", dtype=Array(Int64))
    pydantic_obj = FieldModel.from_field(python_obj)
    converted_python_obj = pydantic_obj.to_field()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = Field.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == FieldModel.model_validate_json(pydantic_json)

    pydantic_dict = pydantic_obj.model_dump()
    assert pydantic_obj == FieldModel.model_validate(pydantic_dict)


def test_datasource_child_deserialization():
    class DataSourcesByWire(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
        source_models: List[AnyDataSource] = []

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
        "schema_": [
            {
                "name": "string",
                "dtype": PrimitiveFeastType.INT64,
                "description": "",
                "tags": {},
            }
        ],
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
    python_obj = Entity(
        name="my-entity",
        description="My entity",
        value_type=ValueType.INT64,
        tags={"key1": "val1", "key2": "val2"},
    )
    pydantic_obj = EntityModel.from_entity(python_obj)
    converted_python_obj = pydantic_obj.to_entity()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = Entity.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == EntityModel.model_validate_json(pydantic_json)

    pydantic_dict = pydantic_obj.model_dump()
    assert pydantic_obj == EntityModel.model_validate(pydantic_dict)


def test_idempotent_requestsource_conversion():
    schema = [
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Bool),
    ]
    python_obj = RequestSource(
        name="source",
        schema=schema,
        description="desc",
        tags={},
        owner="feast",
    )
    pydantic_obj = RequestSourceModel.from_data_source(python_obj)
    converted_python_obj = pydantic_obj.to_data_source()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = RequestSource.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == RequestSourceModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == RequestSourceModel.model_validate(pydantic_json)


def test_idempotent_sparksource_conversion():
    python_obj = SparkSource(
        name="source",
        table="thingy",
        description="desc",
        tags={},
        owner="feast",
    )
    pydantic_obj = SparkSourceModel.from_data_source(python_obj)
    converted_python_obj = pydantic_obj.to_data_source()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = SparkSource.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == SparkSourceModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == SparkSourceModel.model_validate(pydantic_json)


def test_idempotent_pushsource_conversion():
    spark_source = SparkSource(
        name="spark-source",
        table="thingy",
        description="desc",
        tags={},
        owner="feast",
    )

    python_obj = PushSource(
        name="push-source",
        batch_source=spark_source,
        description="desc",
        tags={},
        owner="feast",
    )

    pydantic_obj = PushSourceModel.from_data_source(python_obj)
    converted_python_obj = pydantic_obj.to_data_source()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = PushSource.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == PushSourceModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == PushSourceModel.model_validate(pydantic_json)


def test_idempotent_kafkasource_avroformat_conversion():
    schema = [
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Bool),
    ]
    batch_source = RequestSource(
        name="source",
        schema=schema,
        description="desc",
        tags={"tag1": "val1"},
        owner="feast",
    )

    python_obj = KafkaSource(
        name="kafka_source",
        timestamp_field="whatevs, just a string",
        message_format=AvroFormat(schema_json="whatevs, also just a string"),
        batch_source=batch_source,
        description="Bob's used message formats emporium is open 24/7",
        tags={"source_thing": "thing_val"},
        kafka_bootstrap_servers="http://stuff.vals.place.go:3543/duck/duck/goose",
        topic="ad_spam",
        created_timestamp_column="created",
        field_mapping={"source_thing": "thing_val"},
        owner="test@mail.com",
        watermark_delay_threshold=timedelta(days=1),
    )

    with pytest.raises(ValueError) as exceptionInfo:
        KafkaSourceModel.from_data_source(python_obj)

    assert (
        str(exceptionInfo.value)
        == "Kafka Source's batch source type is not a supported data source type."
    )


def test_idempotent_kafkasource_confluentavroformat_conversion():
    batch_source = SparkSource(
        name="source",
        table="thingy",
        description="desc",
        tags={},
        owner="feast",
    )

    python_obj = KafkaSource(
        name="kafka_source",
        timestamp_field="whatevs, just a string",
        message_format=ConfluentAvroFormat(
            record_name="record_name", record_namespace="com.unittest"
        ),
        batch_source=batch_source,
        description="Bob's used message formats emporium is open 24/7",
        tags={"source_thing": "thing_val"},
        topic="ad_spam",
        created_timestamp_column="created",
        owner="test@mail.com",
    )

    pydantic_obj = KafkaSourceModel.from_data_source(python_obj)
    converted_python_obj = pydantic_obj.to_data_source()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = KafkaSource.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == KafkaSourceModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == KafkaSourceModel.model_validate(pydantic_json)


def test_idempotent_featureview_conversion():
    schema = [
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Bool),
    ]
    user_entity = Entity(
        name="user1", join_keys=["user_id"], value_type=ValueType.INT64
    )
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
            Field(name="user_id", dtype=Int64),
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=request_source,
        ttl=timedelta(days=0),
    )
    feature_view_model = FeatureViewModel.from_feature_view(feature_view)
    feature_view_b = feature_view_model.to_feature_view()
    assert feature_view == feature_view_b
    assert feature_view_model.created_timestamp == feature_view.created_timestamp
    assert (
        feature_view_model.last_updated_timestamp == feature_view.last_updated_timestamp
    )
    assert feature_view_b.created_timestamp == feature_view_model.created_timestamp
    assert (
        feature_view_b.last_updated_timestamp
        == feature_view_model.last_updated_timestamp
    )

    spark_source = SparkSource(
        name="sparky_sparky_boom_man",
        path="/data/driver_hourly_stats",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    python_obj = FeatureView(
        name="my-feature-view",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=spark_source,
        ttl=timedelta(days=10),
    )
    python_obj.materialization_intervals = [
        (
            datetime.now(timezone.utc) - timedelta(days=10),
            datetime.now(timezone.utc) - timedelta(days=9),
        ),
        (datetime.now(timezone.utc), datetime.now(timezone.utc)),
    ]
    pydantic_obj = FeatureViewModel.from_feature_view(python_obj)
    converted_python_obj = pydantic_obj.to_feature_view()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = FeatureView.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == FeatureViewModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == FeatureViewModel.model_validate(pydantic_json)


def test_idempotent_featureview_with_streaming_source_conversion():
    user_entity = Entity(
        name="user1", join_keys=["user_id"], value_type=ValueType.INT64
    )
    spark_source = SparkSource(
        name="sparky_sparky_boom_man",
        path="/data/driver_hourly_stats",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    kafka_source = KafkaSource(
        name="kafka_source",
        timestamp_field="whatevs, just a string",
        message_format=AvroFormat(schema_json="whatevs, also just a string"),
        batch_source=spark_source,
        description="Bob's used message formats emporium is open 24/7",
    )
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[user_entity],
        schema=[
            Field(name="user1", dtype=Int64),
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=kafka_source,
    )
    feature_view_model = FeatureViewModel.from_feature_view(feature_view)
    feature_view_b = feature_view_model.to_feature_view()
    assert feature_view == feature_view_b
    assert feature_view_model.created_timestamp == feature_view.created_timestamp
    assert (
        feature_view_model.last_updated_timestamp == feature_view.last_updated_timestamp
    )
    assert feature_view_b.created_timestamp == feature_view_model.created_timestamp
    assert (
        feature_view_b.last_updated_timestamp
        == feature_view_model.last_updated_timestamp
    )

    spark_source = SparkSource(
        name="sparky_sparky_boom_man",
        path="/data/driver_hourly_stats",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    python_obj = FeatureView(
        name="my-feature-view",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=spark_source,
        ttl=timedelta(days=0),
    )
    python_obj.materialization_intervals = [
        (
            datetime.now(timezone.utc) - timedelta(days=10),
            datetime.now(timezone.utc) - timedelta(days=9),
        ),
        (datetime.now(timezone.utc), datetime.now(timezone.utc)),
    ]
    pydantic_obj = FeatureViewModel.from_feature_view(python_obj)
    converted_python_obj = pydantic_obj.to_feature_view()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = FeatureView.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == FeatureViewModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == FeatureViewModel.model_validate(pydantic_json)


def test_idempotent_featureview_with_confluent_streaming_source_conversion():
    user_entity = Entity(
        name="user1", join_keys=["user_id"], value_type=ValueType.INT64
    )
    spark_source = SparkSource(
        name="sparky_sparky_boom_man",
        path="/data/driver_hourly_stats",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    kafka_source = KafkaSource(
        name="kafka_source",
        timestamp_field="whatevs, just a string",
        message_format=ConfluentAvroFormat(
            record_name="record_name", record_namespace="com.unittest"
        ),
        batch_source=spark_source,
        description="Bob's used message formats emporium is open 24/7",
    )
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[user_entity],
        schema=[
            Field(name="user1", dtype=Int64),
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=kafka_source,
    )
    feature_view_model = FeatureViewModel.from_feature_view(feature_view)
    feature_view_b = feature_view_model.to_feature_view()
    assert feature_view == feature_view_b
    assert feature_view_model.created_timestamp == feature_view.created_timestamp
    assert (
        feature_view_model.last_updated_timestamp == feature_view.last_updated_timestamp
    )
    assert feature_view_b.created_timestamp == feature_view_model.created_timestamp
    assert (
        feature_view_b.last_updated_timestamp
        == feature_view_model.last_updated_timestamp
    )

    spark_source = SparkSource(
        name="sparky_sparky_boom_man",
        path="/data/driver_hourly_stats",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    python_obj = FeatureView(
        name="my-feature-view",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=spark_source,
    )
    python_obj.materialization_intervals = [
        (
            datetime.now(timezone.utc) - timedelta(days=10),
            datetime.now(timezone.utc) - timedelta(days=9),
        ),
        (datetime.now(timezone.utc), datetime.now(timezone.utc)),
    ]
    pydantic_obj = FeatureViewModel.from_feature_view(python_obj)
    converted_python_obj = pydantic_obj.to_feature_view()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = FeatureView.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == FeatureViewModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == FeatureViewModel.model_validate(pydantic_json)


def test_idempotent_feature_view_projection_conversion():
    # Feast not using desired_features while converting to proto
    python_obj = FeatureViewProjection(
        name="example_projection",
        name_alias="alias",
        desired_features=[],
        features=[
            Field(name="feature1", dtype=Float64),
            Field(name="feature2", dtype=String),
        ],
        join_key_map={"old_key": "new_key"},
    )
    pydantic_obj = FeatureViewProjectionModel.from_feature_view_projection(python_obj)
    converted_python_obj = pydantic_obj.to_feature_view_projection()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = FeatureViewProjection.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == FeatureViewProjectionModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == FeatureViewProjectionModel.model_validate(pydantic_json)


def test_idempotent_on_demand_feature_view_conversion():
    tags = {
        "tag1": "val1",
        "tag2": "val2",
        "tag3": "val3",
    }

    """
        Entity is a collection of semantically related features.
    """
    entity1: Entity = Entity(
        name="entity1",
        description="entity1",
        value_type=ValueType.INT64,
        owner="x@xyz.com",
        tags=tags,
    )

    entity2: Entity = Entity(
        name="entity2",
        description="entity2",
        value_type=ValueType.INT64,
        owner="x@zyz.com",
        tags=tags,
    )

    """
        Data source refers to raw features data that users own. Feature Store
        does not manage any of the raw underlying data but instead, oversees
        loading this data and performing different operations on
        the data to retrieve or serve features.

        Feast uses a time-series data model to represent data.
    """

    datasource1: SparkSource = SparkSource(
        name="datasource1",
        description="datasource1",
        query="""select entity1
                    , val1
                    , val2
                    , val3
                    , val4
                    , val5
                    , CURRENT_DATE AS event_timestamp
                from table1
                WHERE entity1 < 100000""",
        timestamp_field="event_timestamp",
        tags=tags,
        owner="x@xyz.com",
    )

    datasource2: SparkSource = SparkSource(
        name="datasource2",
        description="datasource2",
        path="s3a://test-bucket/path1/datasource2",
        file_format="parquet",
        timestamp_field="event_timestamp",
        tags=tags,
        owner="x@xyz.com",
    )

    """
        A feature view is an object that represents a logical group
        of time-series feature data as it is found in a data source.
    """

    view1: FeatureView = FeatureView(
        name="view1",
        entities=[entity1],
        ttl=timedelta(days=365),
        source=datasource1,
        tags=tags,
        description="view1",
        owner="x@xyz.com",
        schema=[
            Field(name="val1", dtype=String),
            Field(name="val2", dtype=String),
            Field(name="val3", dtype=Float64),
            Field(name="val4", dtype=Float64),
            Field(name="val5", dtype=String),
        ],
    )

    view2: FeatureView = FeatureView(
        name="view2",
        entities=[entity2],
        ttl=timedelta(days=365),
        source=datasource2,
        tags=tags,
        description="view2",
        owner="x@xyz.com",
        schema=[
            Field(name="r1", dtype=Float64),
            Field(name="r2", dtype=Float64),
            Field(name="r3", dtype=String),
        ],
    )

    distance_decorator = on_demand_feature_view(
        sources=[view1.projection, view2.projection],
        schema=[Field(name="distance_in_kms", dtype=Float64)],
    )

    def calculate_distance_demo_go(features_df: pd.DataFrame) -> pd.DataFrame:
        import numpy as np

        df = pd.DataFrame()
        # Haversine formula
        # Radius of earth in kilometers. Use 3956 for miles
        r = 6371

        # calculate the result
        df["distance_in_kms"] = (
            2
            * np.arcsin(
                np.sqrt(
                    np.sin(
                        (
                            np.radians(features_df["val3"])
                            - np.radians(features_df["r1"])
                        )
                        / 2
                    )
                    ** 2
                    + np.cos(np.radians(features_df["r1"]))
                    * np.cos(np.radians(features_df["val3"]))
                    * np.sin(
                        (
                            np.radians(features_df["val4"])
                            - np.radians(features_df["r2"])
                        )
                        / 2
                    )
                    ** 2
                )
            )
            * r
        )

        return df

    python_obj = distance_decorator(calculate_distance_demo_go)
    pydantic_obj = OnDemandFeatureViewModel.from_feature_view(python_obj)
    converted_python_obj = pydantic_obj.to_feature_view()

    assert python_obj == converted_python_obj
    assert pydantic_obj.created_timestamp == python_obj.created_timestamp
    assert pydantic_obj.last_updated_timestamp == python_obj.last_updated_timestamp
    assert converted_python_obj.created_timestamp == pydantic_obj.created_timestamp
    assert (
        converted_python_obj.last_updated_timestamp
        == pydantic_obj.last_updated_timestamp
    )

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = OnDemandFeatureView.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == OnDemandFeatureViewModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == OnDemandFeatureViewModel.model_validate(pydantic_json)


def test_idempotent_feature_service_conversion():
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
        ttl=timedelta(days=0),
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
    view1 = FeatureView(
        name="my-feature-view1",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=spark_source,
    )

    spark_source_2 = SparkSource(
        name="sparky_sparky_boom_man2",
        path="/data/driver_hourly_stats",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    view2 = FeatureView(
        name="my-feature-view2",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=spark_source_2,
        ttl=timedelta(days=10),
    )

    python_obj = FeatureService(
        name="feature_service_1",
        description="Helps to retrieve features from view1 and view2",
        owner="bdodla@expediagroup.com",
        tags={},
        features=[view1, view2[["feature1"]]],
    )
    pydantic_obj = FeatureServiceModel.from_feature_service(python_obj)
    converted_python_obj = pydantic_obj.to_feature_service()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = FeatureService.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == FeatureServiceModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == FeatureServiceModel.model_validate(pydantic_json)


def test_idempotent_project_metadata_conversion():
    python_obj = ProjectMetadata(
        project_name="test_project",
        project_uuid=f"{uuid.uuid4()}",
        last_updated_timestamp=datetime.utcnow(),
    )
    pydantic_obj = ProjectMetadataModel.from_project_metadata(python_obj)
    converted_python_obj = pydantic_obj.to_project_metadata()
    assert python_obj == converted_python_obj

    feast_proto = converted_python_obj.to_proto()
    python_obj_from_proto = ProjectMetadata.from_proto(feast_proto)
    assert python_obj == python_obj_from_proto

    pydantic_json = pydantic_obj.model_dump_json()
    assert pydantic_obj == ProjectMetadataModel.model_validate_json(pydantic_json)

    pydantic_json = pydantic_obj.model_dump()
    assert pydantic_obj == ProjectMetadataModel.model_validate(pydantic_json)
