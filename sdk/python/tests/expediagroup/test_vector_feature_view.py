from copy import copy
from datetime import timedelta

import pytest
from google.protobuf.duration_pb2 import Duration
from pydantic import ValidationError

from feast.aggregation import Aggregation
from feast.batch_feature_view import BatchFeatureView
from feast.data_format import AvroFormat
from feast.data_source import KafkaSource, PushSource
from feast.entity import Entity
from feast.expediagroup.vectordb.index_type import IndexType
from feast.expediagroup.vectordb.vector_feature_view import VectorFeatureView
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.protos.feast.core.DataSource_pb2 import DataSource
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.VectorFeatureView_pb2 import (
    VectorFeatureView as VectorFeatureViewProto,
)
from feast.protos.feast.types.Value_pb2 import ValueType
from feast.types import Float32


def test_create_vector_feature_view_with_conflicting_entities():
    user1 = Entity(name="user1", join_keys=["user_id"])
    user2 = Entity(name="user2", join_keys=["user_id"])
    batch_source = FileSource(path="some path")

    with pytest.raises(ValueError):
        _ = VectorFeatureView(
            name="test",
            entities=[user1, user2],
            ttl=timedelta(days=30),
            source=batch_source,
            vector_field="vector_field",
            dimensions=32,
            index_algorithm=IndexType.hnsw,
        )


def test_hash():
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view_1 = VectorFeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
        vector_field="vector_field",
        dimensions=32,
        index_algorithm=IndexType.hnsw,
    )
    feature_view_2 = VectorFeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
        vector_field="vector_field",
        dimensions=32,
        index_algorithm=IndexType.hnsw,
    )
    feature_view_3 = VectorFeatureView(
        name="my-feature-view",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
        vector_field="vector_field",
        dimensions=32,
        index_algorithm=IndexType.hnsw,
    )
    feature_view_4 = VectorFeatureView(
        name="my-feature-view",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
        vector_field="vector_field",
        dimensions=32,
        index_algorithm=IndexType.hnsw,
        description="test",
    )

    s1 = {feature_view_1, feature_view_2}
    assert len(s1) == 1

    s2 = {feature_view_1, feature_view_3}
    assert len(s2) == 2

    s3 = {feature_view_3, feature_view_4}
    assert len(s3) == 2

    s4 = {feature_view_1, feature_view_2, feature_view_3, feature_view_4}
    assert len(s4) == 3


def test_vector_field_mapping():
    feature_view_name = "name"
    vector_field_name = "vector_field"
    vector_dimensions = 32
    vector_index_algorithm = IndexType.hnsw

    user1 = Entity(name="user1")
    batch_source = FileSource(path="some path")

    vector_feature_view = VectorFeatureView(
        name=feature_view_name,
        entities=[user1],
        ttl=timedelta(days=30),
        source=batch_source,
        vector_field=vector_field_name,
        dimensions=vector_dimensions,
        index_algorithm=vector_index_algorithm,
    )

    assert vector_feature_view.vector_field == vector_field_name
    assert vector_feature_view.dimensions == vector_dimensions
    assert vector_feature_view.index_algorithm == vector_index_algorithm
    assert vector_feature_view.feature_view.name == feature_view_name


def test_copy():
    user1 = Entity(name="user1")
    batch_source = FileSource(path="some path")

    feature_view = VectorFeatureView(
        name="test",
        entities=[user1],
        ttl=timedelta(days=30),
        source=batch_source,
        vector_field="vector_field",
        dimensions=32,
        index_algorithm=IndexType.hnsw,
    )

    feature_view_copy = copy(feature_view)
    assert feature_view_copy == feature_view


def test_join_keys():
    entity1 = Entity(name="user1", join_keys=["feature1"])
    entity2 = Entity(name="user2", join_keys=["feature2"])
    batch_source = FileSource(path="some path")

    feature_view = VectorFeatureView(
        name="test",
        entities=[entity1, entity2],
        ttl=timedelta(days=30),
        source=batch_source,
        vector_field="vector_field",
        dimensions=32,
        index_algorithm=IndexType.hnsw,
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
    )

    join_keys = feature_view.join_keys
    assert isinstance(join_keys, list)
    assert len(join_keys) == 2
    assert join_keys.__getitem__(0) == "feature1"
    assert join_keys.__getitem__(1) == "feature2"


def test_from_proto():
    vector_field_name = "vector_field"
    vector_dimensions = 32
    vector_index_algorithm = IndexType.hnsw

    entity1 = Entity(name="user1", join_keys=["feature1"])
    entities = [entity1]
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[entity1],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )

    feature_view_proto = feature_view.to_proto()
    entities_proto = [entity.to_proto() for entity in entities]
    vector_feature_view_proto = VectorFeatureViewProto(
        feature_view=feature_view_proto,
        entities=entities_proto,
        vector_field=vector_field_name,
        dimensions=vector_dimensions,
        index_type=vector_index_algorithm.value,
    )

    vector_feature_view = VectorFeatureView.from_proto(vector_feature_view_proto)
    assert isinstance(vector_feature_view, VectorFeatureView)
    assert vector_feature_view.feature_view.original_entities == entities
    assert vector_feature_view.vector_field == vector_field_name
    assert vector_feature_view.dimensions == vector_dimensions
    assert vector_feature_view.index_algorithm == vector_index_algorithm


def test_to_proto():
    vector_field_name = "vector_field"
    vector_dimensions = 32
    vector_index_algorithm = IndexType.hnsw

    entity1 = Entity(name="user1", join_keys=["feature1"])
    entity2 = Entity(name="user2", join_keys=["feature2"])
    batch_source = FileSource(path="some path")

    vector_feature_view = VectorFeatureView(
        name="test",
        entities=[entity1, entity2],
        ttl=timedelta(days=30),
        source=batch_source,
        vector_field=vector_field_name,
        dimensions=vector_dimensions,
        index_algorithm=vector_index_algorithm,
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
    )

    proto = vector_feature_view.to_proto()

    assert isinstance(proto, VectorFeatureViewProto)
    assert proto.vector_field == vector_field_name
    assert proto.dimensions == vector_dimensions
    assert proto.index_type == vector_index_algorithm.value
