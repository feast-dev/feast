# This is an example feature definition file

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, Project, FeatureService
from feast.types import (
    Array,
    Bool,
    Bytes,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    UnixTimestamp,
)
from feast.value_type import ValueType

tags = {"team": "Feast"}

owner = "test@test.com"

# Define a project for the feature repo
project = Project(
    name="feature_integration_repo", description="A project for integration tests"
)

index_entity: Entity = Entity(
    name="index",
    description="Index for the random data",
    join_keys=["index_id"],
    value_type=ValueType.INT64,
    tags=tags,
    owner=owner,
)

mlpfs_test_all_datatypes_source: FileSource = FileSource(
    path="data.parquet", timestamp_field="event_timestamp"
)

mlpfs_test_all_datatypes_view: FeatureView = FeatureView(
    name="all_dtypes",
    entities=[index_entity],
    ttl=timedelta(days=0),
    source=mlpfs_test_all_datatypes_source,
    tags=tags,
    description="Feature View with all supported feast datatypes",
    owner=owner,
    online=True,
    schema=[
        Field(name="index_id", dtype=Int64),
        Field(name="int_val", dtype=Int32),
        Field(name="long_val", dtype=Int64),
        Field(name="float_val", dtype=Float32),
        Field(name="double_val", dtype=Float64),
        Field(name="byte_val", dtype=Bytes),
        Field(name="string_val", dtype=String),
        Field(name="timestamp_val", dtype=UnixTimestamp),
        Field(name="boolean_val", dtype=Bool),
        Field(name="array_int_val", dtype=Array(Int32)),
        Field(name="array_long_val", dtype=Array(Int64)),
        Field(name="array_float_val", dtype=Array(Float32)),
        Field(name="array_double_val", dtype=Array(Float64)),
        Field(name="array_byte_val", dtype=Array(Bytes)),
        Field(name="array_string_val", dtype=Array(String)),
        Field(name="array_timestamp_val", dtype=Array(UnixTimestamp)),
        Field(name="array_boolean_val", dtype=Array(Bool)),
        Field(name="null_int_val", dtype=Int32),
        Field(name="null_long_val", dtype=Int64),
        Field(name="null_float_val", dtype=Float32),
        Field(name="null_double_val", dtype=Float64),
        Field(name="null_byte_val", dtype=Bytes),
        Field(name="null_string_val", dtype=String),
        Field(name="null_timestamp_val", dtype=UnixTimestamp),
        Field(name="null_boolean_val", dtype=Bool),
        Field(name="null_array_int_val", dtype=Array(Int32)),
        Field(name="null_array_long_val", dtype=Array(Int64)),
        Field(name="null_array_float_val", dtype=Array(Float32)),
        Field(name="null_array_double_val", dtype=Array(Float64)),
        Field(name="null_array_byte_val", dtype=Array(Bytes)),
        Field(name="null_array_string_val", dtype=Array(String)),
        Field(name="null_array_timestamp_val", dtype=Array(UnixTimestamp)),
        Field(name="null_array_boolean_val", dtype=Array(Bool)),
    ],
)

mlpfs_test_all_datatypes_service = FeatureService(
    name="test_service",
    features=[mlpfs_test_all_datatypes_view],
)

