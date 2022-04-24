import pytest

from feast import ValueType
from feast.data_format import ProtoFormat
from feast.data_source import (
    DataSource,
    KafkaSource,
    KinesisSource,
    PushSource,
    RequestDataSource,
    RequestSource,
)
from feast.field import Field
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.types import Bool, Float32, Int64


def test_push_with_batch():
    push_source = PushSource(
        name="test", batch_source=BigQuerySource(table="test.test"),
    )
    push_source_proto = push_source.to_proto()
    assert push_source_proto.HasField("batch_source")

    push_source_unproto = PushSource.from_proto(push_source_proto)

    assert push_source.name == push_source_unproto.name
    assert push_source.batch_source.name == push_source_unproto.batch_source.name


def test_request_data_source_deprecation():
    with pytest.warns(DeprecationWarning):
        request_data_source = RequestDataSource(
            name="vals_to_add",
            schema={"val_to_add": ValueType.INT64, "val_to_add_2": ValueType.INT64},
        )
        request_data_source_proto = request_data_source.to_proto()
        returned_request_source = RequestSource.from_proto(request_data_source_proto)
        assert returned_request_source == request_data_source


def test_request_source_primitive_type_to_proto():
    schema = [
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Bool),
    ]
    request_source = RequestSource(
        name="source", schema=schema, description="desc", tags={}, owner="feast",
    )
    request_proto = request_source.to_proto()
    deserialized_request_source = RequestSource.from_proto(request_proto)
    assert deserialized_request_source == request_source


def test_hash():
    push_source_1 = PushSource(
        name="test", batch_source=BigQuerySource(table="test.test"),
    )
    push_source_2 = PushSource(
        name="test", batch_source=BigQuerySource(table="test.test"),
    )
    push_source_3 = PushSource(
        name="test", batch_source=BigQuerySource(table="test.test2"),
    )
    push_source_4 = PushSource(
        name="test",
        batch_source=BigQuerySource(table="test.test2"),
        description="test",
    )

    s1 = {push_source_1, push_source_2}
    assert len(s1) == 1

    s2 = {push_source_1, push_source_3}
    assert len(s2) == 2

    s3 = {push_source_3, push_source_4}
    assert len(s3) == 2

    s4 = {push_source_1, push_source_2, push_source_3, push_source_4}
    assert len(s4) == 3


# TODO(kevjumba): Remove this test in feast 0.23 when positional arguments are removed.
def test_default_data_source_kw_arg_warning():
    # source_class = request.param
    with pytest.warns(DeprecationWarning):
        source = KafkaSource(
            "name", "column", "bootstrap_servers", ProtoFormat("class_path"), "topic"
        )
        assert source.name == "name"
        assert source.timestamp_field == "column"
        assert source.kafka_options.bootstrap_servers == "bootstrap_servers"
        assert source.kafka_options.topic == "topic"
    with pytest.raises(ValueError):
        KafkaSource("name", "column", "bootstrap_servers", topic="topic")

    with pytest.warns(DeprecationWarning):
        source = KinesisSource(
            "name",
            "column",
            "c_column",
            ProtoFormat("class_path"),
            "region",
            "stream_name",
        )
        assert source.name == "name"
        assert source.timestamp_field == "column"
        assert source.created_timestamp_column == "c_column"
        assert source.kinesis_options.region == "region"
        assert source.kinesis_options.stream_name == "stream_name"

    with pytest.raises(ValueError):
        KinesisSource(
            "name", "column", "c_column", region="region", stream_name="stream_name"
        )

    with pytest.warns(DeprecationWarning):
        source = RequestSource(
            "name", [Field(name="val_to_add", dtype=Int64)], description="description"
        )
        assert source.name == "name"
        assert source.description == "description"

    with pytest.raises(ValueError):
        RequestSource("name")

    with pytest.warns(DeprecationWarning):
        source = PushSource(
            "name",
            BigQuerySource(name="bigquery_source", table="table"),
            description="description",
        )
        assert source.name == "name"
        assert source.description == "description"
        assert source.batch_source.name == "bigquery_source"

    with pytest.raises(ValueError):
        PushSource("name")

    # No name warning for DataSource
    with pytest.warns(UserWarning):
        source = KafkaSource(
            timestamp_field="column",
            bootstrap_servers="bootstrap_servers",
            message_format=ProtoFormat("class_path"),
            topic="topic",
        )


def test_proto_conversion():
    bigquery_source = BigQuerySource(
        name="test_source",
        table="test_table",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
        field_mapping={"foo": "bar"},
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
    )

    file_source = FileSource(
        name="test_source",
        path="test_path",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
        field_mapping={"foo": "bar"},
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
    )

    redshift_source = RedshiftSource(
        name="test_source",
        database="test_database",
        schema="test_schema",
        table="test_table",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
        field_mapping={"foo": "bar"},
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
    )

    snowflake_source = SnowflakeSource(
        name="test_source",
        database="test_database",
        warehouse="test_warehouse",
        schema="test_schema",
        table="test_table",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
        field_mapping={"foo": "bar"},
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
    )

    kafka_source = KafkaSource(
        name="test_source",
        bootstrap_servers="test_servers",
        message_format=ProtoFormat("class_path"),
        topic="test_topic",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
        field_mapping={"foo": "bar"},
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
        batch_source=file_source,
    )

    kinesis_source = KinesisSource(
        name="test_source",
        region="test_region",
        record_format=ProtoFormat("class_path"),
        stream_name="test_stream",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
        field_mapping={"foo": "bar"},
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
        batch_source=file_source,
    )

    push_source = PushSource(
        name="test_source",
        batch_source=file_source,
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
    )

    request_source = RequestSource(
        name="test_source",
        schema=[Field(name="test1", dtype=Float32), Field(name="test1", dtype=Int64)],
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
    )

    assert DataSource.from_proto(bigquery_source.to_proto()) == bigquery_source
    assert DataSource.from_proto(file_source.to_proto()) == file_source
    assert DataSource.from_proto(redshift_source.to_proto()) == redshift_source
    assert DataSource.from_proto(snowflake_source.to_proto()) == snowflake_source
    assert DataSource.from_proto(kafka_source.to_proto()) == kafka_source
    assert DataSource.from_proto(kinesis_source.to_proto()) == kinesis_source
    assert DataSource.from_proto(push_source.to_proto()) == push_source
    assert DataSource.from_proto(request_source.to_proto()) == request_source
