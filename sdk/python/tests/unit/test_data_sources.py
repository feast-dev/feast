import pytest

from feast.data_format import ProtoFormat
from feast.data_source import (
    DataSource,
    KafkaSource,
    KinesisSource,
    PushSource,
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
        name="test",
        batch_source=BigQuerySource(table="test.test"),
    )
    push_source_proto = push_source.to_proto()
    assert push_source_proto.HasField("batch_source")

    push_source_unproto = PushSource.from_proto(push_source_proto)

    assert push_source.name == push_source_unproto.name
    assert push_source.batch_source.name == push_source_unproto.batch_source.name


def test_request_source_primitive_type_to_proto():
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
    request_proto = request_source.to_proto()
    deserialized_request_source = RequestSource.from_proto(request_proto)
    assert deserialized_request_source == request_source


def test_hash():
    push_source_1 = PushSource(
        name="test",
        batch_source=BigQuerySource(table="test.test"),
    )
    push_source_2 = PushSource(
        name="test",
        batch_source=BigQuerySource(table="test.test"),
    )
    push_source_3 = PushSource(
        name="test",
        batch_source=BigQuerySource(table="test.test2"),
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
        kafka_bootstrap_servers="test_servers",
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


def test_column_conflict():
    with pytest.raises(ValueError):
        _ = FileSource(
            name="test_source",
            path="test_path",
            timestamp_field="event_timestamp",
            created_timestamp_column="event_timestamp",
        )


@pytest.mark.parametrize(
    "source_kwargs,expected_name",
    [
        (
            {
                "database": "test_database",
                "schema": "test_schema",
                "table": "test_table",
            },
            "test_database.test_schema.test_table",
        ),
        (
            {"database": "test_database", "table": "test_table"},
            "test_database.public.test_table",
        ),
        ({"table": "test_table"}, "public.test_table"),
        ({"database": "test_database", "table": "b.c"}, "test_database.b.c"),
        ({"database": "test_database", "table": "a.b.c"}, "a.b.c"),
        (
            {
                "database": "test_database",
                "schema": "test_schema",
                "query": "select * from abc",
            },
            "",
        ),
    ],
)
def test_redshift_fully_qualified_table_name(source_kwargs, expected_name):
    redshift_source = RedshiftSource(
        name="test_source",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
        field_mapping={"foo": "bar"},
        description="test description",
        tags={"test": "test"},
        owner="test@gmail.com",
        **source_kwargs,
    )

    assert redshift_source.redshift_options.fully_qualified_table_name == expected_name
