from feast.data_source import PushSource
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.protos.feast.types.Value_pb2 import ValueType


def test_push_no_batch():
    push_source = PushSource(
        name="test", schema={"user_id": ValueType.INT64, "ltv": ValueType.INT64}
    )
    push_source_proto = push_source.to_proto()
    assert push_source_proto.push_options is not None
    assert not push_source_proto.push_options.HasField("batch_source")
    push_source_unproto = PushSource.from_proto(push_source_proto)

    assert push_source == push_source_unproto


def test_push_with_batch():
    push_source = PushSource(
        name="test",
        schema={"user_id": ValueType.INT64, "ltv": ValueType.INT64},
        batch_source=BigQuerySource(table="test.test"),
    )
    push_source_proto = push_source.to_proto()
    assert push_source_proto.push_options is not None
    assert push_source_proto.push_options.HasField("batch_source")

    push_source_unproto = PushSource.from_proto(push_source_proto)

    assert push_source == push_source_unproto
