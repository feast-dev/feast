import pytest

from feast import ValueType
from feast.data_source import PushSource, RequestDataSource, RequestSource
from feast.infra.offline_stores.bigquery_source import BigQuerySource


def test_push_with_batch():
    push_source = PushSource(
        name="test",
        schema={"user_id": ValueType.INT64, "ltv": ValueType.INT64},
        batch_source=BigQuerySource(table="test.test"),
    )
    push_source_proto = push_source.to_proto()
    assert push_source_proto.HasField("batch_source")
    assert push_source_proto.push_options is not None

    push_source_unproto = PushSource.from_proto(push_source_proto)

    assert push_source.name == push_source_unproto.name
    assert push_source.schema == push_source_unproto.schema
    assert push_source.batch_source.name == push_source_unproto.batch_source.name


def test_request_data_source_deprecation():
    with pytest.warns(DeprecationWarning):
        request_data_source = RequestDataSource(
            name="vals_to_add",
            schema={"val_to_add": ValueType.INT64, "val_to_add_2": ValueType.INT64},
        )
        request_data_source_proto = request_data_source.to_proto()
        returned_request_source = RequestSource.from_proto(request_data_source_proto)
        assert returned_request_source.name == request_data_source.name
        assert returned_request_source.schema == request_data_source.schema
