import pyarrow as pa
import pytest

from feast.utils import _columns_to_proto_values
from feast.value_type import ValueType

# 2**53 + 1 is the first integer a float64 cannot hold.
LARGE = 2**53 + 1
LARGEST = 2**63 - 1


def _int64_values(batch_type, values):
    data = batch_type.from_pydict({"value": pa.array(values, pa.int64())})
    protos = _columns_to_proto_values(data, [("value", ValueType.INT64)])["value"]
    return [p.int64_val if p.HasField("int64_val") else None for p in protos]


@pytest.mark.parametrize("batch_type", [pa.RecordBatch, pa.Table])
def test_nullable_int64_column_keeps_values_above_2_53_exact(batch_type):
    assert _int64_values(batch_type, [LARGE, None, -LARGE, 7]) == [
        LARGE,
        None,
        -LARGE,
        7,
    ]


@pytest.mark.parametrize("batch_type", [pa.RecordBatch, pa.Table])
def test_nullable_int64_column_accepts_the_largest_value(batch_type):
    assert _int64_values(batch_type, [LARGEST, None]) == [LARGEST, None]


def test_nullable_int32_column_keeps_values_and_nulls():
    data = pa.RecordBatch.from_pydict(
        {"value": pa.array([2**31 - 1, None, -(2**31)], pa.int32())}
    )

    protos = _columns_to_proto_values(data, [("value", ValueType.INT32)])["value"]

    assert [p.int32_val if p.HasField("int32_val") else None for p in protos] == [
        2**31 - 1,
        None,
        -(2**31),
    ]


def test_int64_column_without_nulls_is_unchanged():
    data = pa.RecordBatch.from_pydict({"value": pa.array([LARGE, 7], pa.int64())})

    protos = _columns_to_proto_values(data, [("value", ValueType.INT64)])["value"]

    assert [p.int64_val for p in protos] == [LARGE, 7]
