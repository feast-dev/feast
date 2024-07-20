from datetime import timezone
from typing import List

import pyarrow as pa

from feast.protos.feast.types import Value_pb2
from feast.types import Array, PrimitiveFeastType

PA_TIMESTAMP_TYPE = pa.timestamp("s", tz=timezone.utc)

ARROW_TYPE_TO_PROTO_FIELD = {
    pa.int32(): "int32_val",
    pa.int64(): "int64_val",
    pa.float32(): "float_val",
    pa.float64(): "double_val",
    pa.bool_(): "bool_val",
    pa.string(): "string_val",
    pa.binary(): "bytes_val",
    PA_TIMESTAMP_TYPE: "unix_timestamp_val",
}

ARROW_LIST_TYPE_TO_PROTO_FIELD = {
    pa.int32(): "int32_list_val",
    pa.int64(): "int64_list_val",
    pa.float32(): "float_list_val",
    pa.float64(): "double_list_val",
    pa.bool_(): "bool_list_val",
    pa.string(): "string_list_val",
    pa.binary(): "bytes_list_val",
    PA_TIMESTAMP_TYPE: "unix_timestamp_list_val",
}

ARROW_LIST_TYPE_TO_PROTO_LIST_CLASS = {
    pa.int32(): Value_pb2.Int32List,
    pa.int64(): Value_pb2.Int64List,
    pa.float32(): Value_pb2.FloatList,
    pa.float64(): Value_pb2.DoubleList,
    pa.bool_(): Value_pb2.BoolList,
    pa.string(): Value_pb2.StringList,
    pa.binary(): Value_pb2.BytesList,
    PA_TIMESTAMP_TYPE: Value_pb2.Int64List,
}

FEAST_TYPE_TO_ARROW_TYPE = {
    PrimitiveFeastType.INT32: pa.int32(),
    PrimitiveFeastType.INT64: pa.int64(),
    PrimitiveFeastType.FLOAT32: pa.float32(),
    PrimitiveFeastType.FLOAT64: pa.float64(),
    PrimitiveFeastType.STRING: pa.string(),
    PrimitiveFeastType.BYTES: pa.binary(),
    PrimitiveFeastType.BOOL: pa.bool_(),
    PrimitiveFeastType.UNIX_TIMESTAMP: pa.timestamp("s"),
    Array(PrimitiveFeastType.INT32): pa.list_(pa.int32()),
    Array(PrimitiveFeastType.INT64): pa.list_(pa.int64()),
    Array(PrimitiveFeastType.FLOAT32): pa.list_(pa.float32()),
    Array(PrimitiveFeastType.FLOAT64): pa.list_(pa.float64()),
    Array(PrimitiveFeastType.STRING): pa.list_(pa.string()),
    Array(PrimitiveFeastType.BYTES): pa.list_(pa.binary()),
    Array(PrimitiveFeastType.BOOL): pa.list_(pa.bool_()),
    Array(PrimitiveFeastType.UNIX_TIMESTAMP): pa.list_(pa.timestamp("s")),
}


def arrow_array_to_array_of_proto(
    arrow_type: pa.DataType, arrow_array: pa.Array
) -> List[Value_pb2.Value]:
    values = []
    if isinstance(arrow_type, pa.ListType):
        proto_list_class = ARROW_LIST_TYPE_TO_PROTO_LIST_CLASS[arrow_type.value_type]
        proto_field_name = ARROW_LIST_TYPE_TO_PROTO_FIELD[arrow_type.value_type]

        if arrow_type.value_type == PA_TIMESTAMP_TYPE:
            arrow_array = arrow_array.cast(pa.list_(pa.int64()))

        for v in arrow_array.tolist():
            values.append(
                Value_pb2.Value(**{proto_field_name: proto_list_class(val=v)})
            )
    else:
        proto_field_name = ARROW_TYPE_TO_PROTO_FIELD[arrow_type]

        if arrow_type == PA_TIMESTAMP_TYPE:
            arrow_array = arrow_array.cast(pa.int64())

        for v in arrow_array.tolist():
            values.append(Value_pb2.Value(**{proto_field_name: v}))

    return values
