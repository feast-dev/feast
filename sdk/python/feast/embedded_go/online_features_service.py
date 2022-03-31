from functools import partial
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from google.protobuf.timestamp_pb2 import Timestamp
from pyarrow.cffi import ffi

from feast.errors import (
    FeatureNameCollisionError,
    RequestDataNotFoundInEntityRowsException,
)
from feast.feature_service import FeatureService
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types import Value_pb2
from feast.repo_config import RepoConfig
from feast.value_type import ValueType

from .lib.embedded import DataTable, NewOnlineFeatureService, OnlineFeatureServiceConfig
from .lib.go import Slice_string

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore


ARROW_TYPE_TO_PROTO_FIELD = {
    pa.int32(): "int32_val",
    pa.int64(): "int64_val",
    pa.float32(): "float_val",
    pa.float64(): "double_val",
    pa.bool_(): "bool_val",
    pa.string(): "string_val",
    pa.binary(): "bytes_val",
    pa.time64("ns"): "unix_timestamp_val",
}

ARROW_LIST_TYPE_TO_PROTO_FIELD = {
    pa.int32(): "int32_list_val",
    pa.int64(): "int64_list_val",
    pa.float32(): "float_list_val",
    pa.float64(): "double_list_val",
    pa.bool_(): "bool_list_val",
    pa.string(): "string_list_val",
    pa.binary(): "bytes_list_val",
    pa.time64("ns"): "unix_timestamp_list_val",
}

ARROW_LIST_TYPE_TO_PROTO_LIST_CLASS = {
    pa.int32(): Value_pb2.Int32List,
    pa.int64(): Value_pb2.Int64List,
    pa.float32(): Value_pb2.FloatList,
    pa.float64(): Value_pb2.DoubleList,
    pa.bool_(): Value_pb2.BoolList,
    pa.string(): Value_pb2.StringList,
    pa.binary(): Value_pb2.BytesList,
    pa.time64("ns"): Value_pb2.Int64List,
}

# used for entity types only
PROTO_TYPE_TO_ARROW_TYPE = {
    ValueType.INT32: pa.int32(),
    ValueType.INT64: pa.int64(),
    ValueType.FLOAT: pa.float32(),
    ValueType.DOUBLE: pa.float64(),
    ValueType.STRING: pa.string(),
    ValueType.BYTES: pa.binary(),
}


class EmbeddedOnlineFeatureServer:
    def __init__(
        self, repo_path: str, repo_config: RepoConfig, feature_store: "FeatureStore"
    ):
        # keep callback in self to prevent it from GC
        self._transformation_callback = partial(transformation_callback, feature_store)

        self._service = NewOnlineFeatureService(
            OnlineFeatureServiceConfig(
                RepoPath=repo_path, RepoConfig=repo_config.json()
            ),
            self._transformation_callback,
        )

    def get_online_features(
        self,
        features_refs: List[str],
        feature_service: Optional[FeatureService],
        entities: Dict[str, Union[List[Any], Value_pb2.RepeatedValue]],
        request_data: Dict[str, Union[List[Any], Value_pb2.RepeatedValue]],
        full_feature_names: bool = False,
    ):

        if feature_service:
            join_keys_types = self._service.GetEntityTypesMapByFeatureService(
                feature_service.name
            )
        else:
            join_keys_types = self._service.GetEntityTypesMap(
                Slice_string(features_refs)
            )

        join_keys_types = {
            join_key: ValueType(enum_value) for join_key, enum_value in join_keys_types
        }

        # Here we create C structures that will be shared between Python and Go.
        # We will pass entities as arrow Record Batch to Go part (in_c_array & in_c_schema)
        # and receive features as Record Batch from Go (out_c_array & out_c_schema)
        # This objects needs to be initialized here in order to correctly
        # free them later using Python GC.
        (
            entities_c_schema,
            entities_ptr_schema,
            entities_c_array,
            entities_ptr_array,
        ) = allocate_schema_and_array()
        (
            req_data_c_schema,
            req_data_ptr_schema,
            req_data_c_array,
            req_data_ptr_array,
        ) = allocate_schema_and_array()

        (
            features_c_schema,
            features_ptr_schema,
            features_c_array,
            features_ptr_array,
        ) = allocate_schema_and_array()

        batch, schema = map_to_record_batch(entities, join_keys_types)
        schema._export_to_c(entities_ptr_schema)
        batch._export_to_c(entities_ptr_array)

        batch, schema = map_to_record_batch(request_data)
        schema._export_to_c(req_data_ptr_schema)
        batch._export_to_c(req_data_ptr_array)

        try:
            self._service.GetOnlineFeatures(
                featureRefs=Slice_string(features_refs),
                featureServiceName=feature_service and feature_service.name or "",
                entities=DataTable(
                    SchemaPtr=entities_ptr_schema, DataPtr=entities_ptr_array
                ),
                requestData=DataTable(
                    SchemaPtr=req_data_ptr_schema, DataPtr=req_data_ptr_array
                ),
                fullFeatureNames=full_feature_names,
                output=DataTable(
                    SchemaPtr=features_ptr_schema, DataPtr=features_ptr_array
                ),
            )
        except RuntimeError as exc:
            (msg,) = exc.args
            if msg.startswith("featureNameCollisionError"):
                feature_refs = msg[len("featureNameCollisionError: ") : msg.find(";")]
                feature_refs = feature_refs.split(",")
                raise FeatureNameCollisionError(
                    feature_refs_collisions=feature_refs,
                    full_feature_names=full_feature_names,
                )

            if msg.startswith("requestDataNotFoundInEntityRowsException"):
                feature_refs = msg[len("requestDataNotFoundInEntityRowsException: ") :]
                feature_refs = feature_refs.split(",")
                raise RequestDataNotFoundInEntityRowsException(feature_refs)

            raise

        record_batch = pa.RecordBatch._import_from_c(
            features_ptr_array, features_ptr_schema
        )
        resp = record_batch_to_online_response(record_batch)
        return OnlineResponse(resp)


def _to_arrow(value, type_hint: Optional[ValueType]) -> pa.Array:
    if isinstance(value, Value_pb2.RepeatedValue):
        _proto_to_arrow(value)

    if type_hint in PROTO_TYPE_TO_ARROW_TYPE:
        return pa.array(value, PROTO_TYPE_TO_ARROW_TYPE[type_hint])

    return pa.array(value)


def _proto_to_arrow(value: Value_pb2.RepeatedValue) -> pa.Array:
    """
    ToDo: support entity rows already packed in protos
    """
    raise NotImplementedError


def transformation_callback(
    fs: "FeatureStore",
    on_demand_feature_view_name: str,
    input_arr_ptr: int,
    input_schema_ptr: int,
    output_arr_ptr: int,
    output_schema_ptr: int,
    full_feature_names: bool,
) -> int:
    odfv = fs.get_on_demand_feature_view(on_demand_feature_view_name)

    input_record = pa.RecordBatch._import_from_c(input_arr_ptr, input_schema_ptr)

    output = odfv.get_transformed_features_df(
        input_record.to_pandas(), full_feature_names=full_feature_names
    )
    output_record = pa.RecordBatch.from_pandas(output)

    output_record.schema._export_to_c(output_schema_ptr)
    output_record._export_to_c(output_arr_ptr)

    return output_record.num_rows


def allocate_schema_and_array():
    c_schema = ffi.new("struct ArrowSchema*")
    ptr_schema = int(ffi.cast("uintptr_t", c_schema))

    c_array = ffi.new("struct ArrowArray*")
    ptr_array = int(ffi.cast("uintptr_t", c_array))
    return c_schema, ptr_schema, c_array, ptr_array


def map_to_record_batch(
    map: Dict[str, Union[List[Any], Value_pb2.RepeatedValue]],
    type_hint: Optional[Dict[str, ValueType]] = None,
) -> Tuple[pa.RecordBatch, pa.Schema]:
    fields = []
    columns = []
    type_hint = type_hint or {}

    for name, values in map.items():
        arr = _to_arrow(values, type_hint.get(name))
        fields.append((name, arr.type))
        columns.append(arr)

    schema = pa.schema(fields)
    batch = pa.RecordBatch.from_arrays(columns, schema=schema)
    return batch, schema


def record_batch_to_online_response(record_batch):
    resp = GetOnlineFeaturesResponse()

    for idx, field in enumerate(record_batch.schema):
        if field.name.endswith("__timestamp") or field.name.endswith("__status"):
            continue

        feature_vector = GetOnlineFeaturesResponse.FeatureVector(
            statuses=record_batch.columns[idx + 1].to_pylist(),
            event_timestamps=[
                Timestamp(seconds=seconds)
                for seconds in record_batch.columns[idx + 2].to_pylist()
            ],
        )

        if field.type == pa.null():
            feature_vector.values.extend(
                [Value_pb2.Value()] * len(record_batch.columns[idx])
            )
        else:
            if isinstance(field.type, pa.ListType):
                proto_list_class = ARROW_LIST_TYPE_TO_PROTO_LIST_CLASS[
                    field.type.value_type
                ]
                proto_field_name = ARROW_LIST_TYPE_TO_PROTO_FIELD[field.type.value_type]

                column = record_batch.columns[idx]
                if field.type.value_type == pa.time64("ns"):
                    column = column.cast(pa.list_(pa.int64()))

                for v in column.tolist():
                    feature_vector.values.append(
                        Value_pb2.Value(**{proto_field_name: proto_list_class(val=v)})
                    )
            else:
                proto_field_name = ARROW_TYPE_TO_PROTO_FIELD[field.type]

                column = record_batch.columns[idx]
                if field.type == pa.time64("ns"):
                    column = column.cast(pa.int64())

                for v in column.tolist():
                    feature_vector.values.append(
                        Value_pb2.Value(**{proto_field_name: v})
                    )

        resp.results.append(feature_vector)
        resp.metadata.feature_names.val.append(field.name)

    return resp
