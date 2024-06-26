import logging
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from google.protobuf.timestamp_pb2 import Timestamp
from pyarrow.cffi import ffi

from feast.errors import (
    FeatureNameCollisionError,
    RequestDataNotFoundInEntityRowsException,
)
from feast.feature_service import FeatureService
from feast.infra.feature_servers.base_config import FeatureLoggingConfig
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types import Value_pb2
from feast.repo_config import RepoConfig
from feast.types import from_value_type
from feast.value_type import ValueType

from .lib.embedded import (
    DataTable,
    LoggingOptions,
    NewOnlineFeatureService,
    OnlineFeatureServiceConfig,
)
from .lib.go import Slice_string
from .type_map import FEAST_TYPE_TO_ARROW_TYPE, arrow_array_to_array_of_proto

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

NANO_SECOND = 1
MICRO_SECOND = 1000 * NANO_SECOND
MILLI_SECOND = 1000 * MICRO_SECOND
SECOND = 1000 * MILLI_SECOND

logger = logging.getLogger(__name__)


class EmbeddedOnlineFeatureServer:
    def __init__(
        self, repo_path: str, repo_config: RepoConfig, feature_store: "FeatureStore"
    ):
        # keep callback in self to prevent it from GC
        self._transformation_callback = partial(transformation_callback, feature_store)
        self._logging_callback = partial(logging_callback, feature_store)

        self._config = OnlineFeatureServiceConfig(
            RepoPath=repo_path, RepoConfig=repo_config.json()
        )

        self._service = NewOnlineFeatureService(
            self._config,
            self._transformation_callback,
        )

        # This should raise an exception if there were any errors in NewOnlineFeatureService.
        self._service.CheckForInstantiationError()

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
        del record_batch
        return OnlineResponse(resp)

    def start_grpc_server(
        self,
        host: str,
        port: int,
        enable_logging: bool = True,
        logging_options: Optional[FeatureLoggingConfig] = None,
    ):
        if enable_logging:
            if logging_options:
                self._service.StartGprcServerWithLogging(
                    host,
                    port,
                    self._logging_callback,
                    LoggingOptions(
                        FlushInterval=logging_options.flush_interval_secs * SECOND,
                        WriteInterval=logging_options.write_to_disk_interval_secs
                        * SECOND,
                        EmitTimeout=logging_options.emit_timeout_micro_secs
                        * MICRO_SECOND,
                        ChannelCapacity=logging_options.queue_capacity,
                    ),
                )
            else:
                self._service.StartGprcServerWithLoggingDefaultOpts(
                    host, port, self._logging_callback
                )
        else:
            self._service.StartGprcServer(host, port)

    def start_http_server(
        self,
        host: str,
        port: int,
        enable_logging: bool = True,
        logging_options: Optional[FeatureLoggingConfig] = None,
    ):
        if enable_logging:
            if logging_options:
                self._service.StartHttpServerWithLogging(
                    host,
                    port,
                    self._logging_callback,
                    LoggingOptions(
                        FlushInterval=logging_options.flush_interval_secs * SECOND,
                        WriteInterval=logging_options.write_to_disk_interval_secs
                        * SECOND,
                        EmitTimeout=logging_options.emit_timeout_micro_secs
                        * MICRO_SECOND,
                        ChannelCapacity=logging_options.queue_capacity,
                    ),
                )
            else:
                self._service.StartHttpServerWithLoggingDefaultOpts(
                    host, port, self._logging_callback
                )
        else:
            self._service.StartHttpServer(host, port)

    def stop_grpc_server(self):
        self._service.StopGrpcServer()

    def stop_http_server(self):
        self._service.StopHttpServer()


def _to_arrow(value, type_hint: Optional[ValueType]) -> pa.Array:
    if isinstance(value, Value_pb2.RepeatedValue):
        _proto_to_arrow(value)

    if type_hint:
        feast_type = from_value_type(type_hint)
        if feast_type in FEAST_TYPE_TO_ARROW_TYPE:
            return pa.array(value, FEAST_TYPE_TO_ARROW_TYPE[feast_type])

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
    try:
        odfv = fs.get_on_demand_feature_view(on_demand_feature_view_name)

        input_record = pa.RecordBatch._import_from_c(input_arr_ptr, input_schema_ptr)

        # For some reason, the callback is called with `full_feature_names` as a 1 if True or 0 if false. This handles
        # the typeguard requirement.
        full_feature_names = bool(full_feature_names)

        if odfv.mode != "pandas":
            raise Exception(
                f"OnDemandFeatureView mode '{odfv.mode} not supported by EmbeddedOnlineFeatureServer."
            )

        output = odfv.get_transformed_features_df(  # type: ignore
            input_record.to_pandas(), full_feature_names=full_feature_names
        )
        output_record = pa.RecordBatch.from_pandas(output)

        output_record.schema._export_to_c(output_schema_ptr)
        output_record._export_to_c(output_arr_ptr)

        return output_record.num_rows
    except Exception as e:
        logger.exception(f"transformation callback failed with exception: {e}", e)
        return 0


def logging_callback(
    fs: "FeatureStore",
    feature_service_name: str,
    dataset_dir: str,
) -> bytes:
    feature_service = fs.get_feature_service(feature_service_name, allow_cache=True)
    try:
        fs.write_logged_features(logs=Path(dataset_dir), source=feature_service)
    except Exception as exc:
        return repr(exc).encode()

    return "".encode()  # no error


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
            feature_vector.values.extend(
                arrow_array_to_array_of_proto(field.type, record_batch.columns[idx])
            )

        resp.results.append(feature_vector)
        resp.metadata.feature_names.val.append(field.name)

    return resp
