from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
from pyarrow.cffi import ffi

from feast.errors import FeatureNameCollisionError
from feast.feature_service import FeatureService
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types.Value_pb2 import RepeatedValue, Value
from feast.repo_config import RepoConfig

from .lib.embedded import DataTable, NewOnlineFeatureService, OnlineFeatureServiceConfig
from .lib.go import Slice_string

ARROW_TYPE_TO_PROTO_FIELD = {
    pa.int32(): "int32_val",
    pa.int64(): "int64_val",
    pa.float32(): "float_val",
    pa.float64(): "double_val",
    pa.bool_(): "bool_val",
    pa.string(): "string_val",
    pa.binary(): "bytes_val",
}


class EmbeddedOnlineFeatureServer:
    def __init__(self, repo_path: str, repo_config: RepoConfig):
        self._service = NewOnlineFeatureService(
            OnlineFeatureServiceConfig(
                RepoPath=repo_path, RepoConfig=repo_config.json()
            )
        )

    def get_online_features(
        self,
        features_refs: List[str],
        feature_service: Optional[FeatureService],
        entities: Dict[str, Union[List[Any], RepeatedValue]],
        project: str,
        full_feature_names: bool = False,
    ):
        entity_fields = []
        entity_arrays = []
        for entity_name, entity_values in entities.items():
            arr = _to_arrow(entity_values)
            entity_fields.append((entity_name, arr.type))
            entity_arrays.append(arr)

        schema = pa.schema(entity_fields)
        batch = pa.RecordBatch.from_arrays(entity_arrays, schema=schema)

        # Here we create C structures that will be shared between Python and Go.
        # We will pass entities as arrow Record Batch to Go part (in_c_array & in_c_schema)
        # and receive features as Record Batch from Go (out_c_array & out_c_schema)
        # This objects needs to be initialized here in order to correctly
        # free them later using Python GC.
        out_c_schema = ffi.new("struct ArrowSchema*")
        out_ptr_schema = int(ffi.cast("uintptr_t", out_c_schema))

        out_c_array = ffi.new("struct ArrowArray*")
        out_ptr_array = int(ffi.cast("uintptr_t", out_c_array))

        in_c_schema = ffi.new("struct ArrowSchema*")
        in_ptr_schema = int(ffi.cast("uintptr_t", in_c_schema))

        in_c_array = ffi.new("struct ArrowArray*")
        in_ptr_array = int(ffi.cast("uintptr_t", in_c_array))

        schema._export_to_c(in_ptr_schema)
        batch._export_to_c(in_ptr_array)
        try:
            self._service.GetOnlineFeatures(
                featureRefs=Slice_string(features_refs),
                featureServiceName=feature_service and feature_service.name or "",
                entities=DataTable(SchemaPtr=in_ptr_schema, DataPtr=in_ptr_array),
                projectName=project,
                fullFeatureNames=full_feature_names,
                output=DataTable(SchemaPtr=out_ptr_schema, DataPtr=out_ptr_array),
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

            raise

        result = pa.RecordBatch._import_from_c(out_ptr_array, out_ptr_schema)

        resp = GetOnlineFeaturesResponse()

        for idx, field in enumerate(result.schema):
            feature_vector = GetOnlineFeaturesResponse.FeatureVector()

            if field.type == pa.null():
                feature_vector.values.extend([Value()] * len(result.columns[idx]))
            else:
                proto_field_name = ARROW_TYPE_TO_PROTO_FIELD[field.type]
                for v in result.columns[idx].tolist():
                    feature_vector.values.append(Value(**{proto_field_name: v}))

            resp.results.append(feature_vector)
            resp.metadata.feature_names.val.append(field.name)

        return OnlineResponse(resp)


def _to_arrow(value) -> pa.Array:
    if isinstance(value, RepeatedValue):
        _proto_to_arrow(value)

    return pa.array(value)


def _proto_to_arrow(value: RepeatedValue) -> pa.Array:
    """
    ToDo: support entity rows already packed in protos
    """
    raise NotImplementedError
