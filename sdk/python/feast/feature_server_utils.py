"""Fast serialization utilities for Feature Server responses.

Matches the output format of MessageToDict with proto_json.patch() applied.
Values are serialized as native Python types (not wrapped dicts).
"""

import base64
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types.Value_pb2 import Value

logger = logging.getLogger(__name__)

# FieldStatus enum mapping (protos/feast/serving/ServingService.proto)
_STATUS_NAMES: Dict[int, str] = {
    0: "INVALID",
    1: "PRESENT",
    2: "NULL_VALUE",
    3: "NOT_FOUND",
    4: "OUTSIDE_MAX_AGE",
}


def convert_response_to_dict(response: GetOnlineFeaturesResponse) -> Dict[str, Any]:
    """Convert GetOnlineFeaturesResponse to a JSON-serializable dict.

    Matches the structure produced by MessageToDict(proto, preserving_proto_field_name=True)
    with proto_json.patch() applied, with one intentional difference:

    - double_val fields are returned as Python float objects (json.dumps uses Python 3.1+
      shortest round-trip form, ~15-17 sig digits) rather than 18 fixed significant digits
      (float_precision=18). Values are numerically identical; only the JSON string length
      may differ. This is safe for all ML feature types and avoids unnecessary precision
      overhead.
    """
    result: Dict[str, Any] = {
        "results": [
            {
                "values": [_value_to_native(v) for v in feature_vector.values],
                "statuses": [
                    _STATUS_NAMES.get(s, "INVALID") for s in feature_vector.statuses
                ],
                **(
                    {
                        "event_timestamps": [
                            _timestamp_to_str(ts)
                            for ts in feature_vector.event_timestamps
                        ]
                    }
                    if feature_vector.event_timestamps
                    else {}
                ),
            }
            for feature_vector in response.results
        ]
    }

    if response.HasField("metadata"):
        result["metadata"] = _metadata_to_dict(response.metadata)

    if response.status:
        result["status"] = response.status

    return result


def _value_to_native(v: Value) -> Optional[Any]:
    """Convert a Value proto to a JSON-serializable Python type.

    bytes_val and bytes_list_val are base64-encoded (RFC 4648) so that
    JSONResponse can serialize them without TypeError. This matches standard
    protobuf JSON encoding for bytes fields and is safe for all HTTP clients.
    """
    which = v.WhichOneof("val")
    if which is None or which == "null_val":
        return None
    # bytes must be base64-encoded for JSON serialization
    elif which == "bytes_val":
        return base64.b64encode(v.bytes_val).decode("ascii")
    # RepeatedValue — nested Values that must be recursively converted
    elif which in ("list_val", "set_val"):
        return [_value_to_native(nested) for nested in getattr(v, which).val]
    # Map<string, Value> — recursively convert nested Values
    elif which in ("map_val", "struct_val"):
        return {k: _value_to_native(vv) for k, vv in getattr(v, which).val.items()}
    # MapList — list of Map<string, Value>
    elif which in ("map_list_val", "struct_list_val"):
        return [
            {k: _value_to_native(vv) for k, vv in m.val.items()}
            for m in getattr(v, which).val
        ]
    # scalar_map_val has non-string keys; full conversion requires extra work and
    # this type is not returned by standard get_online_features paths today.
    elif which == "scalar_map_val":
        logger.warning(
            "scalar_map_val is not yet supported by convert_response_to_dict; value will be None"
        )
        return None
    # zoned_timestamp_val is a ZonedTimestamp message; serialize as an ISO 8601
    # string in its stored zone so JSONResponse can encode it (the raw message
    # is not JSON-serializable).
    elif which == "zoned_timestamp_val":
        return _zoned_timestamp_to_str(v.zoned_timestamp_val)
    # bytes_list_val / bytes_set_val — base64-encode each element
    elif which in ("bytes_list_val", "bytes_set_val"):
        return [base64.b64encode(b).decode("ascii") for b in getattr(v, which).val]
    # All other list/set types have scalar .val fields
    elif "_list_" in which or "_set_" in which:
        return list(getattr(v, which).val)
    else:
        return getattr(v, which)


def _zoned_timestamp_to_str(zoned) -> Optional[str]:
    """Convert a ZonedTimestamp proto to an ISO 8601 string in its stored zone.

    A null instant (NaT sentinel) returns None. The zone is resolved via the
    same logic used on the read path so IANA names and fixed offsets round-trip.
    """
    from feast.type_map import NULL_TIMESTAMP_INT_VALUE, _zone_from_name

    if zoned.unix_timestamp == NULL_TIMESTAMP_INT_VALUE:
        return None
    tz = _zone_from_name(zoned.zone)
    return datetime.fromtimestamp(zoned.unix_timestamp, tz=tz).isoformat()


def _timestamp_to_str(ts) -> str:
    """Convert protobuf Timestamp to RFC 3339 format with Z suffix.

    Uses adaptive precision to match MessageToDict output:
    - No fractional seconds when nanos == 0
    - 3 digits (milliseconds) when nanos % 1_000_000 == 0
    - 6 digits (microseconds) when nanos % 1_000 == 0
    - 9 digits (nanoseconds) otherwise
    """
    if ts.seconds == 0 and ts.nanos == 0:
        return "1970-01-01T00:00:00Z"
    dt = datetime.fromtimestamp(ts.seconds, tz=timezone.utc)
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    nanos = ts.nanos
    if nanos == 0:
        return base + "Z"
    elif nanos % 1_000_000 == 0:
        return base + ".%03dZ" % (nanos // 1_000_000)
    elif nanos % 1_000 == 0:
        return base + ".%06dZ" % (nanos // 1_000)
    else:
        return base + ".%09dZ" % nanos


def _metadata_to_dict(metadata) -> Dict[str, Any]:
    """Convert FeatureResponseMeta to dict (matches proto_json.patch() format)."""
    result: Dict[str, Any] = {}
    if metadata.HasField("feature_names"):
        result["feature_names"] = list(metadata.feature_names.val)
    return result
