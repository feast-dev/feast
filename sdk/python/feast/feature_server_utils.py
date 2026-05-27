"""Fast serialization utilities for Feature Server responses.

Matches the output format of MessageToDict with proto_json.patch() applied.
Values are serialized as native Python types (not wrapped dicts).
"""

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
    """Convert GetOnlineFeaturesResponse to dict (matches proto_json.patch() format)."""
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
    """Convert a Value proto to native Python type (matches proto_json.patch() format).

    Note: proto_json.patch() modifies MessageToDict to return raw bytes instead of
    base64 encoding, so we return raw bytes here to match that behavior.
    """
    which = v.WhichOneof("val")
    if which is None or which == "null_val":
        return None
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
    # All simple list/set types (.val is a repeated scalar field)
    elif "_list_" in which or "_set_" in which:
        return list(getattr(v, which).val)
    else:
        return getattr(v, which)


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
