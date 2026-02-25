"""Fast serialization utilities for Feature Server responses.

Matches the output format of MessageToDict with proto_json.patch() applied.
Values are serialized as native Python types (not wrapped dicts).
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types.Value_pb2 import Value

# FieldStatus enum mapping (protos/feast/serving/ServingService.proto)
_STATUS_NAMES: Dict[int, str] = {
    0: "INVALID",
    1: "PRESENT",
    2: "NULL_VALUE",
    3: "NOT_FOUND",
    4: "OUTSIDE_MAX_AGE",
}


def response_to_dict_fast(response: GetOnlineFeaturesResponse) -> Dict[str, Any]:
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

    return result


def _value_to_native(v: Value) -> Optional[Any]:
    """Convert a Value proto to native Python type (matches proto_json.patch() format)."""
    which = v.WhichOneof("val")
    if which is None or which == "null_val":
        return None
    elif "_list_" in which:
        return list(getattr(v, which).val)
    else:
        return getattr(v, which)


def _timestamp_to_str(ts) -> str:
    """Convert protobuf Timestamp to RFC 3339 format with Z suffix."""
    if ts.seconds == 0 and ts.nanos == 0:
        return "1970-01-01T00:00:00Z"
    dt = datetime.fromtimestamp(ts.seconds + ts.nanos / 1e9, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + (
        ".%06dZ" % (ts.nanos // 1000) if ts.nanos else "Z"
    )


def _metadata_to_dict(metadata) -> Dict[str, Any]:
    """Convert FeatureResponseMeta to dict (matches proto_json.patch() format)."""
    result: Dict[str, Any] = {}
    if metadata.HasField("feature_names"):
        result["feature_names"] = list(metadata.feature_names.val)
    return result
