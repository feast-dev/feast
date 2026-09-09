# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Unit tests for feature_server_utils.py

Tests the optimized convert_response_to_dict function to ensure it matches
the output format of MessageToDict with proto_json.patch() applied.

Related issue: https://github.com/feast-dev/feast/issues/6013
"""

import base64
import json
import time

import pytest
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp

import feast.proto_json as proto_json
from feast.feature_server_utils import (
    _STATUS_NAMES,
    _metadata_to_dict,
    _timestamp_to_str,
    _value_to_native,
    convert_response_to_dict,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
    GetOnlineFeaturesResponseMetadata,
)
from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    Int64Set,
    Map,
    RepeatedValue,
    StringList,
    StringSet,
    Value,
    ZonedTimestamp,
)


class TestValueToNative:
    """Tests for _value_to_native function (matches proto_json.patch() format)."""

    def test_null_value(self):
        v = Value()
        result = _value_to_native(v)
        assert result is None

    def test_explicit_null_val(self):
        v = Value(null_val=0)
        result = _value_to_native(v)
        assert result is None

    def test_double_val(self):
        v = Value(double_val=3.14159)
        result = _value_to_native(v)
        assert result == 3.14159

    def test_float_val(self):
        v = Value(float_val=2.5)
        result = _value_to_native(v)
        assert result == 2.5

    def test_int64_val(self):
        v = Value(int64_val=9223372036854775807)
        result = _value_to_native(v)
        assert result == 9223372036854775807

    def test_int32_val(self):
        v = Value(int32_val=42)
        result = _value_to_native(v)
        assert result == 42

    def test_string_val(self):
        v = Value(string_val="hello feast")
        result = _value_to_native(v)
        assert result == "hello feast"

    def test_bool_val(self):
        v = Value(bool_val=True)
        result = _value_to_native(v)
        assert result is True

    def test_bytes_val(self):
        data = b"\x00\x01\x02\x03"
        v = Value(bytes_val=data)
        result = _value_to_native(v)
        assert result == base64.b64encode(data).decode("ascii")

    def test_double_list_val(self):
        v = Value(double_list_val=DoubleList(val=[1.1, 2.2, 3.3]))
        result = _value_to_native(v)
        assert result == [1.1, 2.2, 3.3]

    def test_float_list_val(self):
        v = Value(float_list_val=FloatList(val=[1.5, 2.5]))
        result = _value_to_native(v)
        assert result == [1.5, 2.5]

    def test_int64_list_val(self):
        v = Value(int64_list_val=Int64List(val=[100, 200, 300]))
        result = _value_to_native(v)
        assert result == [100, 200, 300]

    def test_int32_list_val(self):
        v = Value(int32_list_val=Int32List(val=[1, 2, 3]))
        result = _value_to_native(v)
        assert result == [1, 2, 3]

    def test_string_list_val(self):
        v = Value(string_list_val=StringList(val=["a", "b", "c"]))
        result = _value_to_native(v)
        assert result == ["a", "b", "c"]

    def test_bool_list_val(self):
        v = Value(bool_list_val=BoolList(val=[True, False, True]))
        result = _value_to_native(v)
        assert result == [True, False, True]

    def test_unix_timestamp_val(self):
        v = Value(unix_timestamp_val=1609459200)
        result = _value_to_native(v)
        assert result == 1609459200

    def test_bytes_list_val(self):
        v = Value(bytes_list_val=BytesList(val=[b"\x00\x01", b"\x02\x03"]))
        result = _value_to_native(v)
        assert result == [
            base64.b64encode(b"\x00\x01").decode("ascii"),
            base64.b64encode(b"\x02\x03").decode("ascii"),
        ]

    def test_unix_timestamp_list_val(self):
        v = Value(unix_timestamp_list_val=Int64List(val=[1609459200, 1609545600]))
        result = _value_to_native(v)
        assert result == [1609459200, 1609545600]

    def test_string_set_val(self):
        v = Value(string_set_val=StringSet(val=["x", "y", "z"]))
        result = _value_to_native(v)
        assert set(result) == {"x", "y", "z"}

    def test_int64_set_val(self):
        v = Value(int64_set_val=Int64Set(val=[10, 20, 30]))
        result = _value_to_native(v)
        assert set(result) == {10, 20, 30}

    def test_list_val_nested_values(self):
        inner = RepeatedValue()
        inner.val.append(Value(int64_val=1))
        inner.val.append(Value(string_val="a"))
        inner.val.append(Value())
        v = Value(list_val=inner)
        result = _value_to_native(v)
        assert result == [1, "a", None]

    def test_set_val_nested_values(self):
        inner = RepeatedValue()
        inner.val.append(Value(bool_val=True))
        inner.val.append(Value(double_val=3.14))
        v = Value(set_val=inner)
        result = _value_to_native(v)
        assert result == [True, 3.14]

    def test_map_val(self):
        m = Map()
        m.val["key1"].CopyFrom(Value(int64_val=42))
        m.val["key2"].CopyFrom(Value(string_val="hello"))
        v = Value(map_val=m)
        result = _value_to_native(v)
        assert result == {"key1": 42, "key2": "hello"}

    def test_map_val_nested_null(self):
        m = Map()
        m.val["present"].CopyFrom(Value(int32_val=7))
        m.val["missing"].CopyFrom(Value())
        v = Value(map_val=m)
        result = _value_to_native(v)
        assert result["present"] == 7
        assert result["missing"] is None

    def test_json_val(self):
        v = Value(json_val='{"foo": 1}')
        result = _value_to_native(v)
        assert result == '{"foo": 1}'

    def test_zoned_timestamp_val(self):
        # 2026-06-09T16:00:00Z viewed in America/Los_Angeles (= 09:00 -07:00).
        v = Value(
            zoned_timestamp_val=ZonedTimestamp(
                unix_timestamp=1781020800, zone="America/Los_Angeles"
            )
        )
        result = _value_to_native(v)
        # Rendered as an ISO 8601 string in the stored zone, and JSON-serializable.
        assert isinstance(result, str)
        assert result == "2026-06-09T09:00:00-07:00"
        json.dumps(result)

    def test_zoned_timestamp_val_utc(self):
        v = Value(
            zoned_timestamp_val=ZonedTimestamp(unix_timestamp=1781020800, zone="")
        )
        result = _value_to_native(v)
        assert result == "2026-06-09T16:00:00+00:00"
        json.dumps(result)


class TestTimestampToStr:
    """Tests for _timestamp_to_str function."""

    def test_zero_timestamp(self):
        ts = Timestamp(seconds=0, nanos=0)
        result = _timestamp_to_str(ts)
        assert result == "1970-01-01T00:00:00Z"

    def test_valid_timestamp(self):
        ts = Timestamp(seconds=1609459200, nanos=0)
        result = _timestamp_to_str(ts)
        assert result == "2021-01-01T00:00:00Z"

    def test_timestamp_with_millis(self):
        ts = Timestamp(seconds=1609459200, nanos=500000000)
        result = _timestamp_to_str(ts)
        assert result == "2021-01-01T00:00:00.500Z"

    def test_timestamp_with_micros(self):
        ts = Timestamp(seconds=1609459200, nanos=123456000)
        result = _timestamp_to_str(ts)
        assert result == "2021-01-01T00:00:00.123456Z"

    def test_timestamp_with_nanos(self):
        ts = Timestamp(seconds=1609459200, nanos=123456789)
        result = _timestamp_to_str(ts)
        assert result == "2021-01-01T00:00:00.123456789Z"

    def test_timestamp_high_nanos_no_float_rounding(self):
        ts = Timestamp(seconds=1609459200, nanos=999999999)
        result = _timestamp_to_str(ts)
        assert result == "2021-01-01T00:00:00.999999999Z"


class TestMetadataToDict:
    """Tests for _metadata_to_dict function (matches proto_json.patch() format)."""

    def test_empty_metadata(self):
        metadata = GetOnlineFeaturesResponseMetadata()
        result = _metadata_to_dict(metadata)
        assert result == {}

    def test_metadata_with_feature_names(self):
        metadata = GetOnlineFeaturesResponseMetadata()
        metadata.feature_names.val.extend(["feature1", "feature2", "feature3"])
        result = _metadata_to_dict(metadata)
        assert result == {"feature_names": ["feature1", "feature2", "feature3"]}


class TestConvertResponseToDict:
    """Tests for the main convert_response_to_dict function."""

    def test_empty_response(self):
        response = GetOnlineFeaturesResponse()
        result = convert_response_to_dict(response)
        assert result == {"results": []}

    def test_single_feature_vector(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(string_val="test"))
        fv.statuses.append(FieldStatus.PRESENT)

        result = convert_response_to_dict(response)

        assert len(result["results"]) == 1
        assert result["results"][0]["values"] == ["test"]
        assert result["results"][0]["statuses"] == ["PRESENT"]

    def test_multiple_feature_vectors(self):
        response = GetOnlineFeaturesResponse()

        fv1 = response.results.add()
        fv1.values.append(Value(int64_val=100))
        fv1.statuses.append(FieldStatus.PRESENT)

        fv2 = response.results.add()
        fv2.values.append(Value(double_val=3.14))
        fv2.statuses.append(FieldStatus.NOT_FOUND)

        result = convert_response_to_dict(response)

        assert len(result["results"]) == 2
        assert result["results"][0]["values"] == [100]
        assert result["results"][0]["statuses"] == ["PRESENT"]
        assert result["results"][1]["values"] == [3.14]
        assert result["results"][1]["statuses"] == ["NOT_FOUND"]

    def test_response_with_metadata(self):
        response = GetOnlineFeaturesResponse()
        response.metadata.feature_names.val.extend(["driver_id", "driver_rating"])

        fv = response.results.add()
        fv.values.append(Value(int64_val=123))
        fv.statuses.append(FieldStatus.PRESENT)

        result = convert_response_to_dict(response)

        assert "metadata" in result
        assert result["metadata"]["feature_names"] == ["driver_id", "driver_rating"]

    def test_response_with_timestamps(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(string_val="test"))
        fv.statuses.append(FieldStatus.PRESENT)
        ts = fv.event_timestamps.add()
        ts.seconds = 1609459200

        result = convert_response_to_dict(response)

        assert len(result["results"][0]["event_timestamps"]) == 1
        assert "2021-01-01" in result["results"][0]["event_timestamps"][0]

    def test_all_status_types(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()

        for status in [
            FieldStatus.INVALID,
            FieldStatus.PRESENT,
            FieldStatus.NULL_VALUE,
            FieldStatus.NOT_FOUND,
            FieldStatus.OUTSIDE_MAX_AGE,
        ]:
            fv.values.append(Value(int32_val=1))
            fv.statuses.append(status)

        result = convert_response_to_dict(response)

        expected_statuses = [
            "INVALID",
            "PRESENT",
            "NULL_VALUE",
            "NOT_FOUND",
            "OUTSIDE_MAX_AGE",
        ]
        assert result["results"][0]["statuses"] == expected_statuses

    def test_status_field_included_when_true(self):
        response = GetOnlineFeaturesResponse()
        response.status = True
        result = convert_response_to_dict(response)
        assert result.get("status") is True

    def test_status_field_omitted_when_false(self):
        response = GetOnlineFeaturesResponse()
        result = convert_response_to_dict(response)
        assert "status" not in result

    def test_null_values_become_none(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value())
        fv.values.append(Value(null_val=0))
        fv.statuses.extend([FieldStatus.NULL_VALUE, FieldStatus.NULL_VALUE])

        result = convert_response_to_dict(response)

        assert result["results"][0]["values"] == [None, None]

    def test_list_values_are_native_lists(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(int64_list_val=Int64List(val=[1, 2, 3])))
        fv.values.append(Value(string_list_val=StringList(val=["a", "b"])))
        fv.statuses.extend([FieldStatus.PRESENT, FieldStatus.PRESENT])

        result = convert_response_to_dict(response)

        assert result["results"][0]["values"] == [[1, 2, 3], ["a", "b"]]


class TestConvertResponseToDictConsistency:
    """Tests ensuring convert_response_to_dict matches MessageToDict with patch."""

    @pytest.fixture(autouse=True)
    def setup_proto_json_patch(self):
        proto_json.patch()

    def _build_complex_response(
        self, num_features: int = 10
    ) -> GetOnlineFeaturesResponse:
        response = GetOnlineFeaturesResponse()
        feature_names = [f"feature_{i}" for i in range(num_features)]
        response.metadata.feature_names.val.extend(feature_names)

        for i in range(num_features):
            fv = response.results.add()
            if i % 4 == 0:
                fv.values.append(Value(int64_val=i * 100))
            elif i % 4 == 1:
                fv.values.append(Value(double_val=i * 0.1))
            elif i % 4 == 2:
                fv.values.append(Value(string_val=f"value_{i}"))
            else:
                fv.values.append(Value())
            fv.statuses.append(FieldStatus.PRESENT)

        return response

    def test_values_match_patched_message_to_dict(self):
        """Ensure value serialization matches proto_json.patch() format."""
        response = self._build_complex_response(8)

        fast_result = convert_response_to_dict(response)
        standard_result = MessageToDict(response, preserving_proto_field_name=True)

        assert set(fast_result.keys()) == set(standard_result.keys())
        assert len(fast_result["results"]) == len(standard_result["results"])

        for i in range(len(fast_result["results"])):
            fast_values = fast_result["results"][i]["values"]
            standard_values = standard_result["results"][i]["values"]
            assert fast_values == standard_values, f"Mismatch at result {i}"

    def test_metadata_matches_patched_format(self):
        """Ensure metadata format matches proto_json.patch() format."""
        response = self._build_complex_response(5)

        fast_result = convert_response_to_dict(response)
        standard_result = MessageToDict(response, preserving_proto_field_name=True)

        if "metadata" in standard_result:
            assert "metadata" in fast_result
            assert (
                fast_result["metadata"]["feature_names"]
                == standard_result["metadata"]["feature_names"]
            )

    def test_float_val_precision_matches_message_to_dict(self):
        """float32 storage causes truncation; both paths must return identical values."""
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        # 3.14 is not exactly representable as float32; both implementations
        # should return the same truncated float64 representation
        fv.values.append(Value(float_val=3.14))
        fv.statuses.append(FieldStatus.PRESENT)

        fast_result = convert_response_to_dict(response)
        standard_result = MessageToDict(response, preserving_proto_field_name=True)

        assert (
            fast_result["results"][0]["values"]
            == standard_result["results"][0]["values"]
        )

    def test_double_val_precision(self):
        """double_val is returned as a Python float (shortest round-trip form).

        The upstream code passed float_precision=18 to MessageToDict, which forced 18
        significant digits for doubles. Our implementation returns native Python floats
        serialized by json.dumps using Python 3.1+ shortest-round-trip representation
        (~15–17 sig digits). The value is identical when round-tripped through float64;
        the only difference is how many trailing digits appear in the JSON string.
        This is an intentional trade-off for speed and is safe for all ML feature values.
        """
        import struct

        # Use a value with many significant digits
        pi = 3.141592653589793
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(double_val=pi))
        fv.statuses.append(FieldStatus.PRESENT)

        result = convert_response_to_dict(response)
        value = result["results"][0]["values"][0]

        # Value must round-trip correctly through json.dumps
        assert value == pi
        round_tripped = json.loads(json.dumps(value))
        assert round_tripped == pi
        # Verify it encodes as the same float64 bit pattern
        assert struct.pack("d", value) == struct.pack("d", pi)

    def test_set_types_return_flat_list(self):
        """set types (string_set_val, int64_set_val, etc.) return flat lists.

        Note: proto_json.patch() does not explicitly handle _set_ types — they fall
        through to the else branch and return the raw proto object, which MessageToDict
        then serializes as {"val": [...]}. Our implementation normalizes these to flat
        lists, which is more useful for API consumers and consistent with list types.
        """
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(string_set_val=StringSet(val=["a", "b", "c"])))
        fv.statuses.append(FieldStatus.PRESENT)

        result = convert_response_to_dict(response)
        values = result["results"][0]["values"]
        assert isinstance(values[0], list), "set type should be a flat list"
        assert set(values[0]) == {"a", "b", "c"}

    def test_bytes_val_is_base64_encoded(self):
        """bytes_val is base64-encoded so JSONResponse can serialize it.

        This intentionally differs from proto_json.patch() which returns raw bytes.
        Raw bytes are not JSON-serializable; base64 is the standard protobuf JSON
        encoding for bytes fields and is safe for all HTTP clients.
        """
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        data = b"\x00\x01\x02\xff"
        fv.values.append(Value(bytes_val=data))
        fv.statuses.append(FieldStatus.PRESENT)

        result = convert_response_to_dict(response)
        encoded = result["results"][0]["values"][0]

        assert encoded == base64.b64encode(data).decode("ascii")
        # Must be JSON-serializable
        assert json.dumps(encoded)


class TestJsonSerializability:
    """Ensure convert_response_to_dict output is always JSON-serializable."""

    def test_complex_types_are_json_serializable(self):
        """map_val, list_val, set_val must not leave proto objects in the output."""
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()

        # map_val
        m = Map()
        m.val["k"].CopyFrom(Value(int64_val=1))
        fv.values.append(Value(map_val=m))

        # list_val (RepeatedValue)
        inner = RepeatedValue()
        inner.val.append(Value(string_val="a"))
        fv.values.append(Value(list_val=inner))

        # int64_set_val
        fv.values.append(Value(int64_set_val=Int64Set(val=[10, 20])))

        fv.statuses.extend(
            [FieldStatus.PRESENT, FieldStatus.PRESENT, FieldStatus.PRESENT]
        )

        result = convert_response_to_dict(response)
        # must not raise
        serialized = json.dumps(result)
        assert serialized  # non-empty


class TestProtobufCompatibility:
    """Regression tests for protobuf >= 7.34.0 compatibility (issue #6435).

    protobuf 7.34.0 removed the float_precision kwarg from MessageToDict.
    convert_response_to_dict must never call MessageToDict, so it is immune
    to this breaking change regardless of the installed protobuf version.
    """

    def test_no_message_to_dict_dependency(self):
        """convert_response_to_dict must not call MessageToDict internally."""
        import unittest.mock as mock

        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(double_val=3.141592653589793))
        fv.values.append(Value(int64_val=42))
        fv.values.append(Value(string_val="hello"))
        fv.statuses.extend(
            [FieldStatus.PRESENT, FieldStatus.PRESENT, FieldStatus.PRESENT]
        )
        response.metadata.feature_names.val.extend(["f1", "f2", "f3"])

        with mock.patch(
            "google.protobuf.json_format.MessageToDict",
            side_effect=AssertionError("MessageToDict must not be called"),
        ):
            result = convert_response_to_dict(response)

        assert len(result["results"]) == 1
        assert result["results"][0]["values"] == [3.141592653589793, 42, "hello"]
        assert result["metadata"]["feature_names"] == ["f1", "f2", "f3"]

    def test_double_precision_without_float_precision_kwarg(self):
        """Doubles must round-trip without the removed float_precision kwarg."""
        values = [
            1e-300,
            1.7976931348623157e308,
            2.2250738585072014e-308,
            0.1 + 0.2,
        ]
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        for v in values:
            fv.values.append(Value(double_val=v))
            fv.statuses.append(FieldStatus.PRESENT)

        result = convert_response_to_dict(response)
        for i, expected in enumerate(values):
            actual = result["results"][0]["values"][i]
            assert actual == expected, f"Mismatch at index {i}: {actual} != {expected}"
            round_tripped = json.loads(json.dumps(actual))
            assert round_tripped == expected


class TestPerformance:
    """Performance tests to validate the optimization claim."""

    def _build_large_response(
        self, num_entities: int = 50, num_features: int = 100
    ) -> GetOnlineFeaturesResponse:
        response = GetOnlineFeaturesResponse()
        feature_names = [f"feature_{i}" for i in range(num_features)]
        response.metadata.feature_names.val.extend(feature_names)

        for i in range(num_features):
            fv = response.results.add()
            for j in range(num_entities):
                if i % 4 == 0:
                    fv.values.append(Value(int64_val=j * 100 + i))
                elif i % 4 == 1:
                    fv.values.append(Value(double_val=j * 0.1 + i))
                elif i % 4 == 2:
                    fv.values.append(Value(string_val=f"entity_{j}_feature_{i}"))
                else:
                    fv.values.append(Value(bool_val=j % 2 == 0))
                fv.statuses.append(FieldStatus.PRESENT)

        return response

    @pytest.mark.slow
    def test_faster_than_message_to_dict(self):
        """Verify convert_response_to_dict is faster than MessageToDict."""
        proto_json.patch()
        response = self._build_large_response(num_entities=50, num_features=100)
        iterations = 100

        for _ in range(10):
            convert_response_to_dict(response)
            MessageToDict(response, preserving_proto_field_name=True)

        start = time.process_time()
        for _ in range(iterations):
            convert_response_to_dict(response)
        fast_time = time.process_time() - start

        start = time.process_time()
        for _ in range(iterations):
            MessageToDict(response, preserving_proto_field_name=True)
        standard_time = time.process_time() - start

        speedup = standard_time / fast_time
        print(f"\nPerformance: fast={fast_time:.3f}s, standard={standard_time:.3f}s")
        print(f"Speedup: {speedup:.2f}x")

        assert speedup >= 1.2, f"Expected at least 1.2x speedup, got {speedup:.2f}x"


class TestStatusNames:
    """Tests for the status name mapping."""

    def test_all_status_codes_mapped(self):
        assert 0 in _STATUS_NAMES  # INVALID
        assert 1 in _STATUS_NAMES  # PRESENT
        assert 2 in _STATUS_NAMES  # NULL_VALUE
        assert 3 in _STATUS_NAMES  # NOT_FOUND
        assert 4 in _STATUS_NAMES  # OUTSIDE_MAX_AGE

    def test_unknown_status_returns_invalid(self):
        assert _STATUS_NAMES.get(999, "INVALID") == "INVALID"
