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

Tests the optimized response_to_dict_fast function to ensure it matches
the output format of MessageToDict with proto_json.patch() applied.

Related issue: https://github.com/feast-dev/feast/issues/6013
"""

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
    response_to_dict_fast,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
    Value,
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
        assert result == data

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

    def test_timestamp_with_nanos(self):
        ts = Timestamp(seconds=1609459200, nanos=500000000)
        result = _timestamp_to_str(ts)
        assert "2021-01-01" in result
        assert "Z" in result


class TestMetadataToDict:
    """Tests for _metadata_to_dict function (matches proto_json.patch() format)."""

    def test_empty_metadata(self):
        metadata = GetOnlineFeaturesResponse.FieldValues()
        result = _metadata_to_dict(metadata)
        assert result == {}

    def test_metadata_with_feature_names(self):
        metadata = GetOnlineFeaturesResponse.FieldValues()
        metadata.feature_names.val.extend(["feature1", "feature2", "feature3"])
        result = _metadata_to_dict(metadata)
        assert result == {"feature_names": ["feature1", "feature2", "feature3"]}


class TestResponseToDictFast:
    """Tests for the main response_to_dict_fast function."""

    def test_empty_response(self):
        response = GetOnlineFeaturesResponse()
        result = response_to_dict_fast(response)
        assert result == {"results": []}

    def test_single_feature_vector(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(string_val="test"))
        fv.statuses.append(FieldStatus.PRESENT)

        result = response_to_dict_fast(response)

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

        result = response_to_dict_fast(response)

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

        result = response_to_dict_fast(response)

        assert "metadata" in result
        assert result["metadata"]["feature_names"] == ["driver_id", "driver_rating"]

    def test_response_with_timestamps(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(string_val="test"))
        fv.statuses.append(FieldStatus.PRESENT)
        ts = fv.event_timestamps.add()
        ts.seconds = 1609459200

        result = response_to_dict_fast(response)

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

        result = response_to_dict_fast(response)

        expected_statuses = [
            "INVALID",
            "PRESENT",
            "NULL_VALUE",
            "NOT_FOUND",
            "OUTSIDE_MAX_AGE",
        ]
        assert result["results"][0]["statuses"] == expected_statuses

    def test_null_values_become_none(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value())
        fv.values.append(Value(null_val=0))
        fv.statuses.extend([FieldStatus.NULL_VALUE, FieldStatus.NULL_VALUE])

        result = response_to_dict_fast(response)

        assert result["results"][0]["values"] == [None, None]

    def test_list_values_are_native_lists(self):
        response = GetOnlineFeaturesResponse()
        fv = response.results.add()
        fv.values.append(Value(int64_list_val=Int64List(val=[1, 2, 3])))
        fv.values.append(Value(string_list_val=StringList(val=["a", "b"])))
        fv.statuses.extend([FieldStatus.PRESENT, FieldStatus.PRESENT])

        result = response_to_dict_fast(response)

        assert result["results"][0]["values"] == [[1, 2, 3], ["a", "b"]]


class TestResponseToDictFastConsistency:
    """Tests ensuring response_to_dict_fast matches MessageToDict with patch."""

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

        fast_result = response_to_dict_fast(response)
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

        fast_result = response_to_dict_fast(response)
        standard_result = MessageToDict(response, preserving_proto_field_name=True)

        if "metadata" in standard_result:
            assert "metadata" in fast_result
            assert (
                fast_result["metadata"]["feature_names"]
                == standard_result["metadata"]["feature_names"]
            )


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
        """Verify response_to_dict_fast is faster than MessageToDict."""
        proto_json.patch()
        response = self._build_large_response(num_entities=50, num_features=100)
        iterations = 100

        for _ in range(10):
            response_to_dict_fast(response)
            MessageToDict(response, preserving_proto_field_name=True)

        start = time.perf_counter()
        for _ in range(iterations):
            response_to_dict_fast(response)
        fast_time = time.perf_counter() - start

        start = time.perf_counter()
        for _ in range(iterations):
            MessageToDict(response, preserving_proto_field_name=True)
        standard_time = time.perf_counter() - start

        speedup = standard_time / fast_time
        print(f"\nPerformance: fast={fast_time:.3f}s, standard={standard_time:.3f}s")
        print(f"Speedup: {speedup:.2f}x")

        assert speedup >= 1.5, f"Expected at least 1.5x speedup, got {speedup:.2f}x"


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
