import assertpy
import pytest
from google.protobuf.json_format import MessageToDict, Parse

from feast import proto_json
from feast.protos.feast.serving.ServingService_pb2 import (
    FeatureList,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import RepeatedValue

FeatureVector = GetOnlineFeaturesResponse.FeatureVector


@pytest.fixture(scope="module")
def proto_json_patch():
    proto_json.patch()


def test_feature_vector_values(proto_json_patch):
    # FeatureVector contains "repeated<feast.types.Value> values" proto field.
    # We want to test that feast.types.Value can take different types in JSON
    # without using additional structure (e.g. 1 instead of {int64_val: 1}).
    feature_vector_str = """{
        "values": [
            1,
            2.0,
            true,
            "foo",
            [1, 2, 3],
            [2.0, 3.0, 4.0, null],
            [true, false, true],
            ["foo", "bar", "foobar"]
        ]
    }"""
    feature_vector_proto = FeatureVector()
    Parse(feature_vector_str, feature_vector_proto)
    assertpy.assert_that(len(feature_vector_proto.values)).is_equal_to(8)
    assertpy.assert_that(feature_vector_proto.values[0].int64_val).is_equal_to(1)
    assertpy.assert_that(feature_vector_proto.values[1].double_val).is_equal_to(2.0)
    assertpy.assert_that(feature_vector_proto.values[2].bool_val).is_equal_to(True)
    assertpy.assert_that(feature_vector_proto.values[3].string_val).is_equal_to("foo")
    assertpy.assert_that(feature_vector_proto.values[4].int64_list_val.val).is_equal_to(
        [1, 2, 3]
    )
    # Can't directly check equality to [2.0, 3.0, 4.0, float("nan")], because float("nan") != float("nan")
    assertpy.assert_that(
        feature_vector_proto.values[5].double_list_val.val[:3]
    ).is_equal_to([2.0, 3.0, 4.0])
    assertpy.assert_that(feature_vector_proto.values[5].double_list_val.val[3]).is_nan()
    assertpy.assert_that(feature_vector_proto.values[6].bool_list_val.val).is_equal_to(
        [True, False, True]
    )
    assertpy.assert_that(
        feature_vector_proto.values[7].string_list_val.val
    ).is_equal_to(["foo", "bar", "foobar"])

    # Now convert protobuf back to json and check that
    feature_vector_json = MessageToDict(feature_vector_proto)
    assertpy.assert_that(len(feature_vector_json["values"])).is_equal_to(8)
    assertpy.assert_that(feature_vector_json["values"][0]).is_equal_to(1)
    assertpy.assert_that(feature_vector_json["values"][1]).is_equal_to(2.0)
    assertpy.assert_that(feature_vector_json["values"][2]).is_equal_to(True)
    assertpy.assert_that(feature_vector_json["values"][3]).is_equal_to("foo")
    assertpy.assert_that(feature_vector_json["values"][4]).is_equal_to([1, 2, 3])
    # Can't directly check equality to [2.0, 3.0, 4.0, float("nan")], because float("nan") != float("nan")
    assertpy.assert_that(feature_vector_json["values"][5][:3]).is_equal_to(
        [2.0, 3.0, 4.0]
    )
    assertpy.assert_that(feature_vector_json["values"][5][3]).is_nan()
    assertpy.assert_that(feature_vector_json["values"][6]).is_equal_to(
        [True, False, True]
    )
    assertpy.assert_that(feature_vector_json["values"][7]).is_equal_to(
        ["foo", "bar", "foobar"]
    )


def test_feast_repeated_value(proto_json_patch):
    # Make sure that RepeatedValue in JSON does not need the
    # additional structure (e.g. [1,2,3] instead of {"val": [1,2,3]})
    repeated_value_str = "[1,2,3]"
    repeated_value_proto = RepeatedValue()
    Parse(repeated_value_str, repeated_value_proto)
    assertpy.assert_that(len(repeated_value_proto.val)).is_equal_to(3)
    assertpy.assert_that(repeated_value_proto.val[0].int64_val).is_equal_to(1)
    assertpy.assert_that(repeated_value_proto.val[1].int64_val).is_equal_to(2)
    assertpy.assert_that(repeated_value_proto.val[2].int64_val).is_equal_to(3)
    # Now convert protobuf back to json and check that
    repeated_value_json = MessageToDict(repeated_value_proto)
    assertpy.assert_that(repeated_value_json).is_equal_to([1, 2, 3])


def test_feature_list(proto_json_patch):
    # Make sure that FeatureList in JSON does not need the additional structure
    # (e.g. ["foo", "bar"] instead of {"val": ["foo", "bar"]})
    feature_list_str = '["feature-a", "feature-b", "feature-c"]'
    feature_list_proto = FeatureList()
    Parse(feature_list_str, feature_list_proto)
    assertpy.assert_that(len(feature_list_proto.val)).is_equal_to(3)
    assertpy.assert_that(feature_list_proto.val[0]).is_equal_to("feature-a")
    assertpy.assert_that(feature_list_proto.val[1]).is_equal_to("feature-b")
    assertpy.assert_that(feature_list_proto.val[2]).is_equal_to("feature-c")
    # Now convert protobuf back to json and check that
    feature_list_json = MessageToDict(feature_list_proto)
    assertpy.assert_that(feature_list_json).is_equal_to(
        ["feature-a", "feature-b", "feature-c"]
    )
