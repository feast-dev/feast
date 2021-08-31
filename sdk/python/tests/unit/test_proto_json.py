import assertpy
import pytest
from google.protobuf.json_format import MessageToDict, Parse

from feast import proto_json
from feast.protos.feast.serving.ServingService_pb2 import (
    FeatureList,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import RepeatedValue

FieldValues = GetOnlineFeaturesResponse.FieldValues


@pytest.fixture(scope="module")
def proto_json_patch():
    proto_json.patch()


def test_feast_value(proto_json_patch):
    # FieldValues contains "map<string, feast.types.Value> fields" proto field.
    # We want to test that feast.types.Value can take different types in JSON
    # without using additional structure (e.g. 1 instead of {int64_val: 1}).
    field_values_str = """{
        "fields": {
            "a": 1,
            "b": 2.0,
            "c": true,
            "d": "foo",
            "e": [1, 2, 3],
            "f": [2.0, 3.0, 4.0, null],
            "g": [true, false, true],
            "h": ["foo", "bar", "foobar"],
            "i": null
        }
    }"""
    field_values_proto = FieldValues()
    Parse(field_values_str, field_values_proto)
    assertpy.assert_that(field_values_proto.fields.keys()).is_equal_to(
        {"a", "b", "c", "d", "e", "f", "g", "h", "i"}
    )
    assertpy.assert_that(field_values_proto.fields["a"].int64_val).is_equal_to(1)
    assertpy.assert_that(field_values_proto.fields["b"].double_val).is_equal_to(2.0)
    assertpy.assert_that(field_values_proto.fields["c"].bool_val).is_equal_to(True)
    assertpy.assert_that(field_values_proto.fields["d"].string_val).is_equal_to("foo")
    assertpy.assert_that(field_values_proto.fields["e"].int64_list_val.val).is_equal_to(
        [1, 2, 3]
    )
    # Can't directly check equality to [2.0, 3.0, 4.0, float("nan")], because float("nan") != float("nan")
    assertpy.assert_that(
        field_values_proto.fields["f"].double_list_val.val[:3]
    ).is_equal_to([2.0, 3.0, 4.0])
    assertpy.assert_that(field_values_proto.fields["f"].double_list_val.val[3]).is_nan()
    assertpy.assert_that(field_values_proto.fields["g"].bool_list_val.val).is_equal_to(
        [True, False, True]
    )
    assertpy.assert_that(
        field_values_proto.fields["h"].string_list_val.val
    ).is_equal_to(["foo", "bar", "foobar"])
    assertpy.assert_that(field_values_proto.fields["i"].null_val).is_equal_to(0)

    # Now convert protobuf back to json and check that
    field_values_json = MessageToDict(field_values_proto)
    assertpy.assert_that(field_values_json["fields"].keys()).is_equal_to(
        {"a", "b", "c", "d", "e", "f", "g", "h", "i"}
    )
    assertpy.assert_that(field_values_json["fields"]["a"]).is_equal_to(1)
    assertpy.assert_that(field_values_json["fields"]["b"]).is_equal_to(2.0)
    assertpy.assert_that(field_values_json["fields"]["c"]).is_equal_to(True)
    assertpy.assert_that(field_values_json["fields"]["d"]).is_equal_to("foo")
    assertpy.assert_that(field_values_json["fields"]["e"]).is_equal_to([1, 2, 3])
    # Can't directly check equality to [2.0, 3.0, 4.0, float("nan")], because float("nan") != float("nan")
    assertpy.assert_that(field_values_json["fields"]["f"][:3]).is_equal_to(
        [2.0, 3.0, 4.0]
    )
    assertpy.assert_that(field_values_json["fields"]["f"][3]).is_nan()
    assertpy.assert_that(field_values_json["fields"]["g"]).is_equal_to(
        [True, False, True]
    )
    assertpy.assert_that(field_values_json["fields"]["h"]).is_equal_to(
        ["foo", "bar", "foobar"]
    )
    assertpy.assert_that(field_values_json["fields"]["i"]).is_equal_to(None)


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
