import pytest

from feast.infra.contrib.grpc_server import parse_typed
from feast.protos.feast.serving.GrpcServer_pb2 import (
    PushRequest,
    WriteToOnlineStoreRequest,
)
from feast.protos.feast.types.Value_pb2 import (
    Int64List,
    Map,
    Null,
    StringSet,
    Value,
)


def test_push_request_string_features():
    request = PushRequest(
        features={"driver_id": "1001", "conv_rate": "0.5"},
        stream_feature_view="driver_stats",
        to="online",
    )
    assert request.features["driver_id"] == "1001"
    assert request.features["conv_rate"] == "0.5"
    assert len(request.typed_features) == 0


def test_push_request_typed_features():
    request = PushRequest(
        typed_features={
            "driver_id": Value(int64_val=1001),
            "conv_rate": Value(float_val=0.5),
            "active": Value(bool_val=True),
            "label": Value(string_val="fast"),
        },
        stream_feature_view="driver_stats",
        to="online",
    )
    assert request.typed_features["driver_id"].int64_val == 1001
    assert request.typed_features["conv_rate"].float_val == pytest.approx(0.5)
    assert request.typed_features["active"].bool_val is True
    assert request.typed_features["label"].string_val == "fast"
    assert len(request.features) == 0


def test_push_request_typed_features_val_case():
    """WhichOneof('val') returns the correct field name for each value type."""
    cases = [
        (Value(int32_val=1), "int32_val"),
        (Value(int64_val=2), "int64_val"),
        (Value(float_val=1.0), "float_val"),
        (Value(double_val=2.0), "double_val"),
        (Value(bool_val=True), "bool_val"),
        (Value(string_val="x"), "string_val"),
    ]
    for value, expected_case in cases:
        assert value.WhichOneof("val") == expected_case


def test_write_to_online_store_request_string_features():
    request = WriteToOnlineStoreRequest(
        features={"driver_id": "1001", "avg_daily_trips": "10"},
        feature_view_name="driver_hourly_stats",
    )
    assert request.features["driver_id"] == "1001"
    assert request.features["avg_daily_trips"] == "10"
    assert len(request.typed_features) == 0


def test_write_to_online_store_request_typed_features():
    request = WriteToOnlineStoreRequest(
        typed_features={
            "driver_id": Value(int64_val=1001),
            "avg_daily_trips": Value(int32_val=10),
            "conv_rate": Value(float_val=0.42),
        },
        feature_view_name="driver_hourly_stats",
    )
    assert request.typed_features["driver_id"].int64_val == 1001
    assert request.typed_features["avg_daily_trips"].int32_val == 10
    assert request.typed_features["conv_rate"].float_val == pytest.approx(0.42)
    assert len(request.features) == 0


def test_push_request_string_and_typed_features_are_independent():
    """Setting features does not affect typed_features and vice versa."""
    r1 = PushRequest(
        features={"driver_id": "1001"}, stream_feature_view="s", to="online"
    )
    r2 = PushRequest(
        typed_features={"driver_id": Value(int64_val=1001)},
        stream_feature_view="s",
        to="online",
    )
    assert len(r1.typed_features) == 0
    assert len(r2.features) == 0


def test_write_to_online_store_string_and_typed_features_are_independent():
    r1 = WriteToOnlineStoreRequest(
        features={"driver_id": "1001"}, feature_view_name="fv"
    )
    r2 = WriteToOnlineStoreRequest(
        typed_features={"driver_id": Value(int64_val=1001)}, feature_view_name="fv"
    )
    assert len(r1.typed_features) == 0
    assert len(r2.features) == 0


def test_parse_typed_null_val_becomes_none():
    """Value(null_val=NULL) must produce None in the DataFrame, not the integer 0."""
    df = parse_typed(
        {
            "present": Value(int64_val=42),
            "missing": Value(null_val=Null.NULL),
        }
    )
    assert df["present"].iloc[0] == 42
    assert df["missing"].iloc[0] is None


def test_parse_typed_unset_val_becomes_none():
    """A Value with no oneof field set (WhichOneof returns None) must also produce None."""
    df = parse_typed({"empty": Value()})
    assert df["empty"].iloc[0] is None


def test_parse_typed_list_val_unwrapped_to_python_list():
    """Compound list values are unwrapped from their protobuf wrapper to a plain list."""
    df = parse_typed(
        {
            "ids": Value(int64_list_val=Int64List(val=[1, 2, 3])),
        }
    )
    assert df["ids"].iloc[0] == [1, 2, 3]


def test_parse_typed_set_val_unwrapped_to_python_list():
    """Compound set values are unwrapped from their protobuf wrapper to a plain list."""
    df = parse_typed(
        {
            "tags": Value(string_set_val=StringSet(val=["a", "b"])),
        }
    )
    assert sorted(df["tags"].iloc[0]) == ["a", "b"]


def test_parse_typed_map_val_unwrapped_to_python_dict():
    """Map values are unwrapped from their protobuf Map wrapper to a plain dict."""
    df = parse_typed(
        {
            "scores": Value(
                map_val=Map(val={"x": Value(float_val=1.0), "y": Value(float_val=2.0)})
            ),
        }
    )
    result = df["scores"].iloc[0]
    assert isinstance(result, dict)
    assert set(result.keys()) == {"x", "y"}
