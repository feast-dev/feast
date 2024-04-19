import uuid
from importlib.metadata import version as importlib_version
from typing import Any, Callable, Type

from google.protobuf.json_format import (  # type: ignore
    _WKTJSONMETHODS,
    ParseError,
    _Parser,
    _Printer,
)
from packaging import version

from feast.protos.feast.serving.ServingService_pb2 import FeatureList
from feast.protos.feast.types.Value_pb2 import RepeatedValue, Value

ProtoMessage = Any
JsonObject = Any


def _patch_proto_json_encoding(
    proto_type: Type[ProtoMessage],
    to_json_object: Callable[[_Printer, ProtoMessage], JsonObject],
    from_json_object: Callable[[_Parser, JsonObject, ProtoMessage], None],
) -> None:
    """Patch Protobuf JSON Encoder / Decoder for a desired Protobuf type with to_json & from_json methods."""
    to_json_fn_name = "_" + uuid.uuid4().hex
    from_json_fn_name = "_" + uuid.uuid4().hex
    setattr(_Printer, to_json_fn_name, to_json_object)
    setattr(_Parser, from_json_fn_name, from_json_object)
    _WKTJSONMETHODS[proto_type.DESCRIPTOR.full_name] = [
        to_json_fn_name,
        from_json_fn_name,
    ]


def _patch_feast_value_json_encoding():
    """Patch Protobuf JSON Encoder / Decoder with a Feast Value type.

    This allows encoding the proto object as a native type, without the dummy structural wrapper.

    Here's a before example:

    {
        "value_1": {
            "int64_val": 1
        },
        "value_2": {
            "double_list_val": [1.0, 2.0, 3.0]
        },
    }

    And here's an after example:

    {
        "value_1": 1,
        "value_2": [1.0, 2.0, 3.0]
    }
    """

    def to_json_object(printer: _Printer, message: ProtoMessage) -> JsonObject:
        which = message.WhichOneof("val")
        # If the Value message is not set treat as null_value when serialize
        # to JSON. The parse back result will be different from original message.
        if which is None or which == "null_val":
            return None
        elif "_list_" in which:
            value = list(getattr(message, which).val)
        else:
            value = getattr(message, which)
        return value

    def from_json_object(
        parser: _Parser, value: JsonObject, message: ProtoMessage
    ) -> None:
        if value is None:
            message.null_val = 0
        elif isinstance(value, bool):
            message.bool_val = value
        elif isinstance(value, str):
            message.string_val = value
        elif isinstance(value, int):
            message.int64_val = value
        elif isinstance(value, float):
            message.double_val = value
        elif isinstance(value, list):
            if len(value) == 0:
                # Clear will mark the struct as modified so it will be created even if there are no values
                message.int64_list_val.Clear()
            elif isinstance(value[0], bool):
                message.bool_list_val.val.extend(value)
            elif isinstance(value[0], str):
                message.string_list_val.val.extend(value)
            elif isinstance(value[0], (float, int, type(None))):
                # Identify array as ints if all of the elements are ints
                if all(isinstance(item, int) for item in value):
                    message.int64_list_val.val.extend(value)
                # If any of the elements are floats or nulls, then parse it as a float array
                else:
                    # Convert each null as NaN.
                    message.double_list_val.val.extend(
                        [item if item is not None else float("nan") for item in value]
                    )
            else:
                raise ParseError(
                    "Value {0} has unexpected type {1}.".format(
                        value[0], type(value[0])
                    )
                )
        else:
            raise ParseError(
                "Value {0} has unexpected type {1}.".format(value, type(value))
            )

    def from_json_object_updated(
        parser: _Parser, value: JsonObject, message: ProtoMessage, path: str
    ):
        from_json_object(parser, value, message)

    # https://github.com/feast-dev/feast/issues/2484 Certain feast users need a higher version of protobuf but the
    # parameters of `from_json_object` changes in feast 3.20.1. This change gives users flexibility to use earlier versions.
    current_version = importlib_version("protobuf")
    if version.parse(current_version) < version.parse("3.20"):
        _patch_proto_json_encoding(Value, to_json_object, from_json_object)
    else:
        _patch_proto_json_encoding(Value, to_json_object, from_json_object_updated)


def _patch_feast_repeated_value_json_encoding():
    """Patch Protobuf JSON Encoder / Decoder with a Feast RepeatedValue type.

    This allows list of lists without dummy field name "val".

    Here's a before example:

    {
        "repeated_value": [
            {"val": [1,2,3]},
            {"val": [4,5,6]}
        ]
    }

    And here's an after example:

    {
        "repeated_value": [
            [1,2,3],
            [4,5,6]
        ]
    }
    """

    def to_json_object(printer: _Printer, message: ProtoMessage) -> JsonObject:
        return [printer._MessageToJsonObject(item) for item in message.val]

    def from_json_object_updated(
        parser: _Parser, value: JsonObject, message: ProtoMessage, path: str
    ) -> None:
        array = value if isinstance(value, list) else value["val"]
        for item in array:
            parser.ConvertMessage(item, message.val.add(), path)

    def from_json_object(
        parser: _Parser, value: JsonObject, message: ProtoMessage
    ) -> None:
        array = value if isinstance(value, list) else value["val"]
        for item in array:
            parser.ConvertMessage(item, message.val.add())

    # https://github.com/feast-dev/feast/issues/2484 Certain feast users need a higher version of protobuf but the
    # parameters of `from_json_object` changes in feast 3.20.1. This change gives users flexibility to use earlier versions.
    current_version = importlib_version("protobuf")
    if version.parse(current_version) < version.parse("3.20"):
        _patch_proto_json_encoding(RepeatedValue, to_json_object, from_json_object)
    else:
        _patch_proto_json_encoding(
            RepeatedValue, to_json_object, from_json_object_updated
        )


def _patch_feast_feature_list_json_encoding():
    """Patch Protobuf JSON Encoder / Decoder with a Feast FeatureList type.

    This allows list of lists without dummy field name "features".

    Here's a before example:

    {
        "feature_list": {
            "features": [
                "feature-1",
                "feature-2",
                "feature-3"
            ]
        }
    }

    And here's an after example:

    {
        "feature_list": [
            "feature-1",
            "feature-2",
            "feature-3"
        ]
    }
    """

    def to_json_object(printer: _Printer, message: ProtoMessage) -> JsonObject:
        return list(message.val)

    def from_json_object(
        parser: _Parser, value: JsonObject, message: ProtoMessage
    ) -> None:
        array = value if isinstance(value, list) else value["val"]
        message.val.extend(array)

    def from_json_object_updated(
        parser: _Parser, value: JsonObject, message: ProtoMessage, path: str
    ) -> None:
        from_json_object(parser, value, message)

    # https://github.com/feast-dev/feast/issues/2484 Certain feast users need a higher version of protobuf but the
    # parameters of `from_json_object` changes in feast 3.20.1. This change gives users flexibility to use earlier versions.
    current_version = importlib_version("protobuf")
    if version.parse(current_version) < version.parse("3.20"):
        _patch_proto_json_encoding(FeatureList, to_json_object, from_json_object)
    else:
        _patch_proto_json_encoding(
            FeatureList, to_json_object, from_json_object_updated
        )


def patch():
    """Patch Protobuf JSON Encoder / Decoder with all desired Feast types."""
    _patch_feast_value_json_encoding()
    _patch_feast_repeated_value_json_encoding()
    _patch_feast_feature_list_json_encoding()
