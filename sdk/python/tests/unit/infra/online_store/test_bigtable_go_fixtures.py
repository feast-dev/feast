"""The Go Bigtable online store's parity fixtures must match what this SDK writes."""

import importlib.util
import json
import pathlib
from types import ModuleType
from typing import Any, Dict, Iterator

import pytest
from google.protobuf import json_format

from feast import proto_json
from feast.protos.feast.serving.ServingService_pb2 import FeatureList
from feast.protos.feast.types.Value_pb2 import (
    BoolSet,
    BytesList,
    DoubleList,
    FloatList,
    Int32Set,
    Int64Set,
    Map,
    MapKey,
    MapList,
    Null,
    RepeatedValue,
    ScalarMap,
    ScalarMapEntry,
    StringList,
    ZonedTimestamp,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto

pytest.importorskip("google.cloud.bigtable")

TESTDATA = (
    pathlib.Path(__file__).resolve().parents[6]
    / "go"
    / "internal"
    / "feast"
    / "onlinestore"
    / "testdata"
)

# The message types whose JSON form proto_json.patch() replaces.
_PATCHED_TYPES = [
    m.DESCRIPTOR.full_name for m in (ValueProto, RepeatedValue, FeatureList)
]


@pytest.fixture
def generator() -> ModuleType:
    """Loads testdata/gen_bigtable_fixtures.py as a module."""
    if not TESTDATA.is_dir():
        pytest.skip("the Go sources are not in this checkout")
    spec = importlib.util.spec_from_file_location(
        "gen_bigtable_fixtures", TESTDATA / "gen_bigtable_fixtures.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def committed() -> Dict[str, Any]:
    """Returns the committed bigtable_fixtures.json."""
    return json.loads((TESTDATA / "bigtable_fixtures.json").read_text(encoding="utf-8"))


@pytest.fixture
def json_methods() -> Iterator[Dict[str, Any]]:
    """Yields json_format's per-type JSON methods and restores them afterwards.

    proto_json.patch() registers Feast's JSON form in this table for the whole process,
    so each test sets the state it needs here instead of depending on test order.
    """
    methods = json_format._WKTJSONMETHODS
    saved = dict(methods)
    yield methods
    methods.clear()
    methods.update(saved)


def _assert_matches(generator: ModuleType, committed: Dict[str, Any]) -> None:
    assert generator.build_fixtures() == committed, (
        "The Go Bigtable fixtures no longer match the Python writer. Regenerate them "
        "with the command in testdata/gen_bigtable_fixtures.py and check the Go tests."
    )


def test_go_fixtures_match_python_writer_without_json_patch(
    generator: ModuleType, committed: Dict[str, Any], json_methods: Dict[str, Any]
) -> None:
    """Fail when the committed fixtures drift from the in-tree Python store."""
    for name in _PATCHED_TYPES:
        json_methods.pop(name, None)
    assert json_format.MessageToDict(
        ValueProto(int64_val=-9007199254740993), preserving_proto_field_name=True
    ) == {"int64_val": "-9007199254740993"}
    _assert_matches(generator, committed)


def test_go_fixtures_match_python_writer_with_json_patch(
    generator: ModuleType, committed: Dict[str, Any], json_methods: Dict[str, Any]
) -> None:
    """The fixtures stay the same after Feast patches protobuf's JSON printer."""
    proto_json.patch()
    # The patch is in effect: MessageToDict now prints a bare value.
    assert (
        json_format.MessageToDict(ValueProto(int64_val=-9007199254740993))
        == -9007199254740993
    )
    _assert_matches(generator, committed)


def _unknown_null() -> ValueProto:
    """Returns a Value whose null_val holds a number Null has no name for."""
    value = ValueProto()
    value.null_val = 7  # type: ignore[assignment]
    return value


# One Value per JSON shape: every scalar kind, list and set wrappers, the Null enum,
# maps, nested Values and nested messages.
_VALUES = [
    ValueProto(),
    ValueProto(bytes_val=b"\x00\xff"),
    ValueProto(string_val="日本 abc"),
    ValueProto(int32_val=-7),
    ValueProto(int64_val=0),
    ValueProto(int64_val=-9007199254740993),
    ValueProto(double_val=0.1),
    ValueProto(double_val=float("nan")),
    ValueProto(double_val=float("-inf")),
    ValueProto(float_val=0.1),
    ValueProto(float_val=float("inf")),
    ValueProto(bool_val=False),
    ValueProto(unix_timestamp_val=1763373600),
    ValueProto(bytes_list_val=BytesList(val=[b"a", b""])),
    ValueProto(string_list_val=StringList(val=[])),
    ValueProto(double_list_val=DoubleList(val=[0.1, -2.5])),
    ValueProto(float_list_val=FloatList(val=[0.1, -1.25, 3.0])),
    ValueProto(int64_set_val=Int64Set(val=[1, -9007199254740993])),
    ValueProto(int32_set_val=Int32Set(val=[-7, 0])),
    ValueProto(bool_set_val=BoolSet(val=[True, False])),
    ValueProto(json_val='{"a": 1}'),
    ValueProto(decimal_val="1.50"),
    ValueProto(uuid_list_val=StringList(val=["0b3e7c1a-6a2f-4c55-9f4e-1d2c3b4a5f60"])),
    ValueProto(null_val=Null.NULL),
    _unknown_null(),
    ValueProto(
        map_val=Map(val={"a": ValueProto(int64_val=5), "b": ValueProto(string_val="x")})
    ),
    ValueProto(
        list_val=RepeatedValue(
            val=[ValueProto(int64_val=1), ValueProto(), ValueProto(bool_val=True)]
        )
    ),
    ValueProto(
        map_list_val=MapList(
            val=[Map(val={"a": ValueProto(double_val=0.5)}), Map(val={})]
        )
    ),
    ValueProto(
        set_val=RepeatedValue(
            val=[ValueProto(string_val="a"), ValueProto(int64_val=-1)]
        )
    ),
    ValueProto(
        scalar_map_val=ScalarMap(
            val=[
                ScalarMapEntry(key=MapKey(int64_key=2), value=ValueProto(float_val=0.5))
            ]
        )
    ),
    ValueProto(
        zoned_timestamp_val=ZonedTimestamp(unix_timestamp=1, zone="Asia/Kolkata")
    ),
]


@pytest.mark.parametrize("value", _VALUES, ids=lambda v: v.WhichOneof("val") or "unset")
def test_typed_json_matches_unpatched_message_to_dict(
    generator: ModuleType, json_methods: Dict[str, Any], value: ValueProto
) -> None:
    """typed_json prints what protobuf itself prints for a Value before the patch."""
    for name in _PATCHED_TYPES:
        json_methods.pop(name, None)
    want = json_format.MessageToDict(value, preserving_proto_field_name=True)
    assert generator.typed_json(value) == want
