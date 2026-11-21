"""Generate Python-side parity fixtures for the Go Bigtable online store.

Calls the in-tree Python BigtableOnlineStore: its table name and row key helpers, and its
writer (_write_rows_to_bt) through a table stand-in that records the cells. Prints
deterministic JSON on stdout; the Go tests in bigtableonlinestore_test.go read it from this
directory, and sdk/python/tests/unit/infra/online_store/test_bigtable_go_fixtures.py fails
when the committed file no longer matches what this SDK writes.

Regenerate from the repository root:

    uv run --extra gcp python go/internal/feast/onlinestore/testdata/gen_bigtable_fixtures.py \
        > go/internal/feast/onlinestore/testdata/bigtable_fixtures.json

Fixture schema:
    table_names: [{project, entities, want}]
    row_keys:    [{name, entityless, join_keys, values: [{type, value}], view, want}]
                 want null = Go must reject; entityless = built from Feast's
                 DUMMY_ENTITY_ID/_VAL; value encoding: string and uuid as-is, bytes as
                 base64, int32 and int64 as decimal strings
    cells:       [{name, value_b64, event_ts, want_seconds, want_nanos, want_value}]
                 want_value = json_format.MessageToDict of the ValueProto
                 (protojson-readable)
    murmur_vectors: [{length, want}]
                 want = mmh3.hash_bytes(bytes(range(length))).hex(), the entity-id hash
                 of Feast's compute_entity_id, for every length 1 to 100 (all 16-byte
                 tail sizes, several blocks)
"""

import base64
import json
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

import mmh3
from google.protobuf import json_format

from feast import utils
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_NAME, DUMMY_ENTITY_VAL
from feast.infra.online_stores.bigtable import BigtableOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig

SERIALIZATION_VERSION = 3
SHORT_PROJECT = "my_project"
# 60 chars: longer than the room any entity part leaves, so the project is hashed.
LONG_PROJECT = "my_feature_store_project_with_a_deliberately_long_name_00060"


def repo_config(project: str) -> RepoConfig:
    """Returns a Bigtable repo config for project."""
    return RepoConfig(
        project=project,
        registry="registry.db",
        provider="gcp",
        online_store={"type": "bigtable", "instance": "fixtures"},
        offline_store={"type": "file"},
        entity_key_serialization_version=SERIALIZATION_VERSION,
    )


def table_name_cases() -> List[Dict[str, Any]]:
    """Returns Python's table name for each (project, entities) case."""
    cases = [
        (SHORT_PROJECT, ["subscriber"]),
        (SHORT_PROJECT, ["title", "subscriber"]),
        (SHORT_PROJECT, ["subscriber_profile_region"]),  # 25 chars: hashed
        # 10 chars, 30 UTF-8 bytes: kept, as the limit counts characters.
        (SHORT_PROJECT, ["日本語日本語日本語日本"]),
        # Project hash cut to 28 of the 32 md5 hex chars.
        (LONG_PROJECT, ["subscriber"]),
        # Short entity parts ask for 33 and 37 hash chars; Python clamps at 32.
        (LONG_PROJECT, ["title"]),
        (LONG_PROJECT, ["t"]),
        (SHORT_PROJECT, []),  # entityless, Python's else branch
        # Entityless as FeatureView and the Go model store it.
        (SHORT_PROJECT, [DUMMY_ENTITY_NAME]),
    ]
    assert len(LONG_PROJECT) == 60
    assert len("subscriber_profile_region") == 25
    out: List[Dict[str, Any]] = []
    for project, entities in cases:
        fv = SimpleNamespace(name="fv", entities=entities)
        want = BigtableOnlineStore._get_table_name(
            config=repo_config(project),
            feature_view=fv,  # type: ignore[arg-type]
        )
        out.append({"project": project, "entities": entities, "want": want})
    return out


def encode_value(type_name: str, value: Any) -> Dict[str, str]:
    """Encodes an entity value as the fixture's {type, value}."""
    if type_name == "bytes":
        return {"type": type_name, "value": base64.b64encode(value).decode()}
    if type_name in ("int32", "int64"):
        return {"type": type_name, "value": str(value)}
    return {"type": type_name, "value": value}


def to_proto(type_name: str, value: Any) -> ValueProto:
    """Returns value as a ValueProto of type_name."""
    field = {
        "string": "string_val",
        "bytes": "bytes_val",
        "int32": "int32_val",
        "int64": "int64_val",
        "uuid": "uuid_val",
    }[type_name]
    return ValueProto(**{field: value})


def row_key_cases() -> List[Dict[str, Any]]:
    """Returns Python's row key per case; None marks a key Go must reject."""
    # (name, [(join_key, type, value)], view, go_must_reject)
    cases: List[Tuple[str, List[Tuple[str, str, Any]], str, bool]] = [
        ("string", [("subscriber_id", "string", "abc")], "subscriber_features", False),
        (
            "utf8 string",
            [("subscriber_id", "string", "日本")],
            "subscriber_features",
            False,
        ),
        ("bytes", [("blob_id", "bytes", b"\x00\xff")], "blob_features", False),
        ("int32", [("region_id", "int32", -7)], "region_features", False),
        ("int64", [("title_id", "int64", -9007199254740993)], "title_features", False),
        (
            "two join keys reversed",
            [("title_id", "int64", 42), ("subscriber_id", "string", "abc")],
            "subscriber_title_features",
            False,
        ),
        (
            "entityless",
            [(DUMMY_ENTITY_ID, "string", DUMMY_ENTITY_VAL)],
            "global_stats",
            False,
        ),
        (
            "uuid",
            [("session_id", "uuid", "0b3e7c1a-6a2f-4c55-9f4e-1d2c3b4a5f60")],
            "session_features",
            True,
        ),
    ]
    store = BigtableOnlineStore()
    config = repo_config(SHORT_PROJECT)
    out: List[Dict[str, Any]] = []
    for name, keys, view, go_must_reject in cases:
        entity_key = EntityKeyProto(
            join_keys=[k for k, _, _ in keys],
            entity_values=[to_proto(t, v) for _, t, v in keys],
        )
        # Python computes a key for every case, uuid included, so all are writable keys.
        key = store._compute_row_key(entity_key, view, config).decode()
        out.append(
            {
                "name": name,
                "entityless": [k for k, _, _ in keys] == [DUMMY_ENTITY_ID],
                "join_keys": [k for k, _, _ in keys],
                "values": [encode_value(t, v) for _, t, v in keys],
                "view": view,
                "want": None if go_must_reject else key,
            }
        )
    return out


class _RecordingRow:
    """Stands in for a Bigtable DirectRow and records the cells set on it."""

    def __init__(self, key: bytes) -> None:
        """Start an empty row with row key key."""
        self.key = key
        self.cells: Dict[bytes, bytes] = {}

    def set_cell(self, family: str, qualifier: bytes, value: bytes) -> None:
        """Record value under qualifier; every cell must be in the features family."""
        assert family == BigtableOnlineStore.feature_column_family
        self.cells[qualifier] = value


class _RecordingTable:
    """Stands in for a Bigtable table and records the rows _write_rows_to_bt builds."""

    def __init__(self) -> None:
        """Start with no rows."""
        self.rows: List[_RecordingRow] = []

    def direct_row(self, key: bytes) -> _RecordingRow:
        """Return a new recording row for key and keep it."""
        row = _RecordingRow(key)
        self.rows.append(row)
        return row

    def mutate_rows(self, rows: List[_RecordingRow]) -> None:
        """Do nothing: the rows were already recorded by direct_row."""


def written_cells(value: ValueProto, ts: datetime) -> Dict[bytes, bytes]:
    """Returns the cells the Python writer sets for one row with feature "f"."""
    table = _RecordingTable()
    entity_key = EntityKeyProto(
        join_keys=["subscriber_id"], entity_values=[ValueProto(string_val="abc")]
    )
    BigtableOnlineStore()._write_rows_to_bt(
        [(entity_key, {"f": value}, ts, None)],
        table,  # type: ignore[arg-type]
        "fv",
        repo_config(SHORT_PROJECT),
        None,
    )
    (row,) = table.rows
    return row.cells


def cell_cases() -> List[Dict[str, Any]]:
    """Returns the cells Python writes for each value and event timestamp case."""
    with_micros = datetime(2026, 11, 17, 10, 0, 0, 123456, tzinfo=timezone.utc)
    whole_second = datetime(2026, 11, 17, 10, 0, 0, tzinfo=timezone.utc)
    ist = datetime(
        2026, 11, 17, 15, 30, 0, 250000, tzinfo=timezone(timedelta(hours=5, minutes=30))
    )
    # make_tzaware turns this into UTC, as the writer does.
    naive = datetime(2026, 11, 17, 10, 0, 0)
    cases = [
        ("int64", ValueProto(int64_val=-9007199254740993), with_micros),
        ("double", ValueProto(double_val=0.1), whole_second),
        ("string", ValueProto(string_val="日本 abc"), ist),
        ("bool", ValueProto(bool_val=True), naive),
        (
            "float list",
            ValueProto(float_list_val=FloatList(val=[0.5, -1.25, 3.0])),
            with_micros,
        ),
        ("null", ValueProto(), whole_second),
    ]
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    out: List[Dict[str, Any]] = []
    for name, value, ts in cases:
        cells = written_cells(value, ts)
        # The expected instant, computed independently of the writer's encoding.
        aware = utils.make_tzaware(ts)
        out.append(
            {
                "name": name,
                "value_b64": base64.b64encode(cells[b"f"]).decode(),
                "event_ts": cells[b"event_ts"].decode(),
                "want_seconds": (aware - epoch) // timedelta(seconds=1),
                "want_nanos": aware.microsecond * 1000,
                "want_value": json_format.MessageToDict(
                    value, preserving_proto_field_name=True
                ),
            }
        )
    return out


def murmur_vectors() -> List[Dict[str, Any]]:
    """Returns Python's entity hash of bytes(range(n)) for n from 1 to 100."""
    # compute_entity_id (feast/infra/online_stores/helpers.py) is
    # mmh3.hash_bytes(serialized key).hex(): x64 128-bit, seed 0, h1 then h2
    # little-endian.
    return [
        {"length": n, "want": mmh3.hash_bytes(bytes(range(n))).hex()}
        for n in range(1, 101)
    ]


def build_fixtures() -> Dict[str, List[Dict[str, Any]]]:
    """Returns every fixture, as written to bigtable_fixtures.json."""
    return {
        "table_names": table_name_cases(),
        "row_keys": row_key_cases(),
        "cells": cell_cases(),
        "murmur_vectors": murmur_vectors(),
    }


def main() -> None:
    """Prints the fixtures as JSON."""
    json.dump(
        build_fixtures(), sys.stdout, indent=2, sort_keys=True, ensure_ascii=False
    )
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
