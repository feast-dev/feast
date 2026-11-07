"""Engine-neutral row change envelopes and keyed in-memory state."""

from __future__ import annotations

import copy
import math
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional


class ChangeOperation(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass(frozen=True)
class ChangeRecord:
    operation: ChangeOperation
    before: Optional[Mapping[str, Any]] = None
    after: Optional[Mapping[str, Any]] = None

    def __post_init__(self) -> None:
        if self.operation is ChangeOperation.INSERT and (
            self.after is None or self.before is not None
        ):
            raise ValueError("INSERT requires after and forbids before")
        if self.operation is ChangeOperation.UPDATE and (
            self.before is None or self.after is None
        ):
            raise ValueError("UPDATE requires before and after")
        if self.operation is ChangeOperation.DELETE and (
            self.before is None or self.after is not None
        ):
            raise ValueError("DELETE requires before and forbids after")
        if self.before is not None:
            object.__setattr__(
                self, "before", MappingProxyType(copy.deepcopy(dict(self.before)))
            )
        if self.after is not None:
            object.__setattr__(
                self, "after", MappingProxyType(copy.deepcopy(dict(self.after)))
            )

    @classmethod
    def insert(cls, after: Mapping[str, Any]) -> ChangeRecord:
        return cls(ChangeOperation.INSERT, after=after)

    @classmethod
    def update(
        cls, before: Mapping[str, Any], after: Mapping[str, Any]
    ) -> ChangeRecord:
        return cls(ChangeOperation.UPDATE, before=before, after=after)

    @classmethod
    def delete(cls, before: Mapping[str, Any]) -> ChangeRecord:
        return cls(ChangeOperation.DELETE, before=before)


@dataclass(frozen=True)
class ChangeSet:
    source: str
    records: tuple[ChangeRecord, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "records", tuple(self.records))


def _values_equal(left: Any, right: Any) -> bool:
    if left is right:
        return True
    try:
        if math.isnan(left) and math.isnan(right):
            return True
    except (TypeError, ValueError):
        pass
    result = left == right
    return bool(result) if isinstance(result, (bool, int)) else False


def rows_equal(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return left.keys() == right.keys() and all(
        _values_equal(left[column], right[column]) for column in left
    )


class KeyedRows:
    """A unique-key row collection with atomic change application."""

    def __init__(
        self,
        primary_keys: tuple[str, ...],
        rows: Optional[dict[tuple[Any, ...], dict[str, Any]]] = None,
    ) -> None:
        if not primary_keys:
            raise ValueError("KeyedRows requires at least one primary key column")
        self.primary_keys = tuple(primary_keys)
        self.rows = rows or {}

    @classmethod
    def from_rows(
        cls,
        rows: Iterable[Mapping[str, Any]],
        primary_keys: tuple[str, ...],
    ) -> KeyedRows:
        keyed = cls(primary_keys)
        for input_row in rows:
            row = copy.deepcopy(dict(input_row))
            key = keyed.key_for(row)
            if key in keyed.rows:
                raise ValueError(f"Duplicate primary key {key}")
            keyed.rows[key] = row
        return keyed

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, KeyedRows) or self.primary_keys != other.primary_keys:
            return False
        return self.rows.keys() == other.rows.keys() and all(
            rows_equal(self.rows[key], other.rows[key]) for key in self.rows
        )

    def clone(self) -> KeyedRows:
        return KeyedRows(self.primary_keys, copy.deepcopy(self.rows))

    def key_for(
        self,
        row: Mapping[str, Any],
        columns: Optional[tuple[str, ...]] = None,
    ) -> tuple[Any, ...]:
        key_columns = columns or self.primary_keys
        for column in key_columns:
            if column not in row:
                raise ValueError(f"Missing primary key column '{column}'")
        return tuple(row[column] for column in key_columns)

    def apply(self, changes: ChangeSet) -> None:
        candidate = self.clone()
        for record in changes.records:
            candidate._apply_record(record)
        self.rows = candidate.rows

    def _apply_record(self, record: ChangeRecord) -> None:
        if record.operation is ChangeOperation.INSERT:
            assert record.after is not None
            new_key = self.key_for(record.after)
            if new_key in self.rows:
                raise ValueError(f"Primary key {new_key} already exists")
            self.rows[new_key] = copy.deepcopy(dict(record.after))
            return

        assert record.before is not None
        old_key = self.key_for(record.before)
        current = self.rows.get(old_key)
        if current is None:
            raise ValueError(f"Cannot change unknown primary key {old_key}")
        if not rows_equal(current, record.before):
            raise ValueError(
                f"Before row for primary key {old_key} does not match current state"
            )
        if record.operation is ChangeOperation.DELETE:
            del self.rows[old_key]
            return

        assert record.after is not None
        new_key = self.key_for(record.after)
        if new_key != old_key and new_key in self.rows:
            raise ValueError(f"Primary key {new_key} already exists")
        del self.rows[old_key]
        self.rows[new_key] = copy.deepcopy(dict(record.after))

    def rows_for_columns(
        self,
        columns: tuple[str, ...],
        keys: set[tuple[Any, ...]],
    ) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(row)
            for row in self.rows.values()
            if self.key_for(row, columns) in keys
        ]


def diff_keyed_rows(source: str, before: KeyedRows, after: KeyedRows) -> ChangeSet:
    if before.primary_keys != after.primary_keys:
        raise ValueError("Cannot diff keyed rows with different primary keys")
    records: list[ChangeRecord] = []
    for key in sorted(before.rows.keys() | after.rows.keys(), key=repr):
        old = before.rows.get(key)
        new = after.rows.get(key)
        if old is None:
            assert new is not None
            records.append(ChangeRecord.insert(new))
        elif new is None:
            records.append(ChangeRecord.delete(old))
        elif not rows_equal(old, new):
            records.append(ChangeRecord.update(old, new))
    return ChangeSet(source, tuple(records))
