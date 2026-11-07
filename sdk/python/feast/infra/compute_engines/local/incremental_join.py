"""In-memory incremental inner-join maintenance for the Local engine PoC."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pyarrow as pa

from feast.infra.compute_engines.backends.base import DataFrameBackend
from feast.infra.compute_engines.dag.change import (
    ChangeSet,
    KeyedRows,
    diff_keyed_rows,
)


@dataclass
class IncrementalInnerJoinState:
    left: KeyedRows
    right: KeyedRows
    output: KeyedRows
    join_keys: tuple[str, ...]

    def clone(self) -> "IncrementalInnerJoinState":
        return IncrementalInnerJoinState(
            self.left.clone(),
            self.right.clone(),
            self.output.clone(),
            self.join_keys,
        )


def _keyed_state(state: Any) -> KeyedRows:
    if isinstance(state, KeyedRows):
        return state
    output = getattr(state, "output", None)
    if isinstance(output, KeyedRows):
        return output
    raise TypeError("Incremental join input does not expose keyed row state")


def initialize_inner_join_state(
    node_name: str,
    input_names: tuple[str, str],
    node_states: Mapping[str, Any],
    output_rows: list[dict[str, Any]],
    output_primary_keys: tuple[str, ...],
    join_keys: tuple[str, ...],
) -> IncrementalInnerJoinState:
    left = _keyed_state(node_states[input_names[0]]).clone()
    right = _keyed_state(node_states[input_names[1]]).clone()
    for side_name, side in (("left", left), ("right", right)):
        for row in side.rows.values():
            for column in join_keys:
                if column not in row:
                    raise ValueError(
                        f"Incremental join '{node_name}' {side_name} input "
                        f"is missing join key '{column}'"
                    )
    return IncrementalInnerJoinState(
        left=left,
        right=right,
        output=KeyedRows.from_rows(output_rows, output_primary_keys),
        join_keys=join_keys,
    )


def _affected_join_keys(
    changes: ChangeSet, join_keys: tuple[str, ...], node_name: str
) -> set[tuple[Any, ...]]:
    affected: set[tuple[Any, ...]] = set()
    for record in changes.records:
        for row in (record.before, record.after):
            if row is None:
                continue
            missing = [column for column in join_keys if column not in row]
            if missing:
                raise ValueError(
                    f"Incremental join '{node_name}' change is missing join key "
                    f"'{missing[0]}'"
                )
            affected.add(tuple(row[column] for column in join_keys))
    return affected


def _join_rows(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    join_keys: tuple[str, ...],
    backend: DataFrameBackend,
) -> list[dict[str, Any]]:
    if not left_rows or not right_rows:
        return []
    left = backend.from_arrow(pa.Table.from_pylist(left_rows))
    right = backend.from_arrow(pa.Table.from_pylist(right_rows))
    joined = backend.join(left, right, on=list(join_keys), how="inner")
    return backend.to_arrow(joined).to_pylist()


def execute_incremental_inner_join(
    node_name: str,
    input_names: tuple[str, str],
    state: IncrementalInnerJoinState,
    input_changes: Mapping[str, ChangeSet],
    backend: DataFrameBackend,
) -> tuple[IncrementalInnerJoinState, ChangeSet]:
    candidate = state.clone()
    affected: set[tuple[Any, ...]] = set()

    for input_name, changes in input_changes.items():
        if input_name == input_names[0]:
            side = candidate.left
        elif input_name == input_names[1]:
            side = candidate.right
        else:
            raise ValueError(
                f"Incremental join '{node_name}' received unknown input '{input_name}'"
            )
        affected.update(_affected_join_keys(changes, state.join_keys, node_name))
        side.apply(changes)

    old_rows = candidate.output.rows_for_columns(state.join_keys, affected)
    old_partition = KeyedRows.from_rows(old_rows, candidate.output.primary_keys)
    left_rows = candidate.left.rows_for_columns(state.join_keys, affected)
    right_rows = candidate.right.rows_for_columns(state.join_keys, affected)
    new_rows = _join_rows(left_rows, right_rows, state.join_keys, backend)
    new_partition = KeyedRows.from_rows(new_rows, candidate.output.primary_keys)

    retained = {
        key: row
        for key, row in candidate.output.rows.items()
        if candidate.output.key_for(row, state.join_keys) not in affected
    }
    for key, row in new_partition.rows.items():
        if key in retained:
            raise ValueError(
                f"Incremental join '{node_name}' produced duplicate output key {key}"
            )
        retained[key] = row
    candidate.output.rows = retained
    return candidate, diff_keyed_rows(node_name, old_partition, new_partition)
