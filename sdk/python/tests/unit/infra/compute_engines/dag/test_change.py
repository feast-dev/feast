import math

import pytest

from feast.infra.compute_engines.dag.change import (
    ChangeOperation,
    ChangeRecord,
    ChangeSet,
    KeyedRows,
    diff_keyed_rows,
    rows_equal,
)


def test_change_record_requires_operation_specific_rows():
    with pytest.raises(ValueError, match="INSERT requires after"):
        ChangeRecord(ChangeOperation.INSERT)
    with pytest.raises(ValueError, match="UPDATE requires before and after"):
        ChangeRecord(ChangeOperation.UPDATE, before={"id": 1})
    with pytest.raises(ValueError, match="DELETE requires before"):
        ChangeRecord(ChangeOperation.DELETE)


def test_change_set_is_immutable_and_ordered():
    first = ChangeRecord.insert({"id": 1, "value": 10})
    second = ChangeRecord.delete({"id": 2, "value": 20})
    changes = ChangeSet(source="source", records=(first, second))
    assert changes.records == (first, second)
    with pytest.raises(TypeError):
        first.after["value"] = 99  # type: ignore[index]


def test_keyed_rows_rejects_empty_missing_and_duplicate_primary_keys():
    with pytest.raises(ValueError, match="at least one primary key"):
        KeyedRows.from_rows([], ())
    with pytest.raises(ValueError, match="Missing primary key column 'id'"):
        KeyedRows.from_rows([{"value": 10}], ("id",))
    with pytest.raises(ValueError, match="Duplicate primary key"):
        KeyedRows.from_rows([{"id": 1}, {"id": 1}], ("id",))


def test_keyed_rows_apply_key_changing_update_atomically():
    state = KeyedRows.from_rows([{"id": 1, "value": "old"}], ("id",))
    state.apply(
        ChangeSet(
            "source",
            (
                ChangeRecord.update(
                    {"id": 1, "value": "old"}, {"id": 2, "value": "new"}
                ),
            ),
        )
    )
    assert state.rows == {(2,): {"id": 2, "value": "new"}}


def test_keyed_rows_rolls_back_complete_change_set_on_invalid_second_record():
    state = KeyedRows.from_rows([{"id": 1, "value": 10}], ("id",))
    changes = ChangeSet(
        "source",
        (
            ChangeRecord.insert({"id": 2, "value": 20}),
            ChangeRecord.delete({"id": 999, "value": 0}),
        ),
    )
    with pytest.raises(ValueError, match="unknown primary key"):
        state.apply(changes)
    assert state.rows == {(1,): {"id": 1, "value": 10}}


def test_keyed_rows_rejects_existing_insert_and_stale_update():
    state = KeyedRows.from_rows([{"id": 1, "value": 10}], ("id",))
    with pytest.raises(ValueError, match="already exists"):
        state.apply(ChangeSet("source", (ChangeRecord.insert({"id": 1}),)))
    with pytest.raises(ValueError, match="does not match current state"):
        state.apply(
            ChangeSet(
                "source",
                (ChangeRecord.update({"id": 1, "value": 9}, {"id": 1}),),
            )
        )


def test_rows_equal_treats_paired_nan_as_equal():
    assert rows_equal({"id": 1, "value": math.nan}, {"id": 1, "value": math.nan})
    assert not rows_equal({"id": 1, "value": math.nan}, {"id": 1, "value": 2})


def test_rows_for_columns_selects_matching_rows():
    state = KeyedRows.from_rows(
        [{"id": 1, "group": "a"}, {"id": 2, "group": "b"}], ("id",)
    )
    assert state.rows_for_columns(("group",), {("b",)}) == [{"id": 2, "group": "b"}]


def test_diff_keyed_rows_classifies_insert_update_and_delete():
    before = KeyedRows.from_rows(
        [{"id": 1, "value": "delete"}, {"id": 2, "value": "old"}], ("id",)
    )
    after = KeyedRows.from_rows(
        [{"id": 2, "value": "new"}, {"id": 3, "value": "insert"}], ("id",)
    )
    changes = diff_keyed_rows("join", before, after)
    assert [record.operation for record in changes.records] == [
        ChangeOperation.DELETE,
        ChangeOperation.UPDATE,
        ChangeOperation.INSERT,
    ]
