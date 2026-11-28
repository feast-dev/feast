from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest

from feast.infra.compute_engines.backends.pandas_backend import PandasBackend
from feast.infra.compute_engines.dag.change import (
    ChangeOperation,
    ChangeRecord,
    ChangeSet,
)
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue
from feast.infra.compute_engines.local.nodes import LocalJoinNode


class TableNode(DAGNode):
    def __init__(self, name, rows):
        super().__init__(name)
        self.rows = rows

    def execute(self, context):
        return ArrowTableValue(pa.Table.from_pylist(self.rows))


def context():
    return ExecutionContext(
        project="test",
        repo_config=MagicMock(),
        offline_store=MagicMock(),
        online_store=MagicMock(),
        entity_defs=[],
        entity_df=None,
    )


def normalized(rows, columns):
    if not rows:
        return pd.DataFrame(columns=columns).sort_values(columns).reset_index(drop=True)
    return (
        pd.DataFrame(rows)
        .sort_values(columns)
        .reset_index(drop=True)
        .sort_index(axis=1)
    )


def single_join_plan(how="inner"):
    left_rows = [
        {"left_id": 1, "entity_id": 10, "left_value": "a"},
        {"left_id": 2, "entity_id": 99, "left_value": "unmatched"},
    ]
    right_rows = [
        {"right_id": 100, "entity_id": 10, "right_value": "x"},
        {"right_id": 200, "entity_id": 20, "right_value": "y"},
    ]
    left = TableNode("left", left_rows)
    right = TableNode("right", right_rows)
    join = LocalJoinNode(
        "join",
        ColumnInfo(["entity_id"], [], "event_timestamp", None),
        PandasBackend(),
        inputs=[left, right],
        how=how,
    )
    plan = ExecutionPlan([left, right, join])
    execution_context = context()
    plan.initialize_incremental(
        execution_context,
        {
            "left": ("left_id",),
            "right": ("right_id",),
            "join": ("left_id", "right_id"),
        },
    )
    return plan, execution_context


def assert_single_join_matches_batch(plan, execution_context):
    left = list(execution_context.node_states["left"].rows.values())
    right = list(execution_context.node_states["right"].rows.values())
    expected = pd.DataFrame(left).merge(
        pd.DataFrame(right), on="entity_id", how="inner"
    )
    actual = list(plan.incremental_result(execution_context).rows.values())
    pd.testing.assert_frame_equal(
        normalized(actual, ["left_id", "right_id"]),
        normalized(expected.to_dict("records"), ["left_id", "right_id"]),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "source,record,operation",
    [
        (
            "left",
            ChangeRecord.insert(
                {"left_id": 3, "entity_id": 20, "left_value": "inserted"}
            ),
            ChangeOperation.INSERT,
        ),
        (
            "right",
            ChangeRecord.insert(
                {"right_id": 300, "entity_id": 99, "right_value": "inserted"}
            ),
            ChangeOperation.INSERT,
        ),
        (
            "left",
            ChangeRecord.update(
                {"left_id": 1, "entity_id": 10, "left_value": "a"},
                {"left_id": 1, "entity_id": 10, "left_value": "updated"},
            ),
            ChangeOperation.UPDATE,
        ),
        (
            "right",
            ChangeRecord.update(
                {"right_id": 100, "entity_id": 10, "right_value": "x"},
                {"right_id": 100, "entity_id": 10, "right_value": "updated"},
            ),
            ChangeOperation.UPDATE,
        ),
        (
            "left",
            ChangeRecord.delete({"left_id": 1, "entity_id": 10, "left_value": "a"}),
            ChangeOperation.DELETE,
        ),
        (
            "right",
            ChangeRecord.delete({"right_id": 100, "entity_id": 10, "right_value": "x"}),
            ChangeOperation.DELETE,
        ),
    ],
)
def test_left_and_right_changes_match_batch(source, record, operation):
    plan, execution_context = single_join_plan()
    result = plan.execute_incremental(
        execution_context, {source: ChangeSet(source, (record,))}
    )
    assert [change.operation for change in result.records] == [operation]
    assert_single_join_matches_batch(plan, execution_context)


def test_key_changing_update_retracts_old_match_and_inserts_new_match():
    plan, execution_context = single_join_plan()
    result = plan.execute_incremental(
        execution_context,
        {
            "left": ChangeSet(
                "left",
                (
                    ChangeRecord.update(
                        {"left_id": 1, "entity_id": 10, "left_value": "a"},
                        {"left_id": 1, "entity_id": 20, "left_value": "a"},
                    ),
                ),
            )
        },
    )
    assert {record.operation for record in result.records} == {
        ChangeOperation.DELETE,
        ChangeOperation.INSERT,
    }
    assert_single_join_matches_batch(plan, execution_context)


def test_unmatched_update_becomes_matched():
    plan, execution_context = single_join_plan()
    result = plan.execute_incremental(
        execution_context,
        {
            "left": ChangeSet(
                "left",
                (
                    ChangeRecord.update(
                        {"left_id": 2, "entity_id": 99, "left_value": "unmatched"},
                        {"left_id": 2, "entity_id": 20, "left_value": "matched"},
                    ),
                ),
            )
        },
    )
    assert [record.operation for record in result.records] == [ChangeOperation.INSERT]
    assert_single_join_matches_batch(plan, execution_context)


def test_mixed_micro_batch_matches_batch():
    plan, execution_context = single_join_plan()
    plan.execute_incremental(
        execution_context,
        {
            "left": ChangeSet(
                "left",
                (
                    ChangeRecord.update(
                        {"left_id": 1, "entity_id": 10, "left_value": "a"},
                        {"left_id": 1, "entity_id": 10, "left_value": "new"},
                    ),
                    ChangeRecord.insert(
                        {"left_id": 3, "entity_id": 20, "left_value": "three"}
                    ),
                ),
            ),
            "right": ChangeSet(
                "right",
                (
                    ChangeRecord.update(
                        {"right_id": 200, "entity_id": 20, "right_value": "y"},
                        {"right_id": 200, "entity_id": 20, "right_value": "new-y"},
                    ),
                ),
            ),
        },
    )
    assert_single_join_matches_batch(plan, execution_context)


def chained_plan():
    customers = TableNode(
        "customers",
        [{"customer_id": 1, "region_id": 7, "customer_name": "Ada"}],
    )
    orders = TableNode("orders", [{"order_id": 10, "customer_id": 1, "amount": 5}])
    regions = TableNode("regions", [{"region_id": 7, "region_name": "west"}])
    customer_orders = LocalJoinNode(
        "customer_orders",
        ColumnInfo(["customer_id"], [], "event_timestamp", None),
        PandasBackend(),
        inputs=[customers, orders],
    )
    enriched = LocalJoinNode(
        "enriched",
        ColumnInfo(["region_id"], [], "event_timestamp", None),
        PandasBackend(),
        inputs=[customer_orders, regions],
    )
    plan = ExecutionPlan([customers, orders, customer_orders, regions, enriched])
    execution_context = context()
    plan.initialize_incremental(
        execution_context,
        {
            "customers": ("customer_id",),
            "orders": ("order_id",),
            "customer_orders": ("order_id",),
            "regions": ("region_id",),
            "enriched": ("order_id",),
        },
    )
    return plan, execution_context


def assert_chain_matches_batch(plan, execution_context):
    def rows(name):
        return list(execution_context.node_states[name].rows.values())

    expected = pd.DataFrame(rows("customers")).merge(
        pd.DataFrame(rows("orders")), on="customer_id"
    )
    expected = expected.merge(pd.DataFrame(rows("regions")), on="region_id")
    actual = list(plan.incremental_result(execution_context).rows.values())
    pd.testing.assert_frame_equal(
        normalized(actual, ["order_id"]),
        normalized(expected.to_dict("records"), ["order_id"]),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "source,record",
    [
        (
            "customers",
            ChangeRecord.update(
                {"customer_id": 1, "region_id": 7, "customer_name": "Ada"},
                {"customer_id": 1, "region_id": 7, "customer_name": "Grace"},
            ),
        ),
        (
            "orders",
            ChangeRecord.update(
                {"order_id": 10, "customer_id": 1, "amount": 5},
                {"order_id": 10, "customer_id": 1, "amount": 8},
            ),
        ),
        (
            "regions",
            ChangeRecord.update(
                {"region_id": 7, "region_name": "west"},
                {"region_id": 7, "region_name": "coast"},
            ),
        ),
    ],
)
def test_chained_join_propagates_changes_from_every_source(source, record):
    plan, execution_context = chained_plan()
    result = plan.execute_incremental(
        execution_context, {source: ChangeSet(source, (record,))}
    )
    assert result.source == "enriched"
    assert [item.operation for item in result.records] == [ChangeOperation.UPDATE]
    assert_chain_matches_batch(plan, execution_context)


def test_rejects_unsupported_join_mode():
    with pytest.raises(ValueError, match="only supports inner joins"):
        single_join_plan(how="left")


def test_rejects_missing_join_key_without_mutating_state():
    plan, execution_context = single_join_plan()
    before = execution_context.node_states["join"].output.clone()
    with pytest.raises(ValueError, match="entity_id"):
        plan.execute_incremental(
            execution_context,
            {
                "left": ChangeSet(
                    "left",
                    (ChangeRecord.insert({"left_id": 3, "left_value": "missing-key"}),),
                )
            },
        )
    assert execution_context.node_states["join"].output == before
