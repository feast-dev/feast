import copy
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from feast.infra.compute_engines.dag.change import ChangeRecord, ChangeSet, KeyedRows
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue


class StaticNode(DAGNode):
    def __init__(self, name, rows, inputs=None):
        super().__init__(name, inputs)
        self.rows = rows

    def execute(self, context):
        return ArrowTableValue(pa.Table.from_pylist(self.rows))


class RecordingNode(StaticNode):
    def __init__(self, name, rows, inputs=None, fail=False):
        super().__init__(name, rows, inputs)
        self.received = []
        self.fail = fail

    def execute_incremental(self, context, input_changes):
        self.received.append(input_changes)
        if self.fail:
            context.node_states[self.name].rows[(1,)]["value"] = "corrupt"
            raise RuntimeError("downstream failure")
        records = tuple(
            record for changes in input_changes.values() for record in changes.records
        )
        changes = ChangeSet(self.name, records)
        context.node_states[self.name].apply(changes)
        return changes


def create_context():
    return ExecutionContext(
        project="test",
        repo_config=MagicMock(),
        offline_store=MagicMock(),
        online_store=MagicMock(),
        entity_defs=[],
    )


def test_initialize_incremental_seeds_keyed_state():
    source = StaticNode("source", [{"id": 1, "value": "a"}])
    plan = ExecutionPlan([source])
    context = create_context()

    result = plan.initialize_incremental(context, {"source": ("id",)})

    assert result.data.to_pylist() == [{"id": 1, "value": "a"}]
    assert context.incremental_initialized is True
    assert context.node_states["source"].rows[(1,)]["value"] == "a"


def test_initialize_incremental_requires_primary_keys_for_every_node():
    plan = ExecutionPlan([StaticNode("source", [{"id": 1}])])
    with pytest.raises(ValueError, match="Missing primary keys for node 'source'"):
        plan.initialize_incremental(create_context(), {})


def test_execute_incremental_requires_initialization():
    plan = ExecutionPlan([StaticNode("source", [{"id": 1}])])
    with pytest.raises(RuntimeError, match="not initialized"):
        plan.execute_incremental(create_context(), {})


def test_execute_incremental_routes_changes_by_input_name_and_skips_other_branch():
    left = StaticNode("left", [{"id": 1, "value": "old"}])
    right = StaticNode("right", [{"id": 2, "value": "stable"}])
    sink = RecordingNode("sink", [{"id": 1, "value": "old"}], inputs=[left, right])
    plan = ExecutionPlan([left, right, sink])
    context = create_context()
    plan.initialize_incremental(
        context, {"left": ("id",), "right": ("id",), "sink": ("id",)}
    )
    left_changes = ChangeSet(
        "left",
        (ChangeRecord.update({"id": 1, "value": "old"}, {"id": 1, "value": "new"}),),
    )

    result = plan.execute_incremental(context, {"left": left_changes})

    assert set(sink.received[0]) == {"left"}
    assert result.source == "sink"
    assert context.node_states["right"].rows[(2,)]["value"] == "stable"
    assert plan.incremental_result(context).rows[(1,)]["value"] == "new"


def test_execute_incremental_rejects_unknown_or_mismatched_source():
    source = StaticNode("source", [{"id": 1}])
    plan = ExecutionPlan([source])
    context = create_context()
    plan.initialize_incremental(context, {"source": ("id",)})
    changes = ChangeSet("source", (ChangeRecord.insert({"id": 2}),))

    with pytest.raises(ValueError, match="Unknown source node 'missing'"):
        plan.execute_incremental(context, {"missing": changes})
    with pytest.raises(ValueError, match="does not match target node"):
        plan.execute_incremental(context, {"source": ChangeSet("other")})


def test_execute_incremental_rejects_non_source_target():
    source = StaticNode("source", [{"id": 1}])
    sink = RecordingNode("sink", [{"id": 1}], inputs=[source])
    plan = ExecutionPlan([source, sink])
    context = create_context()
    plan.initialize_incremental(context, {"source": ("id",), "sink": ("id",)})

    with pytest.raises(ValueError, match="not a source node"):
        plan.execute_incremental(context, {"sink": ChangeSet("sink")})


def test_execute_incremental_rolls_back_all_state_on_downstream_failure():
    source = StaticNode("source", [{"id": 1, "value": "old"}])
    sink = RecordingNode(
        "sink", [{"id": 1, "value": "old"}], inputs=[source], fail=True
    )
    plan = ExecutionPlan([source, sink])
    context = create_context()
    plan.initialize_incremental(context, {"source": ("id",), "sink": ("id",)})
    before = copy.deepcopy(context.node_states)
    changes = ChangeSet(
        "source",
        (ChangeRecord.update({"id": 1, "value": "old"}, {"id": 1, "value": "new"}),),
    )

    with pytest.raises(RuntimeError, match="downstream failure"):
        plan.execute_incremental(context, {"source": changes})

    assert context.node_states == before


def test_batch_only_node_rejects_incremental_changes():
    source = StaticNode("source", [{"id": 1}])
    sink = StaticNode("sink", [{"id": 1}], inputs=[source])
    plan = ExecutionPlan([source, sink])
    context = create_context()
    plan.initialize_incremental(context, {"source": ("id",), "sink": ("id",)})

    with pytest.raises(NotImplementedError, match="does not support incremental"):
        plan.execute_incremental(
            context,
            {"source": ChangeSet("source", (ChangeRecord.insert({"id": 2}),))},
        )


def test_incremental_result_requires_initialization():
    plan = ExecutionPlan([StaticNode("source", [{"id": 1}])])
    with pytest.raises(RuntimeError, match="not initialized"):
        plan.incremental_result(create_context())


def test_keyed_rows_type_is_used_for_initialized_state():
    plan = ExecutionPlan([StaticNode("source", [{"id": 1}])])
    context = create_context()
    plan.initialize_incremental(context, {"source": ("id",)})
    assert isinstance(plan.incremental_result(context), KeyedRows)
