from abc import ABC, abstractmethod
from typing import List, Mapping, Optional

from feast.infra.compute_engines.dag.change import ChangeSet, KeyedRows
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.value import DAGValue


class DAGNode(ABC):
    name: str
    inputs: List["DAGNode"]
    outputs: List["DAGNode"]

    def __init__(self, name: str, inputs: Optional[List["DAGNode"]] = None):
        self.name = name
        self.inputs: List["DAGNode"] = []
        self.outputs: List["DAGNode"] = []

        for node in inputs or []:
            self.add_input(node)

    def add_input(self, node: "DAGNode"):
        if node in self.inputs:
            raise ValueError(f"Input node {node.name} already added to {self.name}")
        self.inputs.append(node)
        node.outputs.append(self)

    def get_input_values(self, context: ExecutionContext) -> List[DAGValue]:
        input_values = []
        for input_node in self.inputs:
            if input_node.name not in context.node_outputs:
                raise KeyError(
                    f"Missing output for input node '{input_node.name}' in context."
                )
            input_values.append(context.node_outputs[input_node.name])
        return input_values

    def get_single_input_value(self, context: ExecutionContext) -> DAGValue:
        if len(self.inputs) != 1:
            raise RuntimeError(
                f"DAGNode '{self.name}' expected exactly 1 input, but got {len(self.inputs)}."
            )
        input_node = self.inputs[0]
        if input_node.name not in context.node_outputs:
            raise KeyError(
                f"Missing output for input node '{input_node.name}' in context."
            )
        return context.node_outputs[input_node.name]

    @abstractmethod
    def execute(self, context: ExecutionContext) -> DAGValue: ...

    def initialize_incremental(
        self, context: ExecutionContext, primary_keys: tuple[str, ...]
    ) -> None:
        value = context.node_outputs[self.name]
        table = getattr(value, "data", None)
        if table is None or not hasattr(table, "to_pylist"):
            raise TypeError(
                f"DAGNode '{self.name}' requires an Arrow-like batch output "
                "to initialize incremental state"
            )
        context.node_states[self.name] = KeyedRows.from_rows(
            table.to_pylist(), primary_keys
        )

    def execute_incremental(
        self,
        context: ExecutionContext,
        input_changes: Mapping[str, ChangeSet],
    ) -> ChangeSet:
        raise NotImplementedError(
            f"DAGNode '{self.name}' does not support incremental execution"
        )
