from abc import ABC, abstractmethod
from typing import List

from infra.compute_engines.dag.value import DAGValue

from feast.infra.compute_engines.dag.model import ExecutionContext


class DAGNode(ABC):
    name: str
    inputs: List["DAGNode"]
    outputs: List["DAGNode"]

    def __init__(self, name: str):
        self.name = name
        self.inputs = []
        self.outputs = []

    def add_input(self, node: "DAGNode"):
        if node in self.inputs:
            raise ValueError(f"Input node {node.name} already added to {self.name}")
        self.inputs.append(node)
        node.outputs.append(self)

    @abstractmethod
    def execute(self, context: ExecutionContext) -> DAGValue: ...
