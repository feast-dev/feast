from abc import ABC
from typing import List, cast

from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue


class LocalNode(DAGNode, ABC):
    def get_single_table(self, context: ExecutionContext) -> ArrowTableValue:
        return cast(ArrowTableValue, self.get_single_input_value(context))

    def get_input_tables(self, context: ExecutionContext) -> List[ArrowTableValue]:
        return [cast(ArrowTableValue, val) for val in self.get_input_values(context)]
