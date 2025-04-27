from typing import List

from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue


class ExecutionPlan:
    """
    ExecutionPlan represents an ordered sequence of DAGNodes that together define
    a data processing pipeline for feature materialization or historical retrieval.

    This plan is constructed as a topological sort of the DAG — meaning that each
    node appears after all its input dependencies. The plan is executed in order,
    caching intermediate results (`DAGValue`) so that each node can reuse outputs
    from upstream nodes without recomputation.

    Key Concepts:
    - DAGNode: Each node performs a specific logical step (e.g., read, aggregate, join).
    - DAGValue: Output of a node, includes data (e.g., Spark DataFrame) and metadata.
    - ExecutionContext: Contains runtime information (config, registry, stores, entity_df).
    - node_outputs: A cache of intermediate results keyed by node name.

    Usage:
        plan = ExecutionPlan(dag_nodes)
        result = plan.execute(context)

    This design enables modular compute backends (e.g., Spark, Pandas, Arrow), where
    each node defines its execution logic independently while benefiting from shared
    execution orchestration, caching, and context injection.

    Example:
        DAG:
            ReadNode -> AggregateNode -> JoinNode -> TransformNode -> WriteNode

        Execution proceeds step by step, passing intermediate DAGValues through
        the plan while respecting node dependencies and formats.

    This approach is inspired by execution DAGs in systems like Apache Spark,
    Apache Beam, and Dask — but specialized for Feast’s feature computation domain.
    """

    def __init__(self, nodes: List[DAGNode]):
        self.nodes = nodes

    def execute(self, context: ExecutionContext) -> DAGValue:
        context.node_outputs = {}

        for node in self.nodes:
            for input_node in node.inputs:
                if input_node.name not in context.node_outputs:
                    context.node_outputs[input_node.name] = input_node.execute(context)

            output = node.execute(context)
            context.node_outputs[node.name] = output

        return context.node_outputs[self.nodes[-1].name]

    def to_sql(self, context: ExecutionContext) -> str:
        """
        Generate SQL query for the entire execution plan.
        This is a placeholder and should be implemented in subclasses.
        """
        raise NotImplementedError("SQL generation is not implemented yet.")
