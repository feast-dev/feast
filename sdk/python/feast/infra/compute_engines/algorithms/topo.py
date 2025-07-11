from typing import List, Set

from feast.infra.compute_engines.dag.node import DAGNode


def topological_sort(root: DAGNode) -> List[DAGNode]:
    """
    Topologically sort a DAG starting from a single root node.

    Args:
        root: The root DAGNode.

    Returns:
        A list of DAGNodes in topological order (dependencies first).
    """
    return topological_sort_multiple([root])


def topological_sort_multiple(roots: List[DAGNode]) -> List[DAGNode]:
    """
    Topologically sort a DAG with multiple roots.

    Args:
        roots: List of root DAGNodes.

    Returns:
        A list of all reachable DAGNodes in execution-safe order.
    """
    visited: Set[int] = set()
    ordered: List[DAGNode] = []

    def dfs(node: DAGNode):
        if id(node) in visited:
            return
        visited.add(id(node))
        for input_node in node.inputs:
            dfs(input_node)
        ordered.append(node)

    for root in roots:
        dfs(root)

    return ordered
