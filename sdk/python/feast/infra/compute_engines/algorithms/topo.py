# feast/infra/compute_engines/dag/utils.py

from typing import List, Set
from feast.infra.compute_engines.dag.node import DAGNode


def topo_sort(root: DAGNode) -> List[DAGNode]:
    """
    Topologically sort a DAGNode graph starting from root.

    Returns:
        List of DAGNodes in execution-safe order (dependencies first).
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

    dfs(root)
    return ordered


def topo_sort_multiple(roots: List[DAGNode]) -> List[DAGNode]:
    """
    Topologically sort a DAG with multiple roots (e.g., multiple write nodes).

    Returns:
        List of all reachable DAGNodes in execution order.
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
