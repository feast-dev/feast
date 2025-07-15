from typing import List, Optional, Set

from feast.feature_view import FeatureView
from feast.infra.compute_engines.algorithms.topo import topological_sort
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue


class FeatureViewNode(DAGNode):
    """
    Logical representation of a node in the FeatureView dependency DAG.
    """

    def __init__(
        self, view: FeatureView, inputs: Optional[List["FeatureViewNode"]] = None
    ):
        super().__init__(name=view.name)
        self.view: FeatureView = view
        self.inputs: List["FeatureViewNode"] = inputs or []  # type: ignore

    def execute(self, context: ExecutionContext) -> DAGValue:
        raise NotImplementedError(
            f"FeatureViewNode '{self.name}' does not implement execute method."
        )


class FeatureResolver:
    """
    Resolves FeatureViews into a dependency graph (DAG) of FeatureViewNode objects.
    This graph represents the logical dependencies between FeatureViews, allowing
    for ordered execution and cycle detection.
    """

    def __init__(self):
        self._visited: Set[str] = set()
        self._resolution_path: List[str] = []
        self._node_cache: dict[str, FeatureViewNode] = {}

    def resolve(self, feature_view: FeatureView) -> FeatureViewNode:
        """
        Entry point for resolving a FeatureView into a DAG node.

        Args:
            feature_view: The root FeatureView to build the dependency graph from.

        Returns:
            A FeatureViewNode representing the root of the logical dependency DAG.
        """
        return self._walk(feature_view)

    def _walk(self, view: FeatureView):
        """
        Recursive traversal of the FeatureView graph.

        If `source_view` is set on the FeatureView, a parent node is created and added.
        Cycles are detected using the visited set.

        Args:
            view: The FeatureView to process.
        """
        if view.name in self._resolution_path:
            cycle = " → ".join(self._resolution_path + [view.name])
            raise ValueError(f"Cycle detected in FeatureView DAG: {cycle}")

        if view.name in self._node_cache:
            return self._node_cache[view.name]

        node = FeatureViewNode(view)
        self._node_cache[view.name] = node

        self._resolution_path.append(view.name)
        if view.source_views:
            for upstream_view in view.source_views:
                input_node = self._walk(upstream_view)
                node.inputs.append(input_node)
        self._resolution_path.pop()

        return node

    def topological_sort(self, root: FeatureViewNode) -> List[FeatureViewNode]:
        return topological_sort(root)  # type: ignore

    def debug_dag(self, node: FeatureViewNode, depth=0):
        """
        Prints the FeatureView dependency DAG for debugging.

        Args:
            node: The root node to print from.
            depth: Internal argument used for recursive indentation.
        """
        indent = "  " * depth
        print(f"{indent}- {node.view.name}")
        for input_node in node.inputs:
            self.debug_dag(input_node, depth + 1)  # type: ignore
