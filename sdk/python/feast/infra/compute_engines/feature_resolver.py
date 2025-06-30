from feast.feature_view import FeatureView

from typing import Set, List, Optional


class FeatureViewNode:
    """
    Logical representation of a node in the FeatureView dependency DAG.
    """
    def __init__(self,
                 view: FeatureView):
        self.view: FeatureView = view
        self.parent: Optional["FeatureViewNode"] = None


class FeatureResolver:
    """
    Resolves FeatureViews into a dependency graph (DAG) of FeatureViewNode objects.
    This graph represents the logical dependencies between FeatureViews, allowing
    for ordered execution and cycle detection.
    """
    def __init__(self):
        # Used to detect and prevent cycles in the FeatureView graph.
        self.visited: Set[str] = set()
        self.resolution_path: List[str] = []

    def resolve(self,
                feature_view: FeatureView) -> FeatureViewNode:
        """
        Entry point for resolving a FeatureView into a DAG node.

        Args:
            feature_view: The root FeatureView to build the dependency graph from.

        Returns:
            A FeatureViewNode representing the root of the logical dependency DAG.
        """
        root = FeatureViewNode(feature_view)
        self._walk(root)
        return root

    def _walk(self,
              node: FeatureViewNode):
        """
        Recursive traversal of the FeatureView graph.

        If `source_view` is set on the FeatureView, a parent node is created and added.
        Cycles are detected using the visited set.

        Args:
            node: The current FeatureViewNode being processed.
        """
        view = node.view
        if view.name in self.visited:
            cycle_path = " → ".join(self.resolution_path + [view.name])
            raise ValueError(f"Cycle detected in FeatureView graph: {cycle_path}")
        self.visited.add(view.name)
        self.resolution_path.append(view.name)

        # TODO: Only one parent is allowed via source_view, support more than one
        if view.source_view:
            parent_node = FeatureViewNode(view.source_view)
            node.parent = parent_node
            self._walk(parent_node)

        self.resolution_path.pop()

    def topo_sort(self, root: FeatureViewNode) -> List[FeatureViewNode]:
        visited = set()
        ordered: List[FeatureViewNode] = []

        def dfs(node: FeatureViewNode):
            if id(node) in visited:
                return
            visited.add(id(node))
            if node.parent:
                dfs(node.parent)
            ordered.append(node)

        dfs(root)
        return ordered

    def debug_dag(self,
                  node: FeatureViewNode,
                  depth=0):
        """
        Prints the FeatureView dependency DAG for debugging.

        Args:
            node: The root node to print from.
            depth: Internal argument used for recursive indentation.
        """
        indent = "  " * depth
        print(f"{indent}- {node.view.name}")
        if node.parent:
            self.debug_dag(node.parent, depth + 1)
