from unittest.mock import MagicMock

from feast.infra.compute_engines.algorithms.topo import (
    topological_sort,
    topological_sort_multiple,
)
from feast.infra.compute_engines.dag.node import DAGNode


def _make_node(name, inputs=None):
    """Create a mock DAGNode."""
    node = MagicMock(spec=DAGNode)
    node.name = name
    node.inputs = inputs or []
    return node


class TestTopologicalSort:
    def test_single_node(self):
        root = _make_node("root")
        result = topological_sort(root)
        assert len(result) == 1
        assert result[0] is root

    def test_linear_chain(self):
        """A -> B -> C should produce [A, B, C]."""
        a = _make_node("A")
        b = _make_node("B", inputs=[a])
        c = _make_node("C", inputs=[b])

        result = topological_sort(c)
        assert len(result) == 3
        # Dependencies must come before dependents
        assert result.index(a) < result.index(b)
        assert result.index(b) < result.index(c)

    def test_diamond_dependency(self):
        """
        A → B
        A → C
        B,C → D
        Should visit A before B and C, and B/C before D.
        """
        a = _make_node("A")
        b = _make_node("B", inputs=[a])
        c = _make_node("C", inputs=[a])
        d = _make_node("D", inputs=[b, c])

        result = topological_sort(d)
        assert len(result) == 4
        assert result.index(a) < result.index(b)
        assert result.index(a) < result.index(c)
        assert result.index(b) < result.index(d)
        assert result.index(c) < result.index(d)

    def test_no_duplicates(self):
        """Shared dependencies should appear only once."""
        shared = _make_node("shared")
        b = _make_node("B", inputs=[shared])
        c = _make_node("C", inputs=[shared])
        root = _make_node("root", inputs=[b, c])

        result = topological_sort(root)
        assert result.count(shared) == 1


class TestTopologicalSortMultiple:
    def test_multiple_roots_no_overlap(self):
        r1 = _make_node("root1")
        r2 = _make_node("root2")

        result = topological_sort_multiple([r1, r2])
        assert len(result) == 2
        assert r1 in result
        assert r2 in result

    def test_multiple_roots_with_shared_dep(self):
        shared = _make_node("shared")
        r1 = _make_node("root1", inputs=[shared])
        r2 = _make_node("root2", inputs=[shared])

        result = topological_sort_multiple([r1, r2])
        assert len(result) == 3
        assert result.count(shared) == 1
        assert result.index(shared) < result.index(r1)
        assert result.index(shared) < result.index(r2)

    def test_empty_roots(self):
        result = topological_sort_multiple([])
        assert result == []
