from unittest.mock import MagicMock

from feast.data_source import DataSource
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.feature_builder import FeatureBuilder

# ---------------------------
# Minimal Mock DAGNode for testing
# ---------------------------


class MockDAGNode(DAGNode):
    def __init__(self, name, inputs=None):
        super().__init__(name, inputs=inputs or [])

    def execute(self, context: ExecutionContext) -> DAGValue:
        return DAGValue(data=None, format=DAGFormat.SPARK, metadata={})


# ---------------------------
# Mock Feature View Definitions
# ---------------------------


class MockFeatureView:
    def __init__(
        self,
        name,
        source=None,
        aggregations=None,
        feature_transformation=None,
    ):
        self.name = name
        self.source = source

        # Internal resolution (emulating what real FeatureView.__init__ would do)
        self.data_source = source if isinstance(source, DataSource) else None
        self.source_views = (
            [source]
            if isinstance(source, MockFeatureView)
            else source
            if isinstance(source, list)
            else []
        )

        self.aggregations = aggregations or []
        self.feature_transformation = feature_transformation
        self.ttl = None
        self.filter = None
        self.enable_validation = False
        self.entities = ["driver_id"]
        self.batch_source = type("BatchSource", (), {"timestamp_field": "ts"})
        self.stream_source = None
        self.tags = {}


class MockTransformation:
    def __init__(self, name):
        self.name = name
        self.udf = lambda df: df


mock_source = MagicMock(spec=DataSource)

# ---------------------------
# Mock DAG
# ---------------------------

hourly_driver_stats = MockFeatureView(
    name="hourly_driver_stats",
    source=mock_source,
    aggregations=[{"function": "sum", "column": "trips"}],
    feature_transformation=MockTransformation("hourly_tf"),
)

daily_driver_stats = MockFeatureView(
    name="daily_driver_stats",
    source=hourly_driver_stats,
    aggregations=[{"function": "mean", "column": "trips"}],
    feature_transformation=MockTransformation("daily_tf"),
)


# ---------------------------
# Mock FeatureBuilder
# ---------------------------


class MockFeatureBuilder(FeatureBuilder):
    def __init__(self, feature_view):
        super().__init__(
            registry=MagicMock(), feature_view=feature_view, task=MagicMock()
        )

    def build_source_node(self, view):
        return MockDAGNode(f"Source({view.name})")

    def build_join_node(self, view, input_nodes):
        return MockDAGNode(f"Join({view.name})", inputs=input_nodes)

    def build_filter_node(self, view, input_node):
        return MockDAGNode(f"Filter({view.name})", inputs=[input_node])

    def build_aggregation_node(self, view, input_node):
        return MockDAGNode(f"Agg({view.name})", inputs=[input_node])

    def build_dedup_node(self, view, input_node):
        return MockDAGNode(f"Dedup({view.name})", inputs=[input_node])

    def build_transformation_node(self, view, input_nodes):
        return MockDAGNode(f"Transform({view.name})", inputs=input_nodes)

    def build_validation_node(self, view, input_node):
        return MockDAGNode(f"Validate({view.name})", inputs=[input_node])

    def build_output_nodes(self, final_node):
        output_node = MockDAGNode(f"Output({final_node.name})", inputs=[final_node])
        self.nodes.append(output_node)
        return output_node


# ---------------------------
# Test
# ---------------------------


def test_recursive_featureview_build():
    builder = MockFeatureBuilder(daily_driver_stats)
    execution_plan: ExecutionPlan = builder.build()

    expected_output = """\
- Output(Agg(daily_driver_stats))
  - Agg(daily_driver_stats)
    - Filter(daily_driver_stats)
      - Transform(daily_driver_stats)
        - Agg(hourly_driver_stats)
          - Filter(hourly_driver_stats)
            - Transform(hourly_driver_stats)
              - Source(hourly_driver_stats)"""

    assert execution_plan.to_dag() == expected_output
