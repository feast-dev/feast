from unittest.mock import MagicMock

import pytest
from feast.infra.compute_engines.feature_builder import FeatureBuilder
from feast.infra.compute_engines.dag.plan import ExecutionPlan


# ---------------------------
# Mock Feature View Definitions
# ---------------------------

class MockFeatureView:
    def __init__(self,
                 name,
                 source=None,
                 source_view=None,
                 aggregations=None,
                 feature_transformation=None):
        self.name = name
        self.source = source
        self.source_view = source_view
        self.aggregations = aggregations or []
        self.feature_transformation = feature_transformation
        self.ttl = None
        self.filter = None
        self.enable_validation = False
        self.entities = ["driver_id"]
        self.batch_source = type("BatchSource", (), {"timestamp_field": "ts"})


class MockTransformation:
    def __init__(self,
                 name):
        self.name = name
        self.udf = lambda df: df


# ---------------------------
# Mock DAG
# ---------------------------

hourly_driver_stats = MockFeatureView(
    name="hourly_driver_stats",
    source="hourly_source",
    aggregations=[{"function": "sum", "column": "trips"}],
    feature_transformation=MockTransformation("hourly_tf"),
)

daily_driver_stats = MockFeatureView(
    name="daily_driver_stats",
    source_view=hourly_driver_stats,
    aggregations=[{"function": "mean", "column": "trips"}],
    feature_transformation=MockTransformation("daily_tf"),
)


# ---------------------------
# Mock FeatureBuilder
# ---------------------------

class MockFeatureBuilder(FeatureBuilder):
    def __init__(self,
                 feature_view):
        super().__init__(registry=MagicMock(), feature_view=feature_view, task=MagicMock())

    def build_source_node(self,
                          source):
        return f"SourceNode({source})"

    def build_join_node(self,
                        view,
                        input_node):
        return f"JoinNode({view.name} <- {input_node})"

    def build_filter_node(self,
                          view,
                          input_node):
        return f"FilterNode({view.name} <- {input_node})"

    def build_aggregation_node(self,
                               view,
                               input_node):
        return f"AggregationNode({view.name} <- {input_node})"

    def build_dedup_node(self,
                         view,
                         input_node):
        return f"DedupNode({view.name} <- {input_node})"

    def build_transformation_node(self,
                                  view,
                                  input_node):
        return f"TransformNode({view.name} <- {input_node})"

    def build_validation_node(self,
                              view,
                              input_node):
        return f"ValidationNode({view.name} <- {input_node})"

    def build_output_nodes(self,
                           final_node):
        self.nodes.append(f"OutputNode({final_node})")


# ---------------------------
# Test
# ---------------------------

def test_recursive_featureview_build():
    builder = MockFeatureBuilder(daily_driver_stats)
    execution_plan: ExecutionPlan = builder.build()

    expected_final_node = (
        "TransformNode(daily_driver_stats <- "
        "AggregationNode(daily_driver_stats <- "
        "FilterNode(daily_driver_stats <- "
        "JoinNode(daily_driver_stats <- "
        "TransformNode(hourly_driver_stats <- "
        "AggregationNode(hourly_driver_stats <- "
        "FilterNode(hourly_driver_stats <- "
        "SourceNode(hourly_source))))))))"
    )
    expected_output_node = f"OutputNode({expected_final_node})"

    assert execution_plan.nodes[-1] == expected_output_node
