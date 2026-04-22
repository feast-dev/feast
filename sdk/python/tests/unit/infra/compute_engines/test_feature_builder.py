from unittest.mock import MagicMock, patch

import pytest

from feast.data_source import DataSource
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.feature_builder import FeatureBuilder
from feast.transformation.mode import TransformationMode

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

    def build_output_nodes(self, view, final_node):
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


# ---------------------------------------------------------------------------
# Helpers for get_column_info tests
# ---------------------------------------------------------------------------

# Stable return value for _get_column_names: (join_keys, feature_cols, ts_col, created_ts_col)
_MOCK_COLUMN_NAMES = (
    ["user_id"],
    ["user_avg_rating", "user_review_count"],
    "event_timestamp",
    None,
)


def _make_transformation(mode):
    """Return a minimal transformation stub with the given mode."""
    t = MagicMock()
    t.mode = mode
    return t


def _make_builder_for_column_info(transformation):
    """
    Build a MockFeatureBuilder whose task.feature_view carries the given
    transformation. registry.get_entity is stubbed out per entity name.
    """
    view = MagicMock()
    view.entities = ["user"]
    view.feature_transformation = transformation
    view.batch_source = MagicMock()
    view.batch_source.field_mapping = {}
    view.stream_source = None

    task = MagicMock()
    task.project = "test_project"
    task.feature_view = view
    task.only_latest = False

    registry = MagicMock()
    registry.get_entity.return_value = MagicMock(join_key="user_id")

    builder = MockFeatureBuilder.__new__(MockFeatureBuilder)
    builder.registry = registry
    builder.task = task
    builder.nodes = []
    return builder, view


# ---------------------------------------------------------------------------
# Bug fix: TransformationMode.PYTHON must set feature_cols=[]
#
# Previously only "ray" and "pandas" were handled. "python" (the default mode
# for @batch_feature_view) was missing, causing get_column_info to forward
# the BFV *output* feature names (e.g. user_avg_rating) to the offline store
# read step — columns that don't exist in raw source data — resulting in
# UNRESOLVED_COLUMN errors at Spark analysis time.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode",
    [
        TransformationMode.PYTHON,
        TransformationMode.PANDAS,
        TransformationMode.RAY,
        # String forms (getattr(mode, "value", None) path)
        "python",
        "pandas",
        "ray",
    ],
)
def test_get_column_info_clears_feature_cols_for_udf_modes(mode):
    """
    For transformation modes that compute output features from raw input
    (python, pandas, ray), get_column_info must set feature_cols=[] so the
    offline store read step issues SELECT * instead of projecting the output
    feature names that don't exist in the raw source schema.
    """
    builder, view = _make_builder_for_column_info(_make_transformation(mode))

    with patch(
        "feast.infra.compute_engines.feature_builder._get_column_names",
        return_value=_MOCK_COLUMN_NAMES,
    ):
        col_info = builder.get_column_info(view)

    assert col_info.feature_cols == [], (
        f"Expected feature_cols=[] for TransformationMode {mode!r}, "
        f"got {col_info.feature_cols!r}. "
        "The offline store read step must not project output feature names "
        "that don't exist in the raw source schema."
    )
    assert col_info.join_keys == ["user_id"]
    assert col_info.ts_col == "event_timestamp"


@pytest.mark.parametrize(
    "mode",
    [
        TransformationMode.SPARK_SQL,
        TransformationMode.SQL,
        TransformationMode.SPARK,
        "spark_sql",
        "sql",
    ],
)
def test_get_column_info_preserves_feature_cols_for_non_udf_modes(mode):
    """
    SQL/Spark-SQL transformations operate on already-projected columns and
    should NOT get feature_cols cleared — the source read must still select
    the named feature columns explicitly.
    """
    builder, view = _make_builder_for_column_info(_make_transformation(mode))

    with patch(
        "feast.infra.compute_engines.feature_builder._get_column_names",
        return_value=_MOCK_COLUMN_NAMES,
    ):
        col_info = builder.get_column_info(view)

    assert col_info.feature_cols == ["user_avg_rating", "user_review_count"], (
        f"Expected feature_cols to be preserved for mode {mode!r}, "
        f"got {col_info.feature_cols!r}."
    )


def test_get_column_info_preserves_feature_cols_with_no_transformation():
    """
    A plain FeatureView (no transformation) must retain its feature column
    names so the offline store read step selects only the required columns.
    """
    builder, view = _make_builder_for_column_info(None)

    with patch(
        "feast.infra.compute_engines.feature_builder._get_column_names",
        return_value=_MOCK_COLUMN_NAMES,
    ):
        col_info = builder.get_column_info(view)

    assert col_info.feature_cols == ["user_avg_rating", "user_review_count"]
