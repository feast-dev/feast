import logging
from typing import TYPE_CHECKING, Union

from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.feature_builder import FeatureBuilder
from feast.infra.compute_engines.ray.nodes import (
    RayAggregationNode,
    RayDedupNode,
    RayFilterNode,
    RayJoinNode,
    RayReadNode,
    RayTransformationNode,
    RayWriteNode,
)

if TYPE_CHECKING:
    from feast.infra.compute_engines.ray.config import RayComputeEngineConfig

logger = logging.getLogger(__name__)


class RayFeatureBuilder(FeatureBuilder):
    """
    Ray-specific feature builder that constructs execution plans using Ray DAG nodes.
    This builder translates FeatureView definitions into Ray-optimized execution DAGs
    that can leverage distributed computing for large-scale feature processing.
    """

    def __init__(
        self,
        registry,
        feature_view,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
        config: "RayComputeEngineConfig",
    ):
        super().__init__(registry, feature_view, task)
        self.config = config
        self.is_historical_retrieval = isinstance(task, HistoricalRetrievalTask)

    def build_source_node(self, view):
        """Build the source node for reading feature data."""
        source = view.batch_source
        start_time = self.task.start_time
        end_time = self.task.end_time
        column_info = self.get_column_info(view)

        node = RayReadNode(
            name="source",
            source=source,
            column_info=column_info,
            config=self.config,
            start_time=start_time,
            end_time=end_time,
        )

        self.nodes.append(node)
        logger.debug(f"Built source node for {source}")
        return node

    def build_join_node(self, view, input_nodes):
        """Build the join node for entity-feature joining."""
        column_info = self.get_column_info(view)
        node = RayJoinNode(
            name="join",
            column_info=column_info,
            config=self.config,
            # Pass entity_df information if this is a historical retrieval
            is_historical_retrieval=self.is_historical_retrieval,
        )
        for input_node in input_nodes:
            node.add_input(input_node)
        self.nodes.append(node)
        logger.debug("Built join node")
        return node

    def build_filter_node(self, view, input_node):
        """Build the filter node for TTL and custom filtering."""
        filter_expr = None
        if hasattr(view, "filter"):
            filter_expr = view.filter

        ttl = getattr(view, "ttl", None)
        column_info = self.get_column_info(view)

        node = RayFilterNode(
            name="filter",
            column_info=column_info,
            config=self.config,
            ttl=ttl,
            filter_condition=filter_expr,
        )

        node.add_input(input_node)
        self.nodes.append(node)
        logger.debug(f"Built filter node with TTL: {ttl}")
        return node

    def build_aggregation_node(self, view, input_node):
        """Build the aggregation node for feature aggregations."""
        if not hasattr(view, "aggregations"):
            raise ValueError("Feature view does not have aggregations")

        aggregations = view.aggregations
        group_by_keys = view.entities

        # Get timestamp field from batch source
        timestamp_field = getattr(
            view.batch_source, "timestamp_field", "event_timestamp"
        )

        node = RayAggregationNode(
            name="aggregation",
            aggregations=aggregations,
            group_by_keys=group_by_keys,
            timestamp_col=timestamp_field,
            config=self.config,
        )

        node.add_input(input_node)
        self.nodes.append(node)
        logger.debug(f"Built aggregation node with {len(aggregations)} aggregations")
        return node

    def build_dedup_node(self, view, input_node):
        """Build the deduplication node for removing duplicates."""
        column_info = self.get_column_info(view)
        node = RayDedupNode(
            name="dedup",
            column_info=column_info,
            config=self.config,
        )

        node.add_input(input_node)
        self.nodes.append(node)
        logger.debug("Built dedup node")
        return node

    def build_transformation_node(self, view, input_nodes):
        """Build the transformation node for feature transformations."""
        transformation = None

        # Check for feature_transformation first
        if hasattr(view, "feature_transformation") and view.feature_transformation:
            transformation = view.feature_transformation
        # For BatchFeatureView, also check for direct UDF
        elif hasattr(view, "udf") and view.udf:
            transformation = view.udf
        else:
            raise ValueError("Feature view does not have feature transformation or UDF")

        node = RayTransformationNode(
            name="transformation",
            transformation=transformation,
            config=self.config,
        )

        for input_node in input_nodes:
            node.add_input(input_node)
        self.nodes.append(node)
        transformation_name = getattr(
            transformation, "name", getattr(transformation, "__name__", "unknown")
        )
        logger.debug(f"Built transformation node: {transformation_name}")
        return node

    def build_output_nodes(self, view, final_node):
        """Build the output node for writing results."""
        node = RayWriteNode(
            name="output",
            feature_view=view,
            config=self.config,
        )

        node.add_input(final_node)
        self.nodes.append(node)
        logger.debug("Built output node")
        return node

    def build_validation_node(self, view, input_node):
        """Build the validation node for data quality checks."""
        # For now, validation is handled in the retrieval job
        # This could be extended to include Ray-specific validation logic
        logger.debug("Validation node not implemented yet")
        return input_node

    def build(self) -> ExecutionPlan:
        """Build execution plan with optimized order for aggregation scenarios."""

        # For historical retrieval with aggregations, use a different execution order
        if self.is_historical_retrieval and self._should_aggregate(self.feature_view):
            return self._build_aggregation_optimized_plan()

        # Use the default build logic for other scenarios
        return super().build()

    def _build_aggregation_optimized_plan(self) -> ExecutionPlan:
        """Build execution plan optimized for aggregation scenarios."""

        # 1. Read source data
        last_node = self.build_source_node(self.feature_view)

        # 2. Apply filters (TTL, custom filters) BEFORE aggregation
        last_node = self.build_filter_node(self.feature_view, last_node)

        # 3. Aggregate across all historical records
        last_node = self.build_aggregation_node(self.feature_view, last_node)

        # 4. Join with entity_df to get aggregated features for each entity
        last_node = self.build_join_node(self.feature_view, [last_node])

        # 5. Apply transformations to aggregated features
        if self._should_transform(self.feature_view):
            last_node = self.build_transformation_node(self.feature_view, [last_node])

        # 6. Validation if needed
        if self._should_validate(self.feature_view):
            last_node = self.build_validation_node(self.feature_view, last_node)

        # 7. Output
        last_node = self.build_output_nodes(self.feature_view, last_node)

        return ExecutionPlan(self.nodes)
