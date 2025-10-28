import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Union, cast

from feast import FeatureView
from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.algorithms.topo import topological_sort
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.feature_builder import FeatureBuilder
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.nodes import (
    RayAggregationNode,
    RayDedupNode,
    RayDerivedReadNode,
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
        self.is_materialization = isinstance(task, MaterializationTask)

    def build_source_node(self, view):
        """Build the source node for reading feature data."""
        data_source = getattr(view, "batch_source", None) or getattr(
            view, "source", None
        )
        start_time = self.task.start_time
        end_time = self.task.end_time
        column_info = self.get_column_info(view)

        node = RayReadNode(
            name="source",
            source=data_source,
            column_info=column_info,
            config=self.config,
            start_time=start_time,
            end_time=end_time,
        )

        self.nodes.append(node)
        return node

    def build_aggregation_node(self, view, input_node: DAGNode) -> DAGNode:
        """Build aggregation node for Ray."""
        agg_specs = getattr(view, "aggregations", [])
        if not agg_specs:
            raise ValueError(f"No aggregations found for {view.name}")

        group_by_keys = view.entities
        timestamp_col = getattr(view.batch_source, "timestamp_field", "event_timestamp")

        node = RayAggregationNode(
            name="aggregation",
            aggregations=agg_specs,
            group_by_keys=group_by_keys,
            timestamp_col=timestamp_col,
            config=self.config,
        )
        node.add_input(input_node)

        self.nodes.append(node)
        return node

    def build_join_node(self, view, input_nodes):
        """Build the join node for combining multiple feature sources."""
        column_info = self.get_column_info(view)

        node = RayJoinNode(
            name="join",
            column_info=column_info,
            config=self.config,
            is_historical_retrieval=self.is_historical_retrieval,
        )
        for input_node in input_nodes:
            node.add_input(input_node)

        self.nodes.append(node)
        return node

    def build_filter_node(self, view, input_node):
        """Build the filter node for TTL and custom filtering."""
        ttl = getattr(view, "ttl", None)
        filter_condition = getattr(view, "filter", None)
        column_info = self.get_column_info(view)

        node = RayFilterNode(
            name="filter",
            column_info=column_info,
            config=self.config,
            ttl=ttl,
            filter_condition=filter_condition,
        )
        node.add_input(input_node)

        self.nodes.append(node)
        return node

    def build_dedup_node(self, view, input_node):
        """Build the deduplication node for removing duplicate records."""
        column_info = self.get_column_info(view)

        node = RayDedupNode(
            name="dedup",
            column_info=column_info,
            config=self.config,
        )
        node.add_input(input_node)

        self.nodes.append(node)
        return node

    def build_transformation_node(self, view, input_nodes):
        """Build the transformation node for user-defined transformations."""
        feature_transformation = getattr(view, "feature_transformation", None)
        udf = getattr(view, "udf", None)

        transformation = feature_transformation or udf
        if not transformation:
            raise ValueError(f"No feature transformation found for {view.name}")

        node = RayTransformationNode(
            name="transformation",
            transformation=transformation,
            config=self.config,
        )
        for input_node in input_nodes:
            node.add_input(input_node)

        self.nodes.append(node)
        return node

    def build_output_nodes(self, view, final_node):
        """Build the output node for writing processed features."""
        node = RayWriteNode(
            name="output",
            feature_view=view,
            inputs=[final_node],
            config=self.config,
        )

        self.nodes.append(node)
        return node

    def build_validation_node(self, view, input_node):
        """Build the validation node for feature validation."""
        # TODO: Implement validation logic
        logger.warning(
            "Feature validation is not yet implemented for Ray compute engine."
        )
        return input_node

    def _build(self, view, input_nodes: Optional[List[DAGNode]]) -> DAGNode:
        has_physical_source = (hasattr(view, "batch_source") and view.batch_source) or (
            hasattr(view, "source")
            and view.source
            and not isinstance(view.source, FeatureView)
        )

        is_derived_view = hasattr(view, "source_views") and view.source_views

        if has_physical_source and not is_derived_view:
            last_node = self.build_source_node(view)
            if self._should_transform(view):
                last_node = self.build_transformation_node(view, [last_node])
        elif input_nodes:
            if self._should_transform(view):
                last_node = self.build_transformation_node(view, input_nodes)
            else:
                last_node = self.build_join_node(view, input_nodes)
        else:
            raise ValueError(f"FeatureView {view.name} has no valid source or inputs")

        if last_node is None:
            raise ValueError(f"Failed to build processing node for {view.name}")

        last_node = self.build_filter_node(view, last_node)

        if self._should_aggregate(view):
            last_node = self.build_aggregation_node(view, last_node)
        elif self._should_dedupe(view):
            last_node = self.build_dedup_node(view, last_node)

        if self._should_validate(view):
            last_node = self.build_validation_node(view, last_node)

        return last_node

    def build(self) -> ExecutionPlan:
        """Build execution plan with support for derived feature views and sink_source writing."""
        if self.is_historical_retrieval and self._should_aggregate(self.feature_view):
            return self._build_aggregation_optimized_plan()

        if self.is_materialization:
            return self._build_materialization_plan()

        return super().build()

    def _build_materialization_plan(self) -> ExecutionPlan:
        """Build execution plan for materialization with intermediate sink writes."""
        logger.info(f"Building materialization plan for {self.feature_view.name}")

        # Step 1: Topo sort the FeatureViewNode DAG (Logical DAG)
        logical_nodes = self.feature_resolver.topological_sort(self.dag_root)
        logger.info(
            f"Logical nodes in topo order: {[node.view.name for node in logical_nodes]}"
        )

        # Step 2: For each FeatureView, build its corresponding execution DAGNode and write node
        # Build them in dependency order to ensure proper execution
        view_to_write_node: Dict[str, RayWriteNode] = {}

        for i, logical_node in enumerate(logical_nodes):
            view = logical_node.view
            logger.info(
                f"Building nodes for view {view.name} (step {i + 1}/{len(logical_nodes)})"
            )

            # For derived views, we need to ensure parent views are materialized first
            # So we create a processing chain that depends on parent write nodes
            parent_write_nodes = []
            processing_node: DAGNode
            if hasattr(view, "source_views") and view.source_views:
                # This is a derived view - collect parent write nodes as dependencies
                for parent in logical_node.inputs:
                    if parent.view.name in view_to_write_node:
                        parent_write_nodes.append(view_to_write_node[parent.view.name])

                if parent_write_nodes:
                    derived_read_node = RayDerivedReadNode(
                        name=f"{view.name}:derived_read",
                        feature_view=view,
                        parent_dependencies=cast(List[DAGNode], parent_write_nodes),
                        config=self.config,
                        column_info=self.get_column_info(view),
                        is_materialization=self.is_materialization,
                    )
                    self.nodes.append(derived_read_node)

                    # Then build the rest of the processing chain (filter, aggregate, etc.)
                    processing_node = self._build(view, [derived_read_node])
                else:
                    # Parent not yet built - this shouldn't happen in topo order
                    raise ValueError(f"Parent views for {view.name} not yet built")
            else:
                # Regular view - build normal processing chain
                processing_node = self._build(view, None)

            # Create a write node for this view
            write_node = RayWriteNode(
                name=f"{view.name}:write",
                feature_view=view,
                inputs=[processing_node],
                config=self.config,
            )

            view_to_write_node[view.name] = write_node
            logger.info(f"Created write node for {view.name}")

        # Step 3: The final write node is the one for the top-level feature view
        final_node = view_to_write_node[self.feature_view.name]

        # Step 4: Topo sort the final DAG from the output node (Physical DAG)
        sorted_nodes = topological_sort(final_node)

        # Step 5: Update self.nodes to include all nodes for the execution plan
        self.nodes = sorted_nodes

        # Step 6: Return sorted execution plan
        return ExecutionPlan(sorted_nodes)

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

        # 6. Output
        last_node = self.build_output_nodes(self.feature_view, last_node)

        return ExecutionPlan(self.nodes)
