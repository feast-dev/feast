from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from feast import BatchFeatureView, FeatureView, StreamFeatureView
from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.algorithms.topo import topological_sort
from feast.infra.compute_engines.dag.context import ColumnInfo
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.feature_resolver import (
    FeatureResolver,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.utils import _get_column_names


class FeatureBuilder(ABC):
    """
    Translates a FeatureView definition and execution task into an execution DAG.
    This builder is engine-specific and returns an ExecutionPlan that ComputeEngine can run.
    """

    def __init__(
        self,
        registry: BaseRegistry,
        feature_view,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ):
        self.registry = registry
        self.feature_view = feature_view
        self.task = task
        self.nodes: List[DAGNode] = []
        self.feature_resolver = FeatureResolver()
        self.dag_root = self.feature_resolver.resolve(self.feature_view)

    @abstractmethod
    def build_source_node(self, view):
        raise NotImplementedError

    @abstractmethod
    def build_aggregation_node(self, view, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_join_node(self, view, input_nodes):
        raise NotImplementedError

    @abstractmethod
    def build_filter_node(self, view, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_dedup_node(self, view, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_transformation_node(self, view, input_nodes):
        raise NotImplementedError

    @abstractmethod
    def build_output_nodes(self, view, final_node):
        raise NotImplementedError

    @abstractmethod
    def build_validation_node(self, view, input_node):
        raise NotImplementedError

    def _should_aggregate(self, view):
        return bool(getattr(view, "aggregations", []))

    def _should_transform(self, view):
        return bool(getattr(view, "feature_transformation", None))

    def _should_validate(self, view):
        return getattr(view, "enable_validation", False)

    def _should_dedupe(self, view):
        return isinstance(self.task, HistoricalRetrievalTask) or self.task.only_latest

    def _build(self, view, input_nodes: Optional[List[DAGNode]]) -> DAGNode:
        # Step 1: build source node
        if view.data_source:
            last_node = self.build_source_node(view)

            if self._should_transform(view):
                # Transform applied to the source data
                last_node = self.build_transformation_node(view, [last_node])

        # If there are input nodes, transform or join them
        elif input_nodes:
            # User-defined transform handles the merging of input views
            if self._should_transform(view):
                last_node = self.build_transformation_node(view, input_nodes)
            # Default join
            else:
                last_node = self.build_join_node(view, input_nodes)
        else:
            raise ValueError(f"FeatureView {view.name} has no valid source or inputs")

        # Step 2: filter
        last_node = self.build_filter_node(view, last_node)

        # Step 3: aggregate or dedupe
        if self._should_aggregate(view):
            last_node = self.build_aggregation_node(view, last_node)
        elif self._should_dedupe(view):
            last_node = self.build_dedup_node(view, last_node)

        # Step 4: validate
        if self._should_validate(view):
            last_node = self.build_validation_node(view, last_node)

        return last_node

    def build(self) -> ExecutionPlan:
        # Step 1: Topo sort the FeatureViewNode DAG (Logical DAG)
        logical_nodes = self.feature_resolver.topological_sort(self.dag_root)

        # Step 2: For each FeatureView, build its corresponding execution DAGNode
        view_to_node: Dict[str, DAGNode] = {}

        for node in logical_nodes:
            view = node.view
            parent_dag_nodes = [
                view_to_node[parent.view.name]
                for parent in node.inputs
                if parent.view.name in view_to_node
            ]
            dag_node = self._build(view, parent_dag_nodes)
            view_to_node[view.name] = dag_node

        # Step 3: Build output node
        final_node = self.build_output_nodes(
            self.feature_view, view_to_node[self.feature_view.name]
        )

        # Step 4: Topo sort the final DAG from the output node (Physical DAG)
        sorted_nodes = topological_sort(final_node)

        # Step 5: Return sorted execution plan
        return ExecutionPlan(sorted_nodes)

    def get_column_info(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
    ) -> ColumnInfo:
        entities = []
        for entity_name in view.entities:
            entities.append(self.registry.get_entity(entity_name, self.task.project))

        join_keys, feature_cols, ts_col, created_ts_col = _get_column_names(
            view, entities
        )
        field_mapping = self.get_field_mapping(self.task.feature_view)

        # For feature views with transformations that need access to all source columns,
        # we need to read ALL source columns, not just the output feature columns.
        # This is specifically for transformations that create new columns or need raw data.
        mode = getattr(getattr(view, "feature_transformation", None), "mode", None)
        if mode == "ray" or getattr(mode, "value", None) == "ray":
            # Signal to read all columns by passing empty list for feature_cols
            # The transformation will produce the output columns defined in the schema
            feature_cols = []

        return ColumnInfo(
            join_keys=join_keys,
            feature_cols=feature_cols,
            ts_col=ts_col,
            created_ts_col=created_ts_col,
            field_mapping=field_mapping,
        )

    def get_field_mapping(
        self, feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
    ) -> Optional[dict]:
        """
        Get the field mapping for a feature view.
        Args:
            feature_view: The feature view to get the field mapping for.

        Returns:
            A dictionary mapping field names to column names.
        """
        if feature_view.stream_source:
            return feature_view.stream_source.field_mapping
        if feature_view.batch_source:
            return feature_view.batch_source.field_mapping
        return None
