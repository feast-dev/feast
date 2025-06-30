from abc import ABC, abstractmethod
from typing import Union, List, Optional

from feast import BatchFeatureView, StreamFeatureView, FeatureView
from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.feature_resolver import FeatureResolver, FeatureViewNode
from feast.infra.compute_engines.dag.context import ColumnInfo
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
        self.task = task
        self.nodes: List[DAGNode] = []
        self.resolver = FeatureResolver()
        self.dag_root = self.resolver.resolve(feature_view)
        self.sorted_nodes = self.resolver.topo_sort(self.dag_root)

    @abstractmethod
    def build_source_node(self, view):
        raise NotImplementedError

    @abstractmethod
    def build_aggregation_node(self,
                               view,
                               input_node):
        raise NotImplementedError

    @abstractmethod
    def build_join_node(self,
                        view,
                        input_node):
        raise NotImplementedError

    @abstractmethod
    def build_filter_node(self,
                          view,
                          input_node):
        raise NotImplementedError

    @abstractmethod
    def build_dedup_node(self,
                         view,
                         input_node):
        raise NotImplementedError

    @abstractmethod
    def build_transformation_node(self,
                                  view,
                                  input_node):
        raise NotImplementedError

    @abstractmethod
    def build_output_nodes(self,
                           final_node):
        raise NotImplementedError

    @abstractmethod
    def build_validation_node(self,
                              view,
                              input_node):
        raise NotImplementedError

    def _should_aggregate(self,
                          view):
        return bool(getattr(view, "aggregations", []))

    def _should_transform(self,
                          view):
        return bool(getattr(view, "feature_transformation", None))

    def _should_validate(self,
                         view):
        return getattr(view, "enable_validation", False)

    def _should_dedupe(self,
                       view):
        return isinstance(self.task, HistoricalRetrievalTask) or self.task.only_latest

    def _build(self,
               current_node: FeatureViewNode) -> DAGNode:
        current_view = current_node.view

        # Step 1: build source or parent join
        if current_node.parent:
            parent_node = self._build(current_node.parent)
            last_node = self.build_join_node(current_view, parent_node)
        else:
            last_node = self.build_source_node(current_view)

        # Step 2: filter
        last_node = self.build_filter_node(current_view, last_node)

        # Step 3: aggregate or dedupe
        if self._should_aggregate(current_view):
            last_node = self.build_aggregation_node(current_view, last_node)
        elif self._should_dedupe(current_view):
            last_node = self.build_dedup_node(current_view, last_node)

        # Step 4: transform
        if self._should_transform(current_view):
            last_node = self.build_transformation_node(current_view, last_node)

        # Step 5: validate
        if self._should_validate(current_view):
            last_node = self.build_validation_node(current_view, last_node)

        return last_node

    def build(self) -> ExecutionPlan:
        final_node = self._build(self.dag_root)
        self.build_output_nodes(final_node)
        return ExecutionPlan(self.nodes)

    def get_column_info(
            self,
            view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
    ) -> ColumnInfo:
        entities = []
        for entity_name in view.entities:
            entities.append(self.registry.get_entity(entity_name, self.task.project))

        if view.source_view:
            # If the view has a source_view, the column information come from the tags dict
            # unpack to get those values
            join_keys = view.source_view.tags.get("join_keys", [])
            feature_cols = view.source_view.tags.get("feature_cols", [])
            ts_col = view.source_view.tags.get("ts_col", None)
            created_ts_col = view.source_view.tags.get("created_ts_col", None)
        else:
            join_keys, feature_cols, ts_col, created_ts_col = _get_column_names(
                view, entities
            )
        field_mapping = self.get_field_mapping(self.task.feature_view)

        return ColumnInfo(
            join_keys=join_keys,
            feature_cols=feature_cols,
            ts_col=ts_col,
            created_ts_col=created_ts_col,
            field_mapping=field_mapping,
        )

    def get_field_mapping(
            self,
            feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
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
