from abc import ABC, abstractmethod
from typing import Union

from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan


class FeatureBuilder(ABC):
    """
    Translates a FeatureView definition and execution task into an execution DAG.
    This builder is engine-specific and returns an ExecutionPlan that ComputeEngine can run.
    """

    def __init__(
        self,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ):
        self.feature_view = task.feature_view
        self.task = task
        self.nodes: list[DAGNode] = []

    @abstractmethod
    def build_source_node(self):
        raise NotImplementedError

    @abstractmethod
    def build_aggregation_node(self, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_join_node(self, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_filter_node(self, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_dedup_node(self, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_transformation_node(self, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_output_nodes(self, input_node):
        raise NotImplementedError

    @abstractmethod
    def build_validation_node(self, input_node):
        raise

    def _should_aggregate(self):
        return (
            hasattr(self.feature_view, "aggregations")
            and self.feature_view.aggregations is not None
            and len(self.feature_view.aggregations) > 0
        )

    def _should_transform(self):
        return (
            hasattr(self.feature_view, "feature_transformation")
            and self.feature_view.feature_transformation
        )

    def _should_validate(self):
        return getattr(self.feature_view, "enable_validation", False)

    def build(self) -> ExecutionPlan:
        last_node = self.build_source_node()

        # Join entity_df with source if needed
        last_node = self.build_join_node(last_node)

        # PIT filter, TTL, and user-defined filter
        last_node = self.build_filter_node(last_node)

        if self._should_aggregate():
            last_node = self.build_aggregation_node(last_node)
        elif isinstance(self.task, HistoricalRetrievalTask):
            last_node = self.build_dedup_node(last_node)

        if self._should_transform():
            last_node = self.build_transformation_node(last_node)

        if self._should_validate():
            last_node = self.build_validation_node(last_node)

        self.build_output_nodes(last_node)
        return ExecutionPlan(self.nodes)
