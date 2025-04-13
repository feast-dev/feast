from abc import ABC, abstractmethod
from typing import Union

from feast import BatchFeatureView, FeatureView, StreamFeatureView
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.tasks import HistoricalRetrievalTask
from feast.infra.materialization.batch_materialization_engine import MaterializationTask


class FeatureBuilder(ABC):
    """
    Translates a FeatureView definition and execution task into an execution DAG.
    This builder is engine-specific and returns an ExecutionPlan that ComputeEngine can run.
    """

    def __init__(
        self,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ):
        self.feature_view = feature_view
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

    def build(self) -> ExecutionPlan:
        last_node = self.build_source_node()

        # PIT join entities to the feature data, and perform filtering
        last_node = self.build_join_node(last_node)
        last_node = self.build_filter_node(last_node)

        if (
            hasattr(self.feature_view, "aggregations")
            and self.feature_view.aggregations is not None
        ):
            last_node = self.build_aggregation_node(last_node)
        else:
            last_node = self.build_dedup_node(last_node)

        if (
            hasattr(self.feature_view, "feature_transformation")
            and self.feature_view.feature_transformation
        ):
            last_node = self.build_transformation_node(last_node)

        if getattr(self.feature_view, "enable_validation", False):
            last_node = self.build_validation_node(last_node)

        self.build_output_nodes(last_node)
        return ExecutionPlan(self.nodes)
