from typing import Union

from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.feature_builder import FeatureBuilder
from feast.infra.compute_engines.local.backends.base import DataFrameBackend
from feast.infra.compute_engines.local.nodes import (
    LocalAggregationNode,
    LocalDedupNode,
    LocalFilterNode,
    LocalJoinNode,
    LocalOutputNode,
    LocalSourceReadNode,
    LocalTransformationNode,
    LocalValidationNode,
)


class LocalFeatureBuilder(FeatureBuilder):
    def __init__(
        self,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
        backend: DataFrameBackend,
    ):
        super().__init__(task)
        self.backend = backend

    def build_source_node(self):
        node = LocalSourceReadNode("source", self.feature_view, self.task)
        self.nodes.append(node)
        return node

    def build_join_node(self, input_node):
        node = LocalJoinNode("join", self.backend)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_filter_node(self, input_node):
        filter_expr = None
        if hasattr(self.feature_view, "filter"):
            filter_expr = self.feature_view.filter
        ttl = self.feature_view.ttl
        node = LocalFilterNode("filter", self.backend, filter_expr, ttl)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_aggregation_node(self, input_node):
        agg_specs = self.feature_view.aggregations
        agg_ops = {}
        for agg in agg_specs:
            if agg.time_window is not None:
                raise ValueError(
                    "Time window aggregation is not supported in local compute engine. Please use a different compute engine."
                )
            alias = f"{agg.function}_{agg.column}"
            agg_ops[alias] = (agg.function, agg.column)
        group_by_keys = self.feature_view.entities
        node = LocalAggregationNode("agg", self.backend, group_by_keys, agg_ops)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_dedup_node(self, input_node):
        node = LocalDedupNode("dedup", self.backend)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_transformation_node(self, input_node):
        node = LocalTransformationNode(
            "transform", self.feature_view.feature_transformation, self.backend
        )
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_validation_node(self, input_node):
        node = LocalValidationNode(
            "validate", self.feature_view.validation_config, self.backend
        )
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_output_nodes(self, input_node):
        node = LocalOutputNode("output")
        node.add_input(input_node)
        self.nodes.append(node)

    def build(self) -> ExecutionPlan:
        last_node = self.build_source_node()

        if isinstance(self.task, HistoricalRetrievalTask):
            last_node = self.build_join_node(last_node)

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
