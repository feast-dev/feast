from typing import Union

from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.backends.base import DataFrameBackend
from feast.infra.compute_engines.feature_builder import FeatureBuilder
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
from feast.infra.registry.base_registry import BaseRegistry


class LocalFeatureBuilder(FeatureBuilder):
    def __init__(
        self,
        registry: BaseRegistry,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
        backend: DataFrameBackend,
    ):
        super().__init__(registry, task.feature_view, task)
        self.backend = backend

    def build_source_node(self, view):
        start_time = self.task.start_time
        end_time = self.task.end_time
        column_info = self.get_column_info(view)
        source = view.source
        node = LocalSourceReadNode("source", source, column_info, start_time, end_time)
        self.nodes.append(node)
        return node

    def build_join_node(self, view, input_nodes):
        column_info = self.get_column_info(view)
        node = LocalJoinNode("join", column_info, self.backend, inputs=input_nodes)
        self.nodes.append(node)
        return node

    def build_filter_node(self, view, input_node):
        filter_expr = getattr(view, "filter", None)
        ttl = getattr(view, "ttl", None)
        column_info = self.get_column_info(view)
        node = LocalFilterNode(
            "filter", column_info, self.backend, filter_expr, ttl, inputs=[input_node]
        )
        self.nodes.append(node)
        return node

    def build_aggregation_node(self, view, input_node):
        agg_specs = view.aggregations
        agg_ops = self._get_aggregate_operations(agg_specs)
        group_by_keys = view.entities
        node = LocalAggregationNode(
            "agg", self.backend, group_by_keys, agg_ops, inputs=[input_node]
        )
        self.nodes.append(node)
        return node

    def build_dedup_node(self, view, input_node):
        column_info = self.get_column_info(view)
        node = LocalDedupNode("dedup", column_info, self.backend, inputs=[input_node])
        self.nodes.append(node)
        return node

    def build_transformation_node(self, view, input_nodes):
        transform_config = view.feature_transformation
        transformation_fn = (
            transform_config.udf
            if hasattr(transform_config, "udf")
            else transform_config
        )
        node = LocalTransformationNode(
            "transform", transformation_fn, self.backend, inputs=input_nodes
        )
        self.nodes.append(node)
        return node

    def build_validation_node(self, view, input_node):
        validation_config = view.validation_config
        node = LocalValidationNode(
            "validate", validation_config, self.backend, inputs=[input_node]
        )
        self.nodes.append(node)
        return node

    def build_output_nodes(self, view, input_node):
        node = LocalOutputNode("output", self.dag_root.view, inputs=[input_node])
        self.nodes.append(node)
        return node

    @staticmethod
    def _get_aggregate_operations(agg_specs):
        agg_ops = {}
        for agg in agg_specs:
            if agg.time_window is not None:
                raise ValueError(
                    "Time window aggregation is not supported in the local compute engine."
                )
            alias = f"{agg.function}_{agg.column}"
            agg_ops[alias] = (agg.function, agg.column)
        return agg_ops
