import logging
from typing import Union

from feast.aggregation import aggregation_specs_to_agg_ops
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
from feast.types import PrimitiveFeastType, from_feast_to_pyarrow_type

logger = logging.getLogger(__name__)


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
        agg_ops = aggregation_specs_to_agg_ops(
            agg_specs,
            time_window_unsupported_error_message=(
                "Time window aggregation is not supported in the local compute engine."
            ),
        )
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
        validation_config = getattr(view, "validation_config", None) or {}

        if not validation_config.get("columns") and hasattr(view, "features"):
            columns = {}
            json_columns = set()
            for feature in view.features:
                try:
                    columns[feature.name] = from_feast_to_pyarrow_type(feature.dtype)
                except (ValueError, KeyError):
                    logger.debug(
                        "Could not resolve PyArrow type for feature '%s' "
                        "(dtype=%s), skipping type check for this column.",
                        feature.name,
                        feature.dtype,
                    )
                    columns[feature.name] = None
                # Track which columns are Json type for content validation
                if (
                    isinstance(feature.dtype, PrimitiveFeastType)
                    and feature.dtype.name == "JSON"
                ):
                    json_columns.add(feature.name)
            if columns:
                validation_config = {**validation_config, "columns": columns}
            if json_columns:
                validation_config = {
                    **validation_config,
                    "json_columns": json_columns,
                }

        node = LocalValidationNode(
            "validate", validation_config, self.backend, inputs=[input_node]
        )
        self.nodes.append(node)
        return node

    def build_output_nodes(self, view, input_node):
        node = LocalOutputNode("output", self.dag_root.view, inputs=[input_node])
        self.nodes.append(node)
        return node
