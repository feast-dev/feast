from __future__ import annotations

import logging
from typing import Any, Union

import pandas as pd

from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.feature_builder import FeatureBuilder
from feast.infra.compute_engines.flink.nodes import (
    FlinkAggregationNode,
    FlinkDedupNode,
    FlinkFilterNode,
    FlinkJoinNode,
    FlinkOutputNode,
    FlinkSourceReadNode,
    FlinkTransformationNode,
    FlinkValidationNode,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.types import PrimitiveFeastType, from_feast_to_pyarrow_type

logger = logging.getLogger(__name__)


class FlinkFeatureBuilder(FeatureBuilder):
    def __init__(
        self,
        registry: BaseRegistry,
        table_env: Any,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
        split_num: int,
    ) -> None:
        super().__init__(registry, task.feature_view, task)
        self.table_env = table_env
        self.split_num = split_num

    def _should_join_entity_df(self) -> bool:
        return isinstance(self.task, HistoricalRetrievalTask) and (
            isinstance(self.task.entity_df, pd.DataFrame)
            or (
                isinstance(self.task.entity_df, str)
                and bool(self.task.entity_df.strip())
            )
        )

    def _build(self, view: Any, input_nodes: list[DAGNode] | None) -> DAGNode:
        if view.data_source:
            last_node: DAGNode = self.build_source_node(view)

            if self._should_transform(view):
                last_node = self.build_transformation_node(view, [last_node])

            if self._should_join_entity_df():
                last_node = self.build_join_node(view, [last_node])

        elif input_nodes:
            if self._should_transform(view):
                last_node = self.build_transformation_node(view, input_nodes)
            else:
                last_node = self.build_join_node(view, input_nodes)
        else:
            raise ValueError(f"FeatureView {view.name} has no valid source or inputs")

        last_node = self.build_filter_node(view, last_node)

        if self._should_aggregate(view):
            last_node = self.build_aggregation_node(view, last_node)
        elif self._should_dedupe(view):
            last_node = self.build_dedup_node(view, last_node)

        if self._should_validate(view):
            last_node = self.build_validation_node(view, last_node)

        return last_node

    def build_source_node(self, view: Any) -> FlinkSourceReadNode:
        source = view.batch_source
        column_info = self.get_column_info(view)
        node = FlinkSourceReadNode(
            f"{view.name}:source",
            source,
            column_info,
            self.table_env,
            self.split_num,
            self.task.start_time,
            self.task.end_time,
        )
        self.nodes.append(node)
        return node

    def build_aggregation_node(
        self, view: Any, input_node: DAGNode
    ) -> FlinkAggregationNode:
        column_info = self.get_column_info(view)
        node = FlinkAggregationNode(
            f"{view.name}:agg",
            column_info.join_keys_columns,
            view.aggregations,
            self.table_env,
            self.split_num,
            inputs=[input_node],
        )
        self.nodes.append(node)
        return node

    def build_join_node(self, view: Any, input_nodes: list[DAGNode]) -> FlinkJoinNode:
        column_info = self.get_column_info(view)
        node = FlinkJoinNode(
            f"{view.name}:join",
            column_info,
            self.table_env,
            self.split_num,
            inputs=input_nodes,
        )
        self.nodes.append(node)
        return node

    def build_filter_node(self, view: Any, input_node: DAGNode) -> FlinkFilterNode:
        filter_expr = getattr(view, "filter", None)
        ttl = getattr(view, "ttl", None)
        column_info = self.get_column_info(view)
        node = FlinkFilterNode(
            f"{view.name}:filter",
            column_info,
            self.table_env,
            self.split_num,
            filter_expr,
            ttl,
            inputs=[input_node],
        )
        self.nodes.append(node)
        return node

    def build_dedup_node(self, view: Any, input_node: DAGNode) -> FlinkDedupNode:
        column_info = self.get_column_info(view)
        node = FlinkDedupNode(
            f"{view.name}:dedup",
            column_info,
            self.table_env,
            self.split_num,
            inputs=[input_node],
        )
        self.nodes.append(node)
        return node

    def build_transformation_node(
        self, view: Any, input_nodes: list[DAGNode]
    ) -> FlinkTransformationNode:
        transform_config = view.feature_transformation
        transformation_fn = (
            transform_config.udf
            if hasattr(transform_config, "udf")
            else transform_config
        )
        node = FlinkTransformationNode(
            f"{view.name}:transform",
            transformation_fn,
            self.table_env,
            self.split_num,
            inputs=input_nodes,
        )
        self.nodes.append(node)
        return node

    def build_output_nodes(self, view: Any, input_node: DAGNode) -> FlinkOutputNode:
        node = FlinkOutputNode(
            f"{view.name}:output",
            self.dag_root.view,
            self.table_env,
            self.split_num,
            isinstance(self.task, MaterializationTask),
            [input_node],
        )
        self.nodes.append(node)
        return node

    def build_validation_node(
        self, view: Any, input_node: DAGNode
    ) -> FlinkValidationNode:
        expected_columns = {}
        json_columns: set[str] = set()
        if hasattr(view, "features"):
            for feature in view.features:
                try:
                    expected_columns[feature.name] = from_feast_to_pyarrow_type(
                        feature.dtype
                    )
                except (ValueError, KeyError):
                    logger.debug(
                        "Could not resolve PyArrow type for feature '%s' "
                        "(dtype=%s), skipping type check for this column.",
                        feature.name,
                        feature.dtype,
                    )
                    expected_columns[feature.name] = None
                if (
                    isinstance(feature.dtype, PrimitiveFeastType)
                    and feature.dtype.name == "JSON"
                ):
                    json_columns.add(feature.name)

        node = FlinkValidationNode(
            f"{view.name}:validate",
            expected_columns,
            json_columns,
            self.table_env,
            self.split_num,
            inputs=[input_node],
        )
        self.nodes.append(node)
        return node
