from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List, Optional, Union

import pandas as pd

from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.feature_builder import FeatureBuilder
from feast.infra.compute_engines.trino.nodes import (
    TrinoAggregationNode,
    TrinoDedupNode,
    TrinoFilterNode,
    TrinoJoinNode,
    TrinoReadNode,
    TrinoTransformationNode,
    TrinoValidationNode,
    TrinoWriteNode,
)
from feast.infra.compute_engines.trino.utils import from_feast_to_trino_type
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.registry.base_registry import BaseRegistry

if TYPE_CHECKING:
    from feast import BatchFeatureView, FeatureView, StreamFeatureView

logger = logging.getLogger(__name__)


class TrinoFeatureBuilder(FeatureBuilder):
    """Translates FeatureView definitions and tasks into a pure Trino SQL DAG ExecutionPlan."""

    def __init__(
        self,
        registry: BaseRegistry,
        client: Trino,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ) -> None:
        super().__init__(registry, task.feature_view, task)
        self.client = client

    def _should_join_entity_df(self) -> bool:
        return isinstance(self.task, HistoricalRetrievalTask) and (
            isinstance(self.task.entity_df, pd.DataFrame)
            or (
                isinstance(self.task.entity_df, str)
                and bool(self.task.entity_df.strip())
            )
        )

    def _build(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView, Any],
        input_nodes: Optional[List[DAGNode]],
    ) -> DAGNode:
        if getattr(view, "batch_source", None) or getattr(view, "data_source", None):
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

    def build_source_node(
        self, view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
    ) -> TrinoReadNode:
        start_time = getattr(self.task, "start_time", None)
        end_time = getattr(self.task, "end_time", None)
        source = view.batch_source
        if source is None:
            raise ValueError(f"FeatureView {view.name} has no batch_source defined")
        column_info = self.get_column_info(view)
        return TrinoReadNode(
            name=f"{view.name}:source",
            source=source,
            column_info=column_info,
            client=self.client,
            start_time=start_time,
            end_time=end_time,
        )

    def build_aggregation_node(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        input_node: DAGNode,
    ) -> TrinoAggregationNode:
        agg_specs = getattr(view, "aggregations", None) or []
        group_by_keys = getattr(view, "entities", []) or []
        batch_source = view.batch_source
        timestamp_col = (
            batch_source.timestamp_field
            if batch_source and hasattr(batch_source, "timestamp_field")
            else None
        )
        if not timestamp_col:
            raise ValueError(
                f"FeatureView {view.name} missing timestamp_field for aggregation"
            )

        enable_tiling = getattr(view, "enable_tiling", False)
        hop_size = getattr(view, "tiling_hop_size", None)

        return TrinoAggregationNode(
            name=f"{view.name}:agg",
            aggregations=agg_specs,
            group_by_keys=group_by_keys,
            timestamp_col=timestamp_col,
            client=self.client,
            inputs=[input_node],
            enable_tiling=enable_tiling,
            hop_size=hop_size,
        )

    def build_join_node(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        input_nodes: List[DAGNode],
    ) -> TrinoJoinNode:
        column_info = self.get_column_info(view)
        return TrinoJoinNode(
            name=f"{view.name}:join",
            column_info=column_info,
            client=self.client,
            inputs=input_nodes,
            how="left",
        )

    def build_filter_node(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        input_node: DAGNode,
    ) -> TrinoFilterNode:
        """Build filter node applying TTL and feature view filter expression.

        Note:
            ``view.filter`` is rendered directly into the Trino SQL WHERE clause and
            must be trusted developer-authored input.
        """
        filter_expr = getattr(view, "filter", None)
        ttl = getattr(view, "ttl", None)
        column_info = self.get_column_info(view)
        return TrinoFilterNode(
            name=f"{view.name}:filter",
            column_info=column_info,
            client=self.client,
            ttl=ttl,
            filter_condition=filter_expr,
            inputs=[input_node],
        )

    def build_dedup_node(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        input_node: DAGNode,
    ) -> TrinoDedupNode:
        column_info = self.get_column_info(view)
        return TrinoDedupNode(
            name=f"{view.name}:dedup",
            column_info=column_info,
            client=self.client,
            inputs=[input_node],
        )

    def build_transformation_node(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        input_nodes: List[DAGNode],
    ) -> TrinoTransformationNode:
        feature_transformation = getattr(view, "feature_transformation", None)
        if feature_transformation is None:
            raise ValueError(
                f"FeatureView {view.name} has no feature_transformation defined"
            )
        udf_name = feature_transformation.name
        udf = feature_transformation.udf
        udf_string = getattr(feature_transformation, "udf_string", "") or ""
        return TrinoTransformationNode(
            name=udf_name,
            udf=udf,
            inputs=input_nodes,
            udf_string=udf_string,
        )

    def build_output_nodes(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        final_node: DAGNode,
    ) -> TrinoWriteNode:
        return TrinoWriteNode(
            name=f"{view.name}:output",
            feature_view=self.dag_root.view,
            client=self.client,
            inputs=[final_node],
        )

    def build_validation_node(
        self,
        view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        input_node: DAGNode,
    ) -> TrinoValidationNode:
        features = getattr(view, "features", []) or []
        expected_columns = {
            feature.name: from_feast_to_trino_type(feature.dtype)
            for feature in features
        }
        json_columns = {
            feature.name
            for feature in features
            if getattr(feature.dtype, "name", None) == "JSON"
        }

        return TrinoValidationNode(
            name=f"{view.name}:validate",
            expected_columns=expected_columns,
            json_columns=json_columns,
            inputs=[input_node],
        )
