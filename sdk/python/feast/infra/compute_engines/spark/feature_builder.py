from typing import Union

from pyspark.sql import SparkSession

from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.feature_builder import FeatureBuilder
from feast.infra.compute_engines.spark.nodes import (
    SparkAggregationNode,
    SparkDedupNode,
    SparkFilterNode,
    SparkJoinNode,
    SparkReadNode,
    SparkTransformationNode,
    SparkWriteNode,
)
from feast.infra.registry.base_registry import BaseRegistry


class SparkFeatureBuilder(FeatureBuilder):
    def __init__(
        self,
        registry: BaseRegistry,
        spark_session: SparkSession,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ):
        super().__init__(registry, task.feature_view, task)
        self.spark_session = spark_session

    def build_source_node(self, view):
        start_time = self.task.start_time
        end_time = self.task.end_time
        source = view.batch_source
        column_info = self.get_column_info(view)
        node = SparkReadNode(
            f"{view.name}:source",
            source,
            column_info,
            self.spark_session,
            start_time,
            end_time,
        )
        self.nodes.append(node)
        return node

    def build_aggregation_node(self, view, input_node):
        agg_specs = view.aggregations
        group_by_keys = view.entities
        timestamp_col = view.batch_source.timestamp_field
        node = SparkAggregationNode(
            f"{view.name}:agg",
            agg_specs,
            group_by_keys,
            timestamp_col,
            inputs=[input_node],
        )
        self.nodes.append(node)
        return node

    def build_join_node(self, view, input_nodes):
        column_info = self.get_column_info(view)
        node = SparkJoinNode(
            name=f"{view.name}:join",
            column_info=column_info,
            spark_session=self.spark_session,
            inputs=input_nodes,
            how="left",  # You can make this configurable later
        )
        self.nodes.append(node)
        return node

    def build_filter_node(self, view, input_node):
        filter_expr = getattr(view, "filter", None)
        ttl = getattr(view, "ttl", None)
        column_info = self.get_column_info(view)
        node = SparkFilterNode(
            f"{view.name}:filter",
            column_info,
            self.spark_session,
            ttl,
            filter_expr,
            inputs=[input_node],
        )
        self.nodes.append(node)
        return node

    def build_dedup_node(self, view, input_node):
        column_info = self.get_column_info(view)
        node = SparkDedupNode(
            f"{view.name}:dedup", column_info, self.spark_session, inputs=[input_node]
        )
        self.nodes.append(node)
        return node

    def build_transformation_node(self, view, input_nodes):
        udf_name = view.feature_transformation.name
        udf = view.feature_transformation.udf
        node = SparkTransformationNode(udf_name, udf, inputs=input_nodes)
        self.nodes.append(node)
        return node

    def build_output_nodes(self, view, input_node):
        node = SparkWriteNode(
            f"{view.name}:output", self.dag_root.view, inputs=[input_node]
        )
        self.nodes.append(node)
        return node

    def build_validation_node(self, view, input_node):
        pass
