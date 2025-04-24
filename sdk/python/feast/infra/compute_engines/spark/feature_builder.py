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


class SparkFeatureBuilder(FeatureBuilder):
    def __init__(
        self,
        spark_session: SparkSession,
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ):
        super().__init__(task)
        self.spark_session = spark_session

    def build_source_node(self):
        source = self.feature_view.batch_source
        start_time = self.task.start_time
        end_time = self.task.end_time
        node = SparkReadNode("source", source, start_time, end_time)
        self.nodes.append(node)
        return node

    def build_aggregation_node(self, input_node):
        agg_specs = self.feature_view.aggregations
        group_by_keys = self.feature_view.entities
        timestamp_col = self.feature_view.batch_source.timestamp_field
        node = SparkAggregationNode("agg", agg_specs, group_by_keys, timestamp_col)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_join_node(self, input_node):
        node = SparkJoinNode("join", self.spark_session)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_filter_node(self, input_node):
        filter_expr = None
        if hasattr(self.feature_view, "filter"):
            filter_expr = self.feature_view.filter
        ttl = self.feature_view.ttl
        node = SparkFilterNode("filter", self.spark_session, ttl, filter_expr)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_dedup_node(self, input_node):
        node = SparkDedupNode("dedup", self.spark_session)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_transformation_node(self, input_node):
        udf_name = self.feature_view.feature_transformation.name
        udf = self.feature_view.feature_transformation.udf
        node = SparkTransformationNode(udf_name, udf)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_output_nodes(self, input_node):
        node = SparkWriteNode("output", self.feature_view)
        node.add_input(input_node)
        self.nodes.append(node)
        return node

    def build_validation_node(self, input_node):
        pass
