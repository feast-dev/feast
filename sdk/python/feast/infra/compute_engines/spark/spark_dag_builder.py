from typing import Union

from pyspark.sql import SparkSession

from feast import BatchFeatureView, FeatureView, StreamFeatureView
from feast.infra.compute_engines.base import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.builder import DAGBuilder
from feast.infra.compute_engines.spark.node import (
    SparkAggregationNode,
    SparkHistoricalRetrievalReadNode,
    SparkJoinNode,
    SparkMaterializationReadNode,
    SparkTransformationNode,
    SparkWriteNode,
)
from feast.infra.materialization.batch_materialization_engine import MaterializationTask


class SparkDAGBuilder(DAGBuilder):
    def __init__(
        self,
        spark_session: SparkSession,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        task: Union[MaterializationTask, HistoricalRetrievalTask],
    ):
        super().__init__(feature_view, task)
        self.spark_session = spark_session

    def build_source_node(self):
        source_path = self.feature_view.source.path
        if isinstance(self.task, MaterializationTask):
            node = SparkMaterializationReadNode("source", source_path)
        else:
            node = SparkHistoricalRetrievalReadNode(
                "source", source_path, self.spark_session
            )
        self.nodes.append(node)
        return node

    def build_aggregation_node(self, input_node):
        agg_specs = self.feature_view.aggregations
        group_by_keys = self.feature_view.entities
        timestamp_col = self.feature_view.batch_source.timestamp_field
        node = SparkAggregationNode(
            "agg", input_node, agg_specs, group_by_keys, timestamp_col
        )
        self.nodes.append(node)
        return node

    def build_join_node(self, input_node):
        join_keys = self.feature_view.entities
        node = SparkJoinNode(
            "join", input_node, join_keys, self.feature_view, self.spark_session
        )
        self.nodes.append(node)
        return node

    def build_transformation_node(self, input_node):
        udf_name = self.feature_view.feature_transformation.name
        udf = self.feature_view.feature_transformation.udf
        node = SparkTransformationNode(udf_name, input_node, udf)
        self.nodes.append(node)
        return node

    def build_output_nodes(self, input_node):
        output_node = SparkWriteNode("output", input_node, self.feature_view)
        self.nodes.append(output_node)

    def build_validation_node(self, input_node):
        pass
