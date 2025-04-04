from feast.infra.compute_engines.dag.builder import DAGBuilder
from feast.infra.compute_engines.spark.node import (
    SparkAggregationNode,
    SparkJoinNode,
    SparkReadNode,
    SparkTransformationNode,
    SparkWriteNode,
)


class SparkDAGBuilder(DAGBuilder):
    def build_source_node(self):
        source_path = self.feature_view.source.path
        node = SparkReadNode("source", source_path)
        self.nodes.append(node)
        return node

    def build_aggregation_node(self, input_node):
        agg_specs = self.feature_view.aggregations
        node = SparkAggregationNode("agg", input_node, agg_specs)
        self.nodes.append(node)
        return node

    def build_join_node(self, input_node):
        join_keys = self.feature_view.entities
        node = SparkJoinNode("join", input_node, join_keys)
        self.nodes.append(node)
        return node

    def build_transformation_node(self, input_node):
        udf_name = self.feature_view.transformation.name
        udf = self.feature_view.transformation.udf
        node = SparkTransformationNode(udf_name, input_node, udf)
        self.nodes.append(node)
        return node

    def build_output_nodes(self, input_node):
        output_node = SparkWriteNode("output", input_node)
        self.nodes.append(output_node)

    def build_validation_node(self, input_node):
        pass
