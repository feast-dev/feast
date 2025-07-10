from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from feast.aggregation import Aggregation
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.spark.nodes import (
    SparkAggregationNode,
    SparkDedupNode,
    SparkJoinNode,
    SparkTransformationNode,
)
from tests.example_repos.example_feature_repo_with_bfvs import (
    driver,
)


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.appName("FeastSparkTests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_spark_transformation_node_executes_udf(spark_session):
    # Sample Spark input
    df = spark_session.createDataFrame(
        [
            {"name": "John  D.", "age": 30},
            {"name": "Alice  G.", "age": 25},
        ]
    )

    def strip_extra_spaces(df):
        from pyspark.sql.functions import col, regexp_replace

        return df.withColumn("name", regexp_replace(col("name"), "\\s+", " "))

    # Wrap DAGValue
    input_value = DAGValue(data=df, format=DAGFormat.SPARK)

    # Setup context
    context = ExecutionContext(
        project="test_proj",
        repo_config=MagicMock(),
        offline_store=MagicMock(),
        online_store=MagicMock(),
        entity_defs=MagicMock(),
        entity_df=None,
        node_outputs={"source": input_value},
    )

    # Prepare mock input node
    input_node = MagicMock()
    input_node.name = "source"

    # Create and run the node
    node = SparkTransformationNode(
        "transform", udf=strip_extra_spaces, inputs=[input_node]
    )
    result = node.execute(context)

    # Assert output
    out_df = result.data
    rows = out_df.orderBy("age").collect()
    assert rows[0]["name"] == "Alice G."
    assert rows[1]["name"] == "John D."


def test_spark_aggregation_node_executes_correctly(spark_session):
    # Sample input DataFrame
    input_df = spark_session.createDataFrame(
        [
            {"user_id": 1, "value": 10},
            {"user_id": 1, "value": 20},
            {"user_id": 2, "value": 5},
        ]
    )

    # Define Aggregation spec (e.g. COUNT on value)
    agg_specs = [Aggregation(column="value", function="count")]

    # Wrap as DAGValue
    input_value = DAGValue(data=input_df, format=DAGFormat.SPARK)

    # Setup context
    context = ExecutionContext(
        project="test_project",
        repo_config=MagicMock(),
        offline_store=MagicMock(),
        online_store=MagicMock(),
        entity_defs=[],
        entity_df=None,
        node_outputs={"source": input_value},
    )

    # Create and configure node
    node = SparkAggregationNode(
        name="agg",
        aggregations=agg_specs,
        group_by_keys=["user_id"],
        timestamp_col="",
    )
    node.add_input(MagicMock())
    node.inputs[0].name = "source"

    # Execute
    result = node.execute(context)
    result_df = result.data.orderBy("user_id").collect()

    # Validate output
    assert result.format == DAGFormat.SPARK
    assert result_df[0]["user_id"] == 1
    assert result_df[0]["count_value"] == 2
    assert result_df[1]["user_id"] == 2
    assert result_df[1]["count_value"] == 1


def test_spark_join_node_executes_point_in_time_join(spark_session):
    now = datetime.utcnow()

    # Entity DataFrame (point-in-time join targets)
    entity_df = spark_session.createDataFrame(
        [
            {"driver_id": 1001, "event_timestamp": now},
            {"driver_id": 1002, "event_timestamp": now},
        ]
    )

    # Feature DataFrame (raw features with timestamp)
    feature_df = spark_session.createDataFrame(
        [
            {
                "driver_id": 1001,
                "event_timestamp": now - timedelta(days=1),
                "created": now - timedelta(hours=2),
                "conv_rate": 0.8,
                "acc_rate": 0.95,
                "avg_daily_trips": 15,
            },
            {
                "driver_id": 1001,
                "event_timestamp": now - timedelta(days=2),
                "created": now - timedelta(hours=4),
                "conv_rate": 0.75,
                "acc_rate": 0.90,
                "avg_daily_trips": 14,
            },
            {
                "driver_id": 1002,
                "event_timestamp": now - timedelta(days=1),
                "created": now - timedelta(hours=3),
                "conv_rate": 0.7,
                "acc_rate": 0.88,
                "avg_daily_trips": 12,
            },
        ]
    )

    # Wrap as DAGValues
    feature_val = DAGValue(data=feature_df, format=DAGFormat.SPARK)

    # Set up context
    context = ExecutionContext(
        project="test_project",
        repo_config=MagicMock(),
        offline_store=MagicMock(),
        online_store=MagicMock(),
        entity_defs=[driver],
        entity_df=entity_df,
        node_outputs={
            "source": feature_val,
        },
    )

    # Prepare mock input node
    input_node = MagicMock()
    input_node.name = "source"

    # Create the node and add input
    join_node = SparkJoinNode(
        name="join",
        spark_session=spark_session,
        inputs=[input_node],
        column_info=ColumnInfo(
            join_keys=["driver_id"],
            feature_cols=["conv_rate", "acc_rate", "avg_daily_trips"],
            ts_col="event_timestamp",
            created_ts_col="created",
        ),
    )

    # Execute the node
    output = join_node.execute(context)
    context.node_outputs["join"] = output

    dedup_node = SparkDedupNode(
        name="dedup",
        spark_session=spark_session,
        column_info=ColumnInfo(
            join_keys=["driver_id"],
            feature_cols=[
                "source__conv_rate",
                "source__acc_rate",
                "source__avg_daily_trips",
            ],
            ts_col="source__event_timestamp",
            created_ts_col="source__created",
        ),
    )
    dedup_node.add_input(MagicMock())
    dedup_node.inputs[0].name = "join"
    dedup_output = dedup_node.execute(context)
    result_df = dedup_output.data.orderBy("driver_id").collect()

    # Assertions
    assert output.format == DAGFormat.SPARK
    assert len(result_df) == 2

    # Validate result for driver_id = 1001
    assert result_df[0]["driver_id"] == 1001
    assert abs(result_df[0]["source__conv_rate"] - 0.8) < 1e-6
    assert result_df[0]["source__avg_daily_trips"] == 15

    # Validate result for driver_id = 1002
    assert result_df[1]["driver_id"] == 1002
    assert abs(result_df[1]["source__conv_rate"] - 0.7) < 1e-6
    assert result_df[1]["source__avg_daily_trips"] == 12
