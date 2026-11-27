from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyspark.sql.pandas.types import from_arrow_schema

from feast.aggregation import Aggregation
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.spark.nodes import (
    SparkAggregationNode,
    SparkDedupNode,
    SparkJoinNode,
    SparkReadNode,
    SparkTransformationNode,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkRetrievalJob,
)
from tests.example_repos.example_feature_repo_with_bfvs import (
    driver,
)


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("arrow_enabled", [False, True])
def test_spark_read_node_preserves_arrow_values_and_schema(
    spark_session, empty, arrow_enabled
):
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("count", pa.int32()),
            pa.field("score", pa.float32()),
            pa.field("name", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("event_timestamp", pa.timestamp("us", tz="UTC")),
        ]
    )
    rows = [
        {"id": 1, "count": 2, "score": 0.5, "name": "first", "active": True},
        {"id": 2, "count": None, "score": None, "name": None, "active": None},
        {"id": 3, "count": -1, "score": 1.5, "name": "last", "active": False},
    ]
    for row in rows:
        row["event_timestamp"] = datetime(
            2026, 1, 1, 12, 0, 0, 123456, tzinfo=timezone.utc
        )
    table = pa.concat_tables(
        [pa.Table.from_pylist(rows[:1], schema), pa.Table.from_pylist(rows[1:], schema)]
    )
    if empty:
        table = table.slice(0, 0)
        rows = []
    job = MagicMock()
    job.to_arrow.return_value = table
    node = SparkReadNode(
        "read",
        MagicMock(),
        ColumnInfo(["id"], ["count"], "event_timestamp", None),
        spark_session,
    )
    config = {
        "spark.sql.execution.arrow.pyspark.enabled": str(arrow_enabled).lower(),
        "spark.sql.session.timeZone": "UTC",
    }
    previous = {key: spark_session.conf.get(key) for key in config}
    for key, value in config.items():
        spark_session.conf.set(key, value)
    try:
        with patch(
            "feast.infra.compute_engines.spark.nodes.create_offline_store_retrieval_job",
            return_value=job,
        ):
            result = node.execute(MagicMock())
        assert result.format == DAGFormat.SPARK
        assert result.data.schema == from_arrow_schema(schema)
        actual_rows = result.data.orderBy("id").toArrow().to_pylist()
        assert actual_rows == rows
    finally:
        for key, value in previous.items():
            spark_session.conf.set(key, value)


def test_spark_read_node_keeps_native_spark_dataframe(spark_session):
    dataframe = spark_session.range(3)
    job = MagicMock(spec=SparkRetrievalJob)
    job.to_spark_df.return_value = dataframe
    node = SparkReadNode(
        "read",
        MagicMock(),
        ColumnInfo(["id"], [], "event_timestamp", None),
        spark_session,
    )
    with patch(
        "feast.infra.compute_engines.spark.nodes.create_offline_store_retrieval_job",
        return_value=job,
    ):
        result = node.execute(MagicMock())
    assert result.data is dataframe
    job.to_arrow.assert_not_called()


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


def test_spark_transformation_node_caches_resolved_udf():
    def identity(df):
        return df

    input_node = MagicMock()
    input_node.name = "source"
    node = SparkTransformationNode(
        "transform",
        udf=identity,
        inputs=[input_node],
        udf_string="",
    )
    first = node._resolve_udf()
    second = node._resolve_udf()
    assert first is second
    assert first is identity


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
        spark_session=spark_session,
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
