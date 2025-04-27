from datetime import timedelta
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa

from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue
from feast.infra.compute_engines.local.backends.pandas_backend import PandasBackend
from feast.infra.compute_engines.local.nodes import (
    LocalAggregationNode,
    LocalDedupNode,
    LocalFilterNode,
    LocalJoinNode,
    LocalOutputNode,
    LocalTransformationNode,
)

backend = PandasBackend()
now = pd.Timestamp.utcnow()

sample_df = pd.DataFrame(
    {
        "entity_id": [1, 1, 2, 2],
        "value": [10, 20, 30, 40],
        "event_timestamp": [
            now,
            now - timedelta(minutes=1),
            now,
            now - timedelta(minutes=5),
        ],
    }
)

entity_df = pd.DataFrame({"entity_id": [1, 2], "event_timestamp": [now, now]})


def create_context(node_outputs):
    # Setup execution context
    return ExecutionContext(
        project="test_proj",
        repo_config=MagicMock(),
        offline_store=MagicMock(),
        online_store=MagicMock(),
        entity_defs=MagicMock(),
        entity_df=entity_df,
        node_outputs=node_outputs,
        column_info=ColumnInfo(
            join_keys=["entity_id"],
            feature_cols=["value"],
            ts_col="event_timestamp",
            created_ts_col=None,
        ),
    )


def test_local_filter_node():
    context = create_context(
        node_outputs={"source": ArrowTableValue(pa.Table.from_pandas(sample_df))}
    )

    # Create filter node and connect input
    filter_node = LocalFilterNode(
        name="filter",
        backend=backend,
        filter_expr="value > 15",
    )
    filter_node.add_input(MagicMock())
    filter_node.inputs[0].name = "source"

    # Execute and validate
    result = filter_node.execute(context)
    assert isinstance(result, ArrowTableValue)
    assert result.data.num_rows == 3


def test_local_aggregation_node():
    context = create_context(
        node_outputs={"source": ArrowTableValue(pa.Table.from_pandas(sample_df))}
    )

    # Create aggregation node and connect input
    agg_ops = {
        "sum_value": ("sum", "value"),
    }
    agg_node = LocalAggregationNode(
        name="agg",
        backend=backend,
        group_keys=["entity_id"],
        agg_ops=agg_ops,
    )
    agg_node.add_input(MagicMock())
    agg_node.inputs[0].name = "source"

    # Execute and validate
    result = agg_node.execute(context)
    assert isinstance(result, ArrowTableValue)
    assert result.data.num_rows == 2
    result_df = result.data.to_pandas()
    assert result_df["sum_value"].iloc[0] == 30
    assert result_df["sum_value"].iloc[1] == 70


def test_local_join_node():
    context = create_context(
        node_outputs={"source": ArrowTableValue(pa.Table.from_pandas(sample_df))}
    )

    # Create join node and connect input
    join_node = LocalJoinNode(
        name="join",
        backend=backend,
    )
    join_node.add_input(MagicMock())
    join_node.inputs[0].name = "source"

    # Execute and validate
    result = join_node.execute(context)
    assert isinstance(result, ArrowTableValue)
    assert result.data.num_rows == 4
    result_df = result.data.to_pandas()
    assert all(result_df["entity_id"].isin([1, 2]))
    assert "__entity_event_timestamp" in result_df.columns


def test_local_dedup_node():
    # Duplicate rows for each entity with different event and created timestamps
    df = pd.DataFrame(
        {
            "entity_id": [1, 1, 2, 2],
            "value": [100, 200, 300, 400],
            "event_timestamp": [
                now - timedelta(seconds=1),
                now,
                now - timedelta(seconds=1),
                now,
            ],
            "created_ts": [
                now - timedelta(seconds=1),
                now,
                now,
                now - timedelta(seconds=2),
            ],
            "__entity_event_timestamp": [
                now,
                now,
                now - timedelta(seconds=1),
                now - timedelta(seconds=1),
            ],
        }
    )

    # Register DataFrame in context
    table = pa.Table.from_pandas(df)
    context = create_context(node_outputs={"source": ArrowTableValue(table)})
    context.entity_timestamp_col = "event_timestamp"

    # Build node
    node = LocalDedupNode(name="dedup", backend=backend)
    node.add_input(MagicMock())
    node.inputs[0].name = "source"

    result = node.execute(context)

    # Validate: only latest row per entity remains
    df_result = result.data.to_pandas()
    assert df_result.shape[0] == 2
    assert set(df_result["entity_id"]) == {1, 2}


def test_local_transformation_node():
    context = create_context(
        node_outputs={"source": ArrowTableValue(pa.Table.from_pandas(sample_df))}
    )

    # Create transformation node and connect input
    transform_node = LocalTransformationNode(
        name="transform",
        backend=backend,
        transformation_fn=lambda df: df.assign(value=df["value"] * 2),
    )
    transform_node.add_input(MagicMock())
    transform_node.inputs[0].name = "source"

    # Execute and validate
    result = transform_node.execute(context)
    assert isinstance(result, ArrowTableValue)
    assert result.data.num_rows == 4
    result_df = result.data.to_pandas()
    assert all(result_df["value"] == sample_df["value"] * 2)


def test_local_output_node():
    context = create_context(
        node_outputs={"source": ArrowTableValue(pa.Table.from_pandas(sample_df))}
    )
    node = LocalOutputNode("output", MagicMock())
    node.add_input(MagicMock())
    node.inputs[0].name = "source"
    result = node.execute(context)
    assert result.num_rows == 4
