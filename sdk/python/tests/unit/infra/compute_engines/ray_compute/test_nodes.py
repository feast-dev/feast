from datetime import datetime, timedelta

import pandas as pd
import pytest
import ray

from feast.aggregation import Aggregation
from feast.infra.compute_engines.dag.context import ColumnInfo
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.nodes import (
    RayAggregationNode,
    RayDedupNode,
    RayFilterNode,
    RayJoinNode,
    RayReadNode,
    RayTransformationNode,
)


class DummyInputNode(DAGNode):
    def __init__(self, name, output):
        super().__init__(name)
        self._output = output

    def execute(self, context):
        return self._output


class DummyFeatureView:
    name = "dummy"
    online = False
    offline = False


class DummySource:
    pass


class DummyRetrievalJob:
    def __init__(self, ray_dataset):
        self._ray_dataset = ray_dataset

    def to_ray_dataset(self):
        return self._ray_dataset


@pytest.fixture(scope="session")
def ray_session():
    """Initialize Ray session for testing."""
    if not ray.is_initialized():
        ray.init(num_cpus=2, ignore_reinit_error=True, include_dashboard=False)
    yield ray
    ray.shutdown()


@pytest.fixture
def ray_config():
    """Create Ray compute engine configuration for testing."""
    return RayComputeEngineConfig(
        type="ray.engine",
        max_workers=2,
        enable_optimization=True,
        broadcast_join_threshold_mb=50,
        target_partition_size_mb=32,
    )


@pytest.fixture
def mock_context():
    class DummyOfflineStore:
        def offline_write_batch(self, *args, **kwargs):
            pass

    class DummyContext:
        def __init__(self):
            self.registry = None
            self.store = None
            self.project = "test_project"
            self.entity_data = None
            self.config = None
            self.node_outputs = {}
            self.offline_store = DummyOfflineStore()

    return DummyContext()


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame(
        [
            {
                "driver_id": 1001,
                "event_timestamp": datetime.now() - timedelta(hours=1),
                "created": datetime.now() - timedelta(hours=2),
                "conv_rate": 0.8,
                "acc_rate": 0.5,
                "avg_daily_trips": 15,
            },
            {
                "driver_id": 1002,
                "event_timestamp": datetime.now() - timedelta(hours=2),
                "created": datetime.now() - timedelta(hours=3),
                "conv_rate": 0.7,
                "acc_rate": 0.4,
                "avg_daily_trips": 12,
            },
            {
                "driver_id": 1001,
                "event_timestamp": datetime.now() - timedelta(hours=3),
                "created": datetime.now() - timedelta(hours=4),
                "conv_rate": 0.75,
                "acc_rate": 0.9,
                "avg_daily_trips": 14,
            },
        ]
    )


@pytest.fixture
def column_info():
    """Create a sample ColumnInfo for testing Ray nodes."""
    return ColumnInfo(
        join_keys=["driver_id"],
        feature_cols=["conv_rate", "acc_rate", "avg_daily_trips"],
        ts_col="event_timestamp",
        created_ts_col="created",
        field_mapping=None,
    )


def test_ray_read_node(ray_session, ray_config, mock_context, sample_data, column_info):
    """Test RayReadNode functionality."""
    ray_dataset = ray.data.from_pandas(sample_data)
    mock_source = DummySource()
    node = RayReadNode(
        name="read",
        source=mock_source,
        column_info=column_info,
        config=ray_config,
    )
    mock_context.registry = None
    mock_context.store = None
    mock_context.offline_store = None
    mock_retrieval_job = DummyRetrievalJob(ray_dataset)
    import feast.infra.compute_engines.ray.nodes as ray_nodes

    ray_nodes.create_offline_store_retrieval_job = lambda **kwargs: mock_retrieval_job
    result = node.execute(mock_context)
    assert isinstance(result, DAGValue)
    assert result.format == DAGFormat.RAY
    result_df = result.data.to_pandas()
    assert len(result_df) == 3
    assert "driver_id" in result_df.columns
    assert "conv_rate" in result_df.columns


def test_ray_aggregation_node(
    ray_session, ray_config, mock_context, sample_data, column_info
):
    """Test RayAggregationNode functionality."""
    ray_dataset = ray.data.from_pandas(sample_data)
    input_value = DAGValue(data=ray_dataset, format=DAGFormat.RAY)
    dummy_node = DummyInputNode("input_node", input_value)
    node = RayAggregationNode(
        name="aggregation",
        aggregations=[
            Aggregation(column="conv_rate", function="sum"),
            Aggregation(column="acc_rate", function="avg"),
        ],
        group_by_keys=["driver_id"],
        timestamp_col="event_timestamp",
        config=ray_config,
    )
    node.add_input(dummy_node)
    mock_context.node_outputs = {"input_node": input_value}
    result = node.execute(mock_context)
    assert isinstance(result, DAGValue)
    assert result.format == DAGFormat.RAY
    result_df = result.data.to_pandas()
    assert len(result_df) == 2
    assert "driver_id" in result_df.columns
    assert "sum_conv_rate" in result_df.columns
    assert "avg_acc_rate" in result_df.columns


def test_ray_join_node(ray_session, ray_config, mock_context, sample_data, column_info):
    """Test RayJoinNode functionality."""
    entity_data = pd.DataFrame(
        [
            {"driver_id": 1001, "event_timestamp": datetime.now()},
            {"driver_id": 1002, "event_timestamp": datetime.now()},
        ]
    )
    feature_dataset = ray.data.from_pandas(sample_data)
    feature_value = DAGValue(data=feature_dataset, format=DAGFormat.RAY)
    dummy_node = DummyInputNode("feature_node", feature_value)
    node = RayJoinNode(
        name="join",
        column_info=column_info,
        config=ray_config,
    )
    node.add_input(dummy_node)
    mock_context.node_outputs = {"feature_node": feature_value}
    mock_context.entity_df = entity_data
    result = node.execute(mock_context)
    assert isinstance(result, DAGValue)
    assert result.format == DAGFormat.RAY
    result_df = result.data.to_pandas()
    assert len(result_df) >= 2
    assert "driver_id" in result_df.columns


def test_ray_transformation_node(
    ray_session, ray_config, mock_context, sample_data, column_info
):
    """Test RayTransformationNode functionality."""
    ray_dataset = ray.data.from_pandas(sample_data)

    def transform_feature(df: pd.DataFrame) -> pd.DataFrame:
        df["conv_rate_doubled"] = df["conv_rate"] * 2
        return df

    input_value = DAGValue(data=ray_dataset, format=DAGFormat.RAY)
    dummy_node = DummyInputNode("input_node", input_value)
    node = RayTransformationNode(
        name="transformation",
        transformation=transform_feature,
        config=ray_config,
    )
    node.add_input(dummy_node)
    mock_context.node_outputs = {"input_node": input_value}
    result = node.execute(mock_context)
    assert isinstance(result, DAGValue)
    assert result.format == DAGFormat.RAY
    result_df = result.data.to_pandas()
    assert len(result_df) == 3
    assert "conv_rate_doubled" in result_df.columns
    assert (
        result_df["conv_rate_doubled"].iloc[0] == sample_data["conv_rate"].iloc[0] * 2
    )


def test_ray_filter_node(
    ray_session, ray_config, mock_context, sample_data, column_info
):
    """Test RayFilterNode functionality."""
    ray_dataset = ray.data.from_pandas(sample_data)
    input_value = DAGValue(data=ray_dataset, format=DAGFormat.RAY)
    dummy_node = DummyInputNode("input_node", input_value)
    node = RayFilterNode(
        name="filter",
        column_info=column_info,
        config=ray_config,
        ttl=timedelta(hours=2),
        filter_condition=None,
    )
    node.add_input(dummy_node)
    mock_context.node_outputs = {"input_node": input_value}
    result = node.execute(mock_context)
    assert isinstance(result, DAGValue)
    assert result.format == DAGFormat.RAY
    result_df = result.data.to_pandas()
    assert len(result_df) <= 3
    assert "event_timestamp" in result_df.columns


def test_ray_dedup_node(
    ray_session, ray_config, mock_context, sample_data, column_info
):
    """Test RayDedupNode functionality."""
    duplicated_data = pd.concat([sample_data, sample_data.iloc[:1]], ignore_index=True)
    ray_dataset = ray.data.from_pandas(duplicated_data)
    input_value = DAGValue(data=ray_dataset, format=DAGFormat.RAY)
    dummy_node = DummyInputNode("input_node", input_value)
    node = RayDedupNode(
        name="dedup",
        column_info=column_info,
        config=ray_config,
    )
    node.add_input(dummy_node)
    mock_context.node_outputs = {"input_node": input_value}
    result = node.execute(mock_context)
    assert isinstance(result, DAGValue)
    assert result.format == DAGFormat.RAY
    result_df = result.data.to_pandas()
    assert len(result_df) == 2  # Should remove the duplicate row
    assert "driver_id" in result_df.columns


def test_ray_config_validation():
    """Test Ray configuration validation."""
    # Test valid configuration
    config = RayComputeEngineConfig(
        type="ray.engine",
        max_workers=4,
        enable_optimization=True,
        broadcast_join_threshold_mb=100,
        target_partition_size_mb=64,
        window_size_for_joins="30min",
    )

    assert config.type == "ray.engine"
    assert config.max_workers == 4
    assert config.window_size_timedelta == timedelta(minutes=30)

    # Test window size parsing
    config_hours = RayComputeEngineConfig(window_size_for_joins="2H")
    assert config_hours.window_size_timedelta == timedelta(hours=2)

    config_seconds = RayComputeEngineConfig(window_size_for_joins="30s")
    assert config_seconds.window_size_timedelta == timedelta(seconds=30)

    # Test invalid window size defaults to 1 hour
    config_invalid = RayComputeEngineConfig(window_size_for_joins="invalid")
    assert config_invalid.window_size_timedelta == timedelta(hours=1)
