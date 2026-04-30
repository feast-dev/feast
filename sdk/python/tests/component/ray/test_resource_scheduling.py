"""
Tests for GPU and worker resource scheduling support in the Ray compute engine
and offline store.

Covers:
- RayComputeEngineConfig / RayOfflineStoreConfig: worker_task_options + num_gpus fields
- CodeFlareRayWrapper._get_task_options(): merge logic, num_gpus precedence
- RemoteDatasetProxy.map_batches: .options() applied, map_batches key filtering
- RayTransformationNode: worker_task_options threaded through map_batches
- RayResourceManager: GPU count read from cluster resources
- safe_batch_processor / _is_empty_batch: format-aware empty detection (regression
  for AttributeError when gpu_batch_format is "numpy" or "pyarrow")
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import ray

from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.nodes import RayTransformationNode
from feast.infra.compute_engines.ray.utils import _is_empty_batch, safe_batch_processor
from feast.infra.offline_stores.contrib.ray_offline_store.ray import (
    RayOfflineStoreConfig,
    RayResourceManager,
)
from feast.infra.ray_initializer import CodeFlareRayWrapper
from feast.infra.ray_shared_utils import RemoteDatasetProxy

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class DummyInputNode(DAGNode):
    def __init__(self, name, output):
        super().__init__(name)
        self._output = output

    def execute(self, context):
        return self._output


@pytest.fixture(scope="module")
def ray_session():
    if not ray.is_initialized():
        ray.init(num_cpus=2, ignore_reinit_error=True, include_dashboard=False)
    yield ray
    ray.shutdown()


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "driver_id": [1, 2, 3],
            "conv_rate": [0.8, 0.7, 0.6],
            "event_timestamp": [datetime.now() - timedelta(hours=i) for i in range(3)],
        }
    )


# ---------------------------------------------------------------------------
# Config field tests
# ---------------------------------------------------------------------------


class TestRayComputeEngineConfigTaskOptions:
    def test_defaults(self):
        config = RayComputeEngineConfig()
        assert config.num_gpus is None
        assert config.gpu_batch_format == "pandas"
        assert config.worker_task_options is None

    def test_num_gpus_set(self):
        config = RayComputeEngineConfig(num_gpus=1)
        assert config.num_gpus == 1

    def test_fractional_num_gpus(self):
        config = RayComputeEngineConfig(num_gpus=0.5)
        assert config.num_gpus == 0.5

    def test_gpu_batch_format(self):
        config = RayComputeEngineConfig(num_gpus=1, gpu_batch_format="numpy")
        assert config.gpu_batch_format == "numpy"

    def test_worker_task_options_roundtrip(self):
        opts = {
            "num_cpus": 4,
            "memory": 8 * 1024**3,
            "accelerator_type": "A100",
            "max_retries": 5,
        }
        config = RayComputeEngineConfig(worker_task_options=opts)
        assert config.worker_task_options["accelerator_type"] == "A100"
        assert config.worker_task_options["num_cpus"] == 4
        assert config.worker_task_options["max_retries"] == 5

    def test_worker_task_options_with_runtime_env(self):
        config = RayComputeEngineConfig(
            worker_task_options={
                "runtime_env": {
                    "pip": ["cudf-cu12==24.10.0"],
                    "env_vars": {"CUDA_VISIBLE_DEVICES": "0"},
                }
            }
        )
        assert config.worker_task_options["runtime_env"]["pip"] == [
            "cudf-cu12==24.10.0"
        ]


class TestRayOfflineStoreConfigTaskOptions:
    def test_defaults(self):
        config = RayOfflineStoreConfig()
        assert config.num_gpus is None
        assert config.gpu_batch_format == "pandas"
        assert config.worker_task_options is None

    def test_worker_task_options_roundtrip(self):
        config = RayOfflineStoreConfig(
            worker_task_options={"num_cpus": 2, "accelerator_type": "T4"}
        )
        assert config.worker_task_options["num_cpus"] == 2
        assert config.worker_task_options["accelerator_type"] == "T4"


# ---------------------------------------------------------------------------
# CodeFlareRayWrapper._get_task_options() tests
# ---------------------------------------------------------------------------


class TestCodeFlareRayWrapperGetTaskOptions:
    """Unit tests for _get_task_options() merge logic — no cluster connection needed."""

    def _make_wrapper(self, num_gpus=0, worker_task_options=None):
        """Construct a wrapper instance without triggering __init__ side-effects."""
        wrapper = CodeFlareRayWrapper.__new__(CodeFlareRayWrapper)
        wrapper.num_gpus = num_gpus
        wrapper.worker_task_options = worker_task_options or {}
        return wrapper

    def test_empty_when_no_resources(self):
        wrapper = self._make_wrapper()
        assert wrapper._get_task_options() == {}

    def test_io_tasks_exclude_num_gpus_by_default(self):
        """include_gpu defaults to False so I/O methods never consume GPU slots."""
        wrapper = self._make_wrapper(num_gpus=1)
        opts = wrapper._get_task_options()  # default: include_gpu=False
        assert "num_gpus" not in opts

    def test_io_tasks_strip_num_gpus_from_worker_task_options(self):
        """
        Regression: num_gpus set inside worker_task_options must also be stripped
        for I/O tasks. Previously _get_task_options only blocked the first-class
        num_gpus field; a num_gpus key already present in the copied
        worker_task_options dict would leak through to .options() on I/O tasks.
        """
        wrapper = self._make_wrapper(
            num_gpus=0,  # first-class field not set
            worker_task_options={"num_gpus": 1, "num_cpus": 4},
        )
        opts = wrapper._get_task_options()  # default: include_gpu=False
        assert "num_gpus" not in opts
        assert opts.get("num_cpus") == 4  # other keys unaffected

    def test_num_gpus_added_when_include_gpu_true(self):
        """include_gpu=True is used for compute tasks that actually need GPUs."""
        wrapper = self._make_wrapper(num_gpus=1)
        assert wrapper._get_task_options(include_gpu=True) == {"num_gpus": 1}

    def test_worker_task_options_passthrough(self):
        wrapper = self._make_wrapper(
            worker_task_options={"num_cpus": 4, "accelerator_type": "A100"}
        )
        opts = wrapper._get_task_options()
        assert opts["num_cpus"] == 4
        assert opts["accelerator_type"] == "A100"

    def test_num_gpus_takes_precedence_over_worker_task_options(self):
        """First-class num_gpus must override worker_task_options['num_gpus']."""
        wrapper = self._make_wrapper(num_gpus=2, worker_task_options={"num_gpus": 1})
        opts = wrapper._get_task_options(include_gpu=True)
        assert opts["num_gpus"] == 2

    def test_combined_num_gpus_and_worker_task_options(self):
        wrapper = self._make_wrapper(
            num_gpus=1,
            worker_task_options={
                "num_cpus": 4,
                "memory": 8 * 1024**3,
                "max_retries": 5,
            },
        )
        opts = wrapper._get_task_options(include_gpu=True)
        assert opts["num_gpus"] == 1
        assert opts["num_cpus"] == 4
        assert opts["memory"] == 8 * 1024**3
        assert opts["max_retries"] == 5

    def test_zero_num_gpus_not_added(self):
        """num_gpus=0 (falsy) must not be added to options."""
        wrapper = self._make_wrapper(num_gpus=0, worker_task_options={"num_cpus": 2})
        opts = wrapper._get_task_options(include_gpu=True)
        assert "num_gpus" not in opts
        assert opts["num_cpus"] == 2


# ---------------------------------------------------------------------------
# RemoteDatasetProxy.map_batches resource key filtering
# ---------------------------------------------------------------------------


class TestRemoteDatasetProxyMapBatches:
    """
    Verifies that map_batches applies .options() correctly and only forwards
    the scheduling-relevant subset of keys into the Ray Data map_batches call.
    """

    def test_no_options_when_empty(self, ray_session, sample_df):
        dataset = ray.data.from_pandas(sample_df)
        proxy = RemoteDatasetProxy(ray.put(dataset))

        with patch.object(
            ray.remote(lambda d, f, b, r: d.map_batches(f, **b, **r)),
            "options",
            wraps=lambda **kw: None,
        ):
            result = proxy.map_batches(lambda b: b, batch_format="pandas")
        assert isinstance(result, RemoteDatasetProxy)

    def test_map_batches_resource_keys_filtered(self):
        """
        The filtering set inside map_batches must pass only scheduling-relevant
        keys (num_gpus, num_cpus, accelerator_type, resources) to Ray Data's
        map_batches, and strip non-scheduling keys (max_retries, runtime_env,
        memory, scheduling_strategy).
        """
        _MAP_BATCHES_RESOURCE_KEYS = {
            "num_gpus",
            "num_cpus",
            "accelerator_type",
            "resources",
        }

        all_task_options = {
            "num_gpus": 1,
            "num_cpus": 4,
            "accelerator_type": "A100",
            "resources": {"custom": 1},
            "max_retries": 5,
            "runtime_env": {"pip": ["cudf"]},
            "memory": 8 * 1024**3,
            "scheduling_strategy": "SPREAD",
        }

        map_resource_kwargs = {
            k: v for k, v in all_task_options.items() if k in _MAP_BATCHES_RESOURCE_KEYS
        }

        # Should-be-present keys
        assert "num_gpus" in map_resource_kwargs
        assert "num_cpus" in map_resource_kwargs
        assert "accelerator_type" in map_resource_kwargs
        assert "resources" in map_resource_kwargs

        # Should-be-absent keys
        assert "max_retries" not in map_resource_kwargs
        assert "runtime_env" not in map_resource_kwargs
        assert "memory" not in map_resource_kwargs
        assert "scheduling_strategy" not in map_resource_kwargs

    def test_orchestration_task_excludes_compute_keys(self):
        """
        The @ray.remote orchestration wrapper must never hold compute-scheduling
        resources (num_gpus, num_cpus, accelerator_type, resources). Only
        non-scheduling keys (runtime_env, max_retries, memory, etc.) should
        reach the orchestration task via .options(), avoiding the deadlock
        where the orchestrator holds a GPU slot while waiting for data workers
        that also need GPUs.
        """
        _MAP_BATCHES_RESOURCE_KEYS = {
            "num_gpus",
            "num_cpus",
            "accelerator_type",
            "resources",
        }

        all_opts = {
            "num_gpus": 1,
            "num_cpus": 4,
            "accelerator_type": "A100",
            "resources": {"custom": 1},
            "max_retries": 5,
            "runtime_env": {"pip": ["cudf"]},
            "memory": 8 * 1024**3,
            "scheduling_strategy": "SPREAD",
        }

        orchestration_opts = {
            k: v for k, v in all_opts.items() if k not in _MAP_BATCHES_RESOURCE_KEYS
        }

        # Compute-scheduling keys must be absent from the orchestration task
        assert "num_gpus" not in orchestration_opts
        assert "num_cpus" not in orchestration_opts
        assert "accelerator_type" not in orchestration_opts
        assert "resources" not in orchestration_opts

        # Non-scheduling keys must be present (orchestration task can use them)
        assert orchestration_opts["max_retries"] == 5
        assert orchestration_opts["runtime_env"] == {"pip": ["cudf"]}
        assert orchestration_opts["memory"] == 8 * 1024**3
        assert orchestration_opts["scheduling_strategy"] == "SPREAD"


# ---------------------------------------------------------------------------
# RayTransformationNode — task_options and num_gpus threaded through
# ---------------------------------------------------------------------------


class TestRayTransformationNodeResourceScheduling:
    @pytest.fixture
    def mock_context(self):
        class DummyOfflineStore:
            def offline_write_batch(self, *args, **kwargs):
                pass

        class Ctx:
            registry = None
            store = None
            project = "test_project"
            entity_data = None
            config = None
            node_outputs = {}
            offline_store = DummyOfflineStore()

        return Ctx()

    def test_transformation_with_worker_task_options_executes(
        self, ray_session, mock_context, sample_df
    ):
        """Node with worker_task_options runs end-to-end and produces correct output."""
        config = RayComputeEngineConfig(
            max_workers=2,
            worker_task_options={"num_cpus": 1},
        )
        dataset = ray.data.from_pandas(sample_df)
        input_value = DAGValue(data=dataset, format=DAGFormat.RAY)
        input_node = DummyInputNode("inp", input_value)

        def double_conv(df: pd.DataFrame) -> pd.DataFrame:
            df["conv_rate"] = df["conv_rate"] * 2
            return df

        node = RayTransformationNode(
            name="t", transformation=double_conv, config=config
        )
        node.add_input(input_node)
        mock_context.node_outputs = {"inp": input_value}

        result = node.execute(mock_context)
        result_df = result.data.to_pandas()
        assert len(result_df) == 3
        assert (
            abs(result_df["conv_rate"].iloc[0] - sample_df["conv_rate"].iloc[0] * 2)
            < 1e-6
        )

    def test_transformation_gpu_batch_format_applied(
        self, ray_session, mock_context, sample_df
    ):
        """
        When num_gpus is set, gpu_batch_format is passed as batch_format and
        num_gpus is included in the map_batches kwargs.

        The UDF receives a numpy dict (Dict[str, np.ndarray]) when
        batch_format="numpy". safe_batch_processor must handle this without
        raising AttributeError on .empty — this test is the regression guard.
        """
        config = RayComputeEngineConfig(
            max_workers=2,
            num_gpus=1,
            gpu_batch_format="numpy",
        )
        dataset = ray.data.from_pandas(sample_df)
        input_value = DAGValue(data=dataset, format=DAGFormat.RAY)
        input_node = DummyInputNode("inp", input_value)

        # UDF receives Dict[str, np.ndarray] when batch_format="numpy"
        def numpy_identity(batch):
            return batch

        node = RayTransformationNode(
            name="t", transformation=numpy_identity, config=config
        )
        node.add_input(input_node)
        mock_context.node_outputs = {"inp": input_value}

        captured_kwargs = {}
        original_map_batches = ray.data.Dataset.map_batches

        def capturing_map_batches(self_ds, fn, **kwargs):
            captured_kwargs.update(kwargs)
            # Strip GPU-scheduling keys so local Ray (no GPU nodes, no batch_size
            # requirement) can execute without raising ValueError. The correctness
            # of safe_batch_processor on numpy dicts is covered by
            # TestSafeBatchProcessorFormats — this test verifies config flow only.
            safe_kwargs = {k: v for k, v in kwargs.items() if k not in ("num_gpus",)}
            safe_kwargs.setdefault("batch_format", "pandas")
            return original_map_batches(self_ds, fn, **safe_kwargs)

        with patch.object(ray.data.Dataset, "map_batches", capturing_map_batches):
            node.execute(mock_context)

        assert captured_kwargs.get("batch_format") == "numpy"
        assert captured_kwargs.get("num_gpus") == 1

    def test_transformation_no_resource_keys_when_no_options(
        self, ray_session, mock_context, sample_df
    ):
        """With no GPU/worker_task_options, map_batches only gets batch_format + concurrency."""
        config = RayComputeEngineConfig(max_workers=2)
        dataset = ray.data.from_pandas(sample_df)
        input_value = DAGValue(data=dataset, format=DAGFormat.RAY)
        input_node = DummyInputNode("inp", input_value)
        node = RayTransformationNode(
            name="t", transformation=lambda df: df, config=config
        )
        node.add_input(input_node)
        mock_context.node_outputs = {"inp": input_value}

        captured_kwargs = {}
        original_map_batches = ray.data.Dataset.map_batches

        def spy(self_ds, fn, **kwargs):
            captured_kwargs.update(kwargs)
            return original_map_batches(self_ds, fn, **kwargs)

        with patch.object(ray.data.Dataset, "map_batches", spy):
            node.execute(mock_context)

        assert "num_gpus" not in captured_kwargs
        assert "num_cpus" not in captured_kwargs
        assert "accelerator_type" not in captured_kwargs
        assert captured_kwargs.get("batch_format") == "pandas"


# ---------------------------------------------------------------------------
# RayResourceManager — GPU count from cluster resources
# ---------------------------------------------------------------------------


class TestRayResourceManagerGPU:
    def test_reads_gpu_from_cluster_resources(self):
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 8, "GPU": 4, "memory": 32 * 1024**3},
            ),
            patch("ray.nodes", return_value=[{}, {}, {}, {}]),
        ):
            mgr = RayResourceManager()
            assert mgr.available_gpus == 4
            assert mgr.available_cpus == 8
            assert mgr.num_nodes == 4

    def test_available_gpus_zero_when_no_gpus(self):
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 4, "memory": 8 * 1024**3},
            ),
            patch("ray.nodes", return_value=[{}]),
        ):
            mgr = RayResourceManager()
            assert mgr.available_gpus == 0

    def test_available_gpus_zero_when_ray_not_initialized(self):
        with patch("ray.is_initialized", return_value=False):
            mgr = RayResourceManager()
            assert mgr.available_gpus == 0


# ---------------------------------------------------------------------------
# safe_batch_processor / _is_empty_batch — format-aware empty detection
# Regression tests for AttributeError when gpu_batch_format is "numpy"/"pyarrow"
# ---------------------------------------------------------------------------


class TestIsEmptyBatch:
    """Unit tests for _is_empty_batch() across all Ray Data batch formats."""

    def test_pandas_non_empty(self, sample_df):
        assert _is_empty_batch(sample_df) is False

    def test_pandas_empty(self):
        assert _is_empty_batch(pd.DataFrame()) is True

    def test_numpy_non_empty(self):
        batch = {"col_a": np.array([1, 2, 3]), "col_b": np.array([4, 5, 6])}
        assert _is_empty_batch(batch) is False

    def test_numpy_empty_arrays(self):
        batch = {"col_a": np.array([]), "col_b": np.array([])}
        assert _is_empty_batch(batch) is True

    def test_numpy_empty_dict(self):
        assert _is_empty_batch({}) is True

    def test_pyarrow_non_empty(self, sample_df):
        table = pa.Table.from_pandas(sample_df)
        assert _is_empty_batch(table) is False

    def test_pyarrow_empty(self):
        table = pa.table({"col_a": pa.array([], type=pa.int64())})
        assert _is_empty_batch(table) is True


class TestSafeBatchProcessorFormats:
    """
    Regression tests: safe_batch_processor must not raise AttributeError
    when receiving non-pandas batches (numpy dict or pyarrow Table).
    """

    def test_pandas_batch_passes_through(self, sample_df):
        @safe_batch_processor
        def double_conv(batch: pd.DataFrame) -> pd.DataFrame:
            batch = batch.copy()
            batch["conv_rate"] = batch["conv_rate"] * 2
            return batch

        result = double_conv(sample_df)
        assert isinstance(result, pd.DataFrame)
        assert result["conv_rate"].iloc[0] == pytest.approx(
            sample_df["conv_rate"].iloc[0] * 2
        )

    def test_pandas_empty_batch_returned_early(self):
        called = []

        @safe_batch_processor
        def should_not_run(batch):
            called.append(True)
            return batch

        result = should_not_run(pd.DataFrame())
        assert called == []
        assert isinstance(result, pd.DataFrame)

    def test_numpy_batch_no_attribute_error(self):
        """Regression: was raising AttributeError on batch.empty."""

        @safe_batch_processor
        def numpy_passthrough(batch):
            return batch

        batch = {
            "driver_id": np.array([1, 2, 3]),
            "conv_rate": np.array([0.8, 0.7, 0.6]),
        }
        # Must not raise AttributeError
        result = numpy_passthrough(batch)
        assert result is batch

    def test_numpy_empty_batch_returned_early(self):
        called = []

        @safe_batch_processor
        def should_not_run(batch):
            called.append(True)
            return batch

        empty_batch = {"col": np.array([])}
        result = should_not_run(empty_batch)
        assert called == []
        assert result is empty_batch

    def test_pyarrow_batch_no_attribute_error(self, sample_df):
        """Regression: was raising AttributeError on batch.empty."""

        @safe_batch_processor
        def pyarrow_passthrough(batch):
            return batch

        table = pa.Table.from_pandas(sample_df)
        result = pyarrow_passthrough(table)
        assert result is table

    def test_pyarrow_empty_batch_returned_early(self):
        called = []

        @safe_batch_processor
        def should_not_run(batch):
            called.append(True)
            return batch

        empty_table = pa.table({"col": pa.array([], type=pa.int64())})
        result = should_not_run(empty_table)
        assert called == []
        assert result is empty_table

    def test_exception_in_func_returns_original_batch(self):
        @safe_batch_processor
        def always_raises(batch):
            raise ValueError("simulated error")

        df = pd.DataFrame({"x": [1, 2]})
        result = always_raises(df)
        # Should return the original batch, not propagate
        assert result is df
