"""Configuration for Ray compute engine."""

from datetime import timedelta
from typing import Any, Dict, Literal, Optional

from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class RayComputeEngineConfig(FeastConfigBaseModel):
    """Configuration for Ray Compute Engine."""

    type: Literal["ray.engine"] = "ray.engine"
    """Ray Compute Engine type selector"""

    ray_address: Optional[str] = None
    """Ray cluster address. If None, uses local Ray cluster."""

    staging_location: Optional[StrictStr] = None
    """Remote path for batch materialization jobs"""

    # Ray-specific performance configurations
    broadcast_join_threshold_mb: int = 100
    """Threshold for using broadcast joins (in MB)"""

    enable_distributed_joins: bool = True
    """Whether to enable distributed joins for large datasets"""

    max_parallelism_multiplier: int = 2
    """Multiplier for max parallelism based on available CPUs"""

    target_partition_size_mb: int = 64
    """Target partition size in MB"""

    window_size_for_joins: str = "1H"
    """Window size for windowed temporal joins"""

    ray_conf: Optional[Dict[str, Any]] = None
    """Ray configuration parameters"""

    # Additional configuration options
    max_workers: Optional[int] = None
    """Maximum number of Ray workers. If None, uses all available cores."""

    enable_optimization: bool = True
    """Enable automatic performance optimizations."""

    # Worker task resource configuration
    num_gpus: Optional[float] = None
    """Number of GPUs to request per worker task. Requires GPU nodes in the
    Ray cluster. Fractional values (e.g. 0.5) are supported by Ray for GPU
    sharing. Supported in all modes: local, remote, and KubeRay."""

    gpu_batch_format: str = "pandas"
    """Batch format for map_batches when num_gpus is set. Use 'numpy' or
    'pyarrow' for GPU-native libraries (e.g. cuDF, PyTorch). Defaults to
    'pandas'."""

    worker_task_options: Optional[Dict[str, Any]] = None
    """Arbitrary Ray task options passed verbatim to @ray.remote .options()
    and map_batches for every worker task Feast dispatches. This is the
    escape hatch for any Ray or CodeFlare SDK scheduling parameter not
    covered by the dedicated fields above.

    Pairs with ray_conf (which configures ray.init) — worker_task_options
    targets the individual worker tasks rather than the cluster connection.

    Common keys (see https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html):
      num_cpus          (float)  – CPUs per task (default: 1)
      memory            (int)    – Heap memory in bytes (e.g. 8 * 1024**3 for 8 GB)
      accelerator_type  (str)    – Specific GPU model, e.g. 'A100', 'T4', 'V100'.
                                   Pins tasks to nodes advertising that type. Useful
                                   on KubeRay clusters with mixed GPU pools.
      resources         (dict)   – Custom/extended resource labels, e.g.
                                   {'intel.com/gpu': 1} for Kubernetes extended resources.
      runtime_env       (dict)   – Per-task runtime environment (pip, conda, env_vars,
                                   working_dir, …). For KubeRay use this to install
                                   extra packages on workers without rebuilding images.
      max_retries       (int)    – Task retry count on worker failure (default: 3).
      scheduling_strategy (str)  – 'DEFAULT', 'SPREAD', or a placement group strategy.

    Example:
      worker_task_options:
        num_cpus: 4
        memory: 8589934592       # 8 GB
        accelerator_type: "A100"
        max_retries: 5
        runtime_env:
          pip: ["cudf-cu12==24.10.0"]
          env_vars: {CUDA_VISIBLE_DEVICES: "0"}
    """

    @property
    def window_size_timedelta(self) -> timedelta:
        """Convert window size string to timedelta."""
        if self.window_size_for_joins.endswith("H"):
            hours = int(self.window_size_for_joins[:-1])
            return timedelta(hours=hours)
        elif self.window_size_for_joins.endswith("min"):
            minutes = int(self.window_size_for_joins[:-3])
            return timedelta(minutes=minutes)
        elif self.window_size_for_joins.endswith("s"):
            seconds = int(self.window_size_for_joins[:-1])
            return timedelta(seconds=seconds)
        else:
            # Default to 1 hour
            return timedelta(hours=1)

    # KubeRay/CodeFlare SDK configurations
    use_kuberay: Optional[bool] = None
    """Whether to use KubeRay/CodeFlare SDK for Ray cluster management"""

    cluster_name: Optional[str] = None
    """Name of the KubeRay cluster to connect to (required for KubeRay mode)"""

    auth_token: Optional[str] = None
    """Authentication token for Ray cluster connection (for secure clusters)"""

    kuberay_conf: Optional[Dict[str, Any]] = None
    """KubeRay/CodeFlare configuration parameters (passed to CodeFlare SDK)"""

    enable_ray_logging: bool = False
    """Enable Ray progress bars and verbose logging"""
