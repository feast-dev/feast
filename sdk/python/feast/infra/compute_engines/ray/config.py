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
