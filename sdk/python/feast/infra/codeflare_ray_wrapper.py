"""
Efficient Ray Data wrapper for Feast operations.

This module provides a unified interface for Ray Data operations that can work
with local Ray clusters and KubeRay clusters via CodeFlare SDK. The key design
principle is efficiency: Ray connection is established once during initialization,
and all subsequent operations use ray.data directly without repeated initialization.
"""

import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa
import ray
import ray.data

from feast.infra.ray_config_manager import RayConfigManager, RayExecutionMode

logger = logging.getLogger(__name__)

try:
    from codeflare_sdk import Cluster, get_cluster

    CODEFLARE_AVAILABLE = True
except ImportError:
    CODEFLARE_AVAILABLE = False
    logger.warning("CodeFlare SDK not available. KubeRay functionality disabled.")


class CodeFlareRayWrapper:
    """
    Efficient wrapper for Ray Data operations.

    This wrapper establishes the Ray connection once during initialization and then
    uses ray.data operations directly. For KubeRay mode, it connects to the cluster
    via CodeFlare SDK during __init__, and all subsequent operations automatically
    use the cluster connection without repeated ray.init() calls.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], object]] = None):
        """
        Initialize the Ray wrapper.

        Args:
            config: Ray configuration object (RayOfflineStoreConfig, RayComputeEngineConfig, or dict)
        """
        self.config_manager = RayConfigManager(config or {})
        self.execution_mode = self.config_manager.determine_execution_mode()
        self.use_kuberay = self.execution_mode == RayExecutionMode.KUBERAY

        # Get configuration from manager for KubeRay connections
        kuberay_config = self.config_manager.get_kuberay_config()
        self.cluster_name = kuberay_config.get("cluster_name")
        self.namespace = kuberay_config.get("namespace", "default")
        self.auth_token = kuberay_config.get("auth_token")

        self.cluster = None
        self._ray_initialized = False

        logger.info(
            f"Ray wrapper initialized in {'KubeRay' if self.use_kuberay else 'client-side'} mode"
        )

        # Initialize Ray connection once if using KubeRay
        if self.use_kuberay:
            self._ensure_ray_connection()

    def _get_cluster(self) -> Optional[Cluster]:
        """Get or connect to existing cluster."""
        if not self.use_kuberay:
            return None

        if self.cluster is None:
            try:
                # Try to get existing cluster first
                self.cluster = get_cluster(
                    cluster_name=self.cluster_name, namespace=self.namespace
                )
                logger.info(
                    f"Connected to existing KubeRay cluster: {self.cluster_name}"
                )

            except Exception as e:
                logger.debug(f"Could not connect to existing cluster: {e}")
                return None

        return self.cluster

    def _ensure_ray_connection(self) -> bool:
        """
        Ensure Ray is connected to the appropriate cluster.
        This is called once during initialization for KubeRay mode.

        Returns:
            bool: True if connection is successful, False otherwise
        """
        if self._ray_initialized:
            return True

        if not self.use_kuberay:
            # For client-side mode, let Ray handle initialization
            self._ray_initialized = True
            return True

        logger.info(f"Attempting to connect to KubeRay cluster: {self.cluster_name}")

        try:
            cluster = self._get_cluster()
            if cluster:
                # Initialize Ray with the KubeRay cluster address
                logger.info(
                    f"Connecting to KubeRay cluster at: {cluster.cluster_uri()}"
                )

                # Prepare Ray init kwargs
                ray_kwargs = {
                    "address": cluster.cluster_uri(),
                    "ignore_reinit_error": True,
                    "log_to_driver": True,
                }

                # Add authentication token if available
                if self.auth_token:
                    logger.info("Using authentication token for Ray connection")
                    ray_kwargs["_redis_password"] = self.auth_token

                ray.init(**ray_kwargs)
                logger.info(
                    f"✓ Successfully connected to KubeRay cluster: {self.cluster_name}"
                )
                self._ray_initialized = True
                return True
            else:
                logger.warning(
                    f"✗ KubeRay cluster '{self.cluster_name}' not found in namespace '{self.namespace}'"
                )
                logger.warning("Falling back to local Ray cluster")
                self._ray_initialized = True
                return False
        except Exception as e:
            logger.warning(
                f"✗ Failed to connect to KubeRay cluster '{self.cluster_name}': {e}"
            )
            logger.warning("Falling back to local Ray cluster")
            self._ray_initialized = True
            return False

    def from_pandas(self, df: pd.DataFrame) -> Any:
        """Create Ray Dataset from pandas DataFrame."""
        return ray.data.from_pandas(df)

    def from_arrow(self, table: pa.Table) -> Any:
        """Create Ray Dataset from PyArrow Table."""
        return ray.data.from_arrow(table)

    def read_parquet(self, path: Union[str, List[str]]) -> Any:
        """Read parquet files into Ray Dataset."""
        return ray.data.read_parquet(path)

    def read_csv(self, path: Union[str, List[str]]) -> Any:
        """Read CSV files into Ray Dataset."""
        return ray.data.read_csv(path)

    def to_pandas(self, dataset: Any) -> pd.DataFrame:
        """Convert Ray Dataset to pandas DataFrame."""
        return dataset.to_pandas()

    def to_arrow(self, dataset: Any) -> pa.Table:
        """Convert Ray Dataset to PyArrow Table."""
        if hasattr(dataset, "to_arrow"):
            return dataset.to_arrow()
        else:
            df = dataset.to_pandas()
            return pa.Table.from_pandas(df)

    def cleanup(self):
        """Clean up resources."""
        if self.cluster:
            try:
                self.cluster.down()
                logger.info("Cluster connection closed")
            except Exception as e:
                logger.warning(f"Error closing cluster connection: {e}")


# Global instance for easy access
_ray_wrapper = None


def get_ray_wrapper() -> CodeFlareRayWrapper:
    """
    Get the global CodeFlare Ray wrapper instance.

    This wrapper should be initialized during Ray offline store or compute engine
    initialization using initialize_ray_wrapper().

    Returns:
        CodeFlareRayWrapper instance

    Raises:
        RuntimeError: If wrapper hasn't been initialized
    """
    global _ray_wrapper

    if _ray_wrapper is None:
        # Fallback to default configuration if not initialized
        logger.warning("Ray wrapper not initialized, using default configuration")
        _ray_wrapper = CodeFlareRayWrapper()

    return _ray_wrapper


def initialize_ray_wrapper_from_config(config: Any) -> CodeFlareRayWrapper:
    """
    Initialize the global CodeFlare Ray wrapper from Ray store/engine config.

    Args:
        config: RayOfflineStoreConfig or RayComputeEngineConfig instance

    Returns:
        CodeFlareRayWrapper instance
    """
    global _ray_wrapper

    # Use the new configuration manager approach
    _ray_wrapper = CodeFlareRayWrapper(config=config)

    return _ray_wrapper
