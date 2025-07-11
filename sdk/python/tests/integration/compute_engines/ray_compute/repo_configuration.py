"""Test configuration for Ray compute engine integration tests."""

from feast.infra.offline_stores.contrib.ray_repo_configuration import (
    RayDataSourceCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)


def get_ray_compute_engine_test_config() -> IntegrationTestRepoConfig:
    """Get test configuration for Ray compute engine."""
    return IntegrationTestRepoConfig(
        provider="local",
        online_store_creator=RedisOnlineStoreCreator,
        offline_store_creator=RayDataSourceCreator,
        batch_engine={
            "type": "ray.engine",
            "use_ray_cluster": False,
            "max_workers": 1,
            "enable_optimization": True,
            "broadcast_join_threshold_mb": 25,
            "target_partition_size_mb": 16,
            "window_size_for_joins": "1H",
            "ray_conf": {
                "num_cpus": 1,
                "object_store_memory": 80 * 1024 * 1024,
                "_memory": 400 * 1024 * 1024,
            },
        },
    )


# Configuration for different test scenarios
COMPUTE_ENGINE_CONFIGS = {
    "local": {
        "type": "ray.engine",
        "use_ray_cluster": False,
        "max_workers": 1,
        "enable_optimization": True,
        "ray_conf": {
            "num_cpus": 1,
            "object_store_memory": 80 * 1024 * 1024,
            "_memory": 400 * 1024 * 1024,
        },
    },
    "cluster": {
        "type": "ray.engine",
        "use_ray_cluster": True,
        "ray_address": "ray://localhost:10001",
        "max_workers": 2,
        "enable_optimization": True,
    },
    "optimized": {
        "type": "ray.engine",
        "use_ray_cluster": False,
        "max_workers": 2,
        "enable_optimization": True,
        "broadcast_join_threshold_mb": 25,
        "enable_distributed_joins": True,
        "max_parallelism_multiplier": 1,
        "target_partition_size_mb": 16,
        "ray_conf": {
            "num_cpus": 1,
            "object_store_memory": 80 * 1024 * 1024,
            "_memory": 400 * 1024 * 1024,
        },
    },
}
