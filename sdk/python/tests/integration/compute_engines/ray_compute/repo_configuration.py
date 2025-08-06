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
            "max_workers": 1,
            "enable_optimization": True,
        },
    )
