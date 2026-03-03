from feast.infra.offline_stores.contrib.ray_repo_configuration import (
    RayDataSourceCreator,
)

AVAILABLE_OFFLINE_STORES = [("local", RayDataSourceCreator)]
AVAILABLE_ONLINE_STORES = {"sqlite": ({"type": "sqlite"}, None)}
