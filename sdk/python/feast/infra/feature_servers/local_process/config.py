from typing import Literal

from feast.infra.feature_servers.base_config import BaseFeatureServerConfig


class LocalFeatureServerConfig(BaseFeatureServerConfig):
    # Feature server type selector.
    type: Literal["local"] = "local"

    # The endpoint definition for transformation_service
    transformation_service_endpoint: str = "localhost:6569"
