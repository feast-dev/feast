from pydantic.typing import Literal

from feast.infra.feature_servers.base_config import BaseFeatureServerConfig


class LocalFeatureServerConfig(BaseFeatureServerConfig):
    type: Literal["local"] = "local"
    """Feature server type selector."""
