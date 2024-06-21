from pydantic import StrictBool
from pydantic.typing import Literal

from feast.infra.feature_servers.base_config import BaseFeatureServerConfig


class GcpCloudRunFeatureServerConfig(BaseFeatureServerConfig):
    """Feature server config for GCP CloudRun."""

    type: Literal["gcp_cloudrun"] = "gcp_cloudrun"
    """Feature server type selector."""

    public: StrictBool = True
    """Whether the endpoint should be publicly accessible."""

    auth: Literal["none", "api-key"] = "none"
    """Authentication method for the endpoint."""
